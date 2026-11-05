#!/usr/bin/env python3
"""
Distributed backup client (binary protocol, persistent connections).

Protocol spoken by server.py
----------------------------
Handshake (client -> server):
    !I  filename length, followed by the UTF-8 filename
Handshake reply (server -> client):
    b"READY"                 5 bytes
    !I  N                    number of chunks the server already holds
    N x !I                   the IDs of those chunks
Per chunk (client -> server):
    !II chunk_id, data_size
    64 ASCII bytes           SHA-256 hex digest of the data
    data_size bytes          raw chunk data
Per chunk reply (server -> client), always 6 bytes:
    "OK    " | "EXISTS" | "BAD   "

Features
--------
* One persistent TCP connection per server, one worker thread per server,
  so servers are fed in parallel.
* Chunk placement: chunk_id % n_servers, plus --mirror extra copies on the
  following servers. If a server is unreachable or dies mid-transfer, its
  chunks are automatically re-routed to the remaining servers.
* Per-chunk retries (reconnect on network errors, resend on BAD).
* Resume: chunks the servers already list are skipped.
* Final verification: the local file is re-hashed to prove it did not change
  during the transfer, and every server is re-queried to confirm that every
  chunk of the file is stored somewhere.
"""

import argparse
import hashlib
import logging
import os
import queue
import socket
import struct
import sys
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

# --------------------------------------------------------------------------
# Defaults (matching the mininet topology: h1 = client, h2/h3 = servers)
# --------------------------------------------------------------------------
DEFAULT_SERVERS = ["10.0.0.2", "10.0.0.3"]
DEFAULT_PORT = 5000
DEFAULT_CHUNK_SIZE = 64 * 1024
DEFAULT_TIMEOUT = 10.0
DEFAULT_RETRIES = 5
DEFAULT_RETRY_DELAY = 1.0
DEFAULT_MIRROR = 1

# --------------------------------------------------------------------------
# Wire-format constants (must match server.py)
# --------------------------------------------------------------------------
READY_MAGIC = b"READY"
STATUS_SIZE = 6
CHECKSUM_SIZE = 64
MAX_FILENAME_SIZE = 4096
MAX_CHUNK_SIZE = 16 * 1024 * 1024
MAX_CHUNK_ID = 2 ** 32 - 1
MAX_LISTED_CHUNKS = 50_000_000      # sanity bound on the server's listing
MIN_SEND_RATE = 32 * 1024           # bytes/s, used to scale send timeouts

log = logging.getLogger("client")


class ProtocolError(ConnectionError):
    """The server sent something that violates the protocol."""


# --------------------------------------------------------------------------
# Low-level helpers
# --------------------------------------------------------------------------
def receive_exact(sock, size):
    """Receive exactly `size` bytes or raise ConnectionError."""
    buf = bytearray()
    while len(buf) < size:
        part = sock.recv(size - len(buf))
        if not part:
            raise ConnectionError("connection closed by peer")
        buf.extend(part)
    return bytes(buf)


def sha256_hex(data):
    return hashlib.sha256(data).hexdigest()


def hash_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


# --------------------------------------------------------------------------
# One persistent connection to one server
# --------------------------------------------------------------------------
class ServerLink:
    def __init__(self, host, port, remote_name, timeout):
        self.host = host
        self.port = port
        self.remote_name = remote_name
        self.timeout = timeout
        self.conn = None
        self.existing = set()          # chunk IDs listed at last handshake
        self.alive = True
        self.queue = queue.Queue()     # chunk IDs assigned to this server
        self.sent = 0                  # chunks stored via this link
        self.stored_ids = set()        # chunk IDs acknowledged this run

    def __str__(self):
        return f"{self.host}:{self.port}"

    def close(self):
        if self.conn is not None:
            try:
                self.conn.close()
            except OSError:
                pass
            self.conn = None

    def connect(self):
        """Open a connection, send the filename, read READY + chunk list."""
        self.close()
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(self.timeout)
        try:
            sock.connect((self.host, self.port))
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

            name = self.remote_name.encode("utf-8")
            sock.sendall(struct.pack("!I", len(name)) + name)

            magic = receive_exact(sock, len(READY_MAGIC))
            if magic != READY_MAGIC:
                raise ProtocolError(f"expected READY, got {magic!r}")

            (count,) = struct.unpack("!I", receive_exact(sock, 4))
            if count > MAX_LISTED_CHUNKS:
                raise ProtocolError(f"implausible chunk count {count}")

            ids = ()
            if count:
                ids = struct.unpack(f"!{count}I", receive_exact(sock, 4 * count))
        except BaseException:
            sock.close()
            raise

        self.conn = sock
        self.existing = set(ids)

    def send_chunk(self, chunk_id, data, checksum):
        """Send one chunk on the open connection and return the status."""
        # sendall()'s timeout covers the whole call, so scale it by size.
        self.conn.settimeout(self.timeout + len(data) / MIN_SEND_RATE)
        header = struct.pack("!II", chunk_id, len(data))
        self.conn.sendall(header + checksum + data)

        raw = receive_exact(self.conn, STATUS_SIZE)
        try:
            return raw.decode("ascii").strip()
        except UnicodeDecodeError:
            raise ProtocolError(f"garbled status {raw!r}")


def push_chunk(link, chunk_id, data, checksum, retries, retry_delay):
    """
    Store one chunk on one server, retrying as needed.

    Returns (stored, link_broken):
      stored       True if the server now holds the chunk
      link_broken  True if the final failure was a network/protocol error
                   (the server should be considered down), False if it was
                   only repeated BAD replies.
    """
    broken = False
    for attempt in range(1, retries + 1):
        try:
            if link.conn is None:
                link.connect()
                # An earlier attempt may have been stored even though its
                # acknowledgement was lost.
                if chunk_id in link.existing:
                    return True, False

            status = link.send_chunk(chunk_id, data, checksum)

            if status in ("OK", "EXISTS"):
                return True, False
            if status == "BAD":
                broken = False
                log.warning("%s: chunk %d rejected (BAD), attempt %d/%d",
                            link, chunk_id, attempt, retries)
            else:
                raise ProtocolError(f"unexpected status {status!r}")

        except (OSError, ValueError) as err:   # includes timeouts/protocol
            broken = True
            link.close()
            log.warning("%s: chunk %d failed (%s), attempt %d/%d",
                        link, chunk_id, err, attempt, retries)

        if attempt < retries:
            time.sleep(min(retry_delay * attempt, 10.0))

    return False, broken


# --------------------------------------------------------------------------
# Transfer coordinator
# --------------------------------------------------------------------------
class Transfer:
    def __init__(self, links, path, chunk_size, total_chunks, file_size,
                 mirror, retries, retry_delay):
        self.links = links
        self.path = path
        self.chunk_size = chunk_size
        self.total_chunks = total_chunks
        self.file_size = file_size
        self.mirror = mirror
        self.retries = retries
        self.retry_delay = retry_delay

        self.lock = threading.Lock()
        self.done = threading.Event()
        self.outstanding = 0                     # queued + in-flight tasks
        self.holders = defaultdict(set)          # chunk -> links storing it
        self.pending = defaultdict(set)          # chunk -> links assigned
        self.failed = defaultdict(set)           # chunk -> links that failed
        self.lost = set()                        # chunks stored nowhere
        self.fatal = None
        self.stored_count = 0
        self.to_send = 0
        self._next_report = 0

    # ---- scheduling (self.lock must be held) -----------------------------
    def _order(self, chunk_id):
        n = len(self.links)
        start = chunk_id % n
        return [self.links[(start + i) % n] for i in range(n)]

    def _enqueue(self, chunk_id, link):
        self.pending[chunk_id].add(link)
        self.outstanding += 1
        link.queue.put(chunk_id)

    def _task_done(self):
        self.outstanding -= 1
        if self.outstanding == 0:
            self.done.set()

    def _reassign(self, chunk_id):
        """A server failed this chunk: hand it to another live server."""
        excluded = (self.holders[chunk_id] | self.pending[chunk_id]
                    | self.failed[chunk_id])
        for link in self._order(chunk_id):
            if link.alive and link not in excluded:
                log.info("Chunk %d re-routed to %s", chunk_id, link)
                self._enqueue(chunk_id, link)
                return
        if not self.holders[chunk_id] and not self.pending[chunk_id]:
            self.lost.add(chunk_id)
            log.error("Chunk %d could not be stored on any server", chunk_id)

    def _link_down(self, link):
        """Mark a server dead and re-send chunks that now have no live copy."""
        link.alive = False
        log.error("Server %s marked DOWN; its remaining and unmirrored "
                  "chunks will be re-routed", link)
        for cid in sorted(link.existing | link.stored_ids):
            if cid < self.total_chunks and link in self.holders[cid]:
                self.holders[cid].discard(link)
                self.failed[cid].add(link)
                if not any(h.alive for h in self.holders[cid]):
                    self._reassign(cid)

    def _plan(self):
        with self.lock:
            alive = [l for l in self.links if l.alive]
            wanted = min(self.mirror, len(alive))
            already = 0
            for cid in range(self.total_chunks):
                order = [l for l in self._order(cid) if l.alive]
                for link in order:
                    if cid in link.existing:
                        self.holders[cid].add(link)
                if self.holders[cid]:
                    already += 1
                need = wanted - len(self.holders[cid])
                for link in order:
                    if need <= 0:
                        break
                    if link not in self.holders[cid]:
                        self._enqueue(cid, link)
                        need -= 1
            self.to_send = self.outstanding
            if already:
                log.info("Resume: %d of %d chunks already stored on at "
                         "least one server", already, self.total_chunks)
            log.info("Chunks to send: %d (copies per chunk: %d)",
                     self.to_send, wanted)
            if self.outstanding == 0:
                self.done.set()

    # ---- worker ------------------------------------------------------------
    def _expected_len(self, chunk_id):
        return min(self.chunk_size, self.file_size - chunk_id * self.chunk_size)

    def _worker(self, link):
        fh = open(self.path, "rb")
        try:
            while True:
                cid = link.queue.get()
                if cid is None:
                    return

                if not link.alive:          # server died; pass work on
                    with self.lock:
                        self.pending[cid].discard(link)
                        self.failed[cid].add(link)
                        self._reassign(cid)
                        self._task_done()
                    continue

                fh.seek(cid * self.chunk_size)
                data = fh.read(self.chunk_size)
                if len(data) != self._expected_len(cid):
                    raise RuntimeError(
                        "input file changed size during the transfer")
                checksum = sha256_hex(data).encode("ascii")

                stored, broken = push_chunk(link, cid, data, checksum,
                                            self.retries, self.retry_delay)

                with self.lock:
                    self.pending[cid].discard(link)
                    if stored:
                        self.holders[cid].add(link)
                        link.stored_ids.add(cid)
                        link.sent += 1
                        self.stored_count += 1
                        self._report_progress()
                    else:
                        self.failed[cid].add(link)
                        if broken and link.alive:
                            self._link_down(link)
                        self._reassign(cid)
                    self._task_done()
        except Exception as err:            # unexpected: abort cleanly
            log.error("Worker for %s crashed: %s", link, err)
            self.fatal = err
            self.done.set()
        finally:
            fh.close()

    def _report_progress(self):
        pct = 100 * self.stored_count // max(1, self.to_send)
        if pct >= self._next_report:
            log.info("Progress: %d%% (%d/%d chunks sent)", pct,
                     self.stored_count, self.to_send)
            self._next_report = pct + 5
        else:
            log.debug("Chunk stored (%d/%d)", self.stored_count, self.to_send)

    # ---- driver ------------------------------------------------------------
    def run(self):
        threads = []
        for link in self.links:
            if link.alive:
                t = threading.Thread(target=self._worker, args=(link,),
                                     name=f"worker-{link}", daemon=True)
                t.start()
                threads.append(t)

        try:
            self._plan()
            while not self.done.wait(0.5):
                pass
        finally:
            for link in self.links:
                link.queue.put(None)
            for t in threads:
                t.join(timeout=5)
            for link in self.links:
                link.close()

        if self.fatal is not None:
            return False
        return not self.lost


# --------------------------------------------------------------------------
# Verification
# --------------------------------------------------------------------------
def verify_transfer(args, remote_name, servers, total_chunks, local_hash,
                    path, file_size):
    """Whole-file verification. Returns True when everything checks out."""
    ok = True

    # 1. The source file must be unchanged since we hashed it.
    if os.path.getsize(path) != file_size or hash_file(path) != local_hash:
        log.error("VERIFY FAILED: the input file changed during the transfer")
        return False
    log.info("Verify: source file unchanged (SHA-256 %s)", local_hash)

    # 2. Re-query every server on a fresh connection.
    copies = defaultdict(int)
    reachable = 0
    for host, port in servers:
        link = ServerLink(host, port, remote_name, args.timeout)
        try:
            link.connect()
        except (OSError, ValueError) as err:
            log.warning("Verify: %s unreachable (%s)", link, err)
            continue
        finally:
            link.close()
        reachable += 1
        for cid in link.existing:
            if cid < total_chunks:
                copies[cid] += 1
        log.info("Verify: %s holds %d chunks", link, len(link.existing))

    if reachable == 0:
        log.error("VERIFY FAILED: no server reachable")
        return False

    missing = [c for c in range(total_chunks) if copies[c] == 0]
    if missing:
        log.error("VERIFY FAILED: %d chunk(s) stored nowhere (first: %s)",
                  len(missing), missing[:10])
        ok = False

    wanted = min(args.mirror, len(servers))
    under = sum(1 for c in range(total_chunks) if 0 < copies[c] < wanted)
    if under:
        log.warning("Verify: %d chunk(s) have fewer than %d copies", under,
                    wanted)

    if ok:
        log.info("Verify: all %d chunks are present on the servers",
                 total_chunks)
    return ok


# --------------------------------------------------------------------------
# Top-level transfer
# --------------------------------------------------------------------------
def transfer_file(args):
    path = os.path.abspath(args.file)
    if not os.path.isfile(path):
        log.error("File not found: %s", path)
        return False

    remote_name = args.name or os.path.basename(path)
    name_len = len(remote_name.encode("utf-8"))
    if name_len == 0 or name_len > MAX_FILENAME_SIZE:
        log.error("Remote filename length %d is invalid", name_len)
        return False

    file_size = os.path.getsize(path)
    total_chunks = (file_size + args.chunk_size - 1) // args.chunk_size
    if total_chunks > MAX_CHUNK_ID:
        log.error("File needs too many chunks; increase --chunk-size")
        return False

    log.info("File: %s (%d bytes) as '%s'", path, file_size, remote_name)
    log.info("Chunk size: %d bytes, total chunks: %d, mirror: %d",
             args.chunk_size, total_chunks, args.mirror)

    if total_chunks == 0:
        log.info("The file is empty; there is nothing to transfer.")
        return True

    local_hash = hash_file(path)
    log.info("Local SHA-256: %s", local_hash)

    links = [ServerLink(h, p, remote_name, args.timeout)
             for h, p in args.servers]

    def initial_connect(link):
        attempts = min(2, args.retries)
        for attempt in range(1, attempts + 1):
            try:
                link.connect()
                log.info("Connected to %s (server already holds %d chunks)",
                         link, len(link.existing))
                return
            except (OSError, ValueError) as err:
                log.warning("Cannot connect to %s: %s (attempt %d/%d)",
                            link, err, attempt, attempts)
                if attempt < attempts:
                    time.sleep(args.retry_delay)
        link.alive = False

    with ThreadPoolExecutor(max_workers=len(links)) as pool:
        list(pool.map(initial_connect, links))

    if not any(l.alive for l in links):
        log.error("No server is reachable.")
        return False
    down = [str(l) for l in links if not l.alive]
    if down:
        log.warning("Continuing without: %s (their share is mirrored to "
                    "the live servers)", ", ".join(down))

    start = time.monotonic()
    transfer = Transfer(links, path, args.chunk_size, total_chunks, file_size,
                        args.mirror, args.retries, args.retry_delay)
    try:
        success = transfer.run()
    except KeyboardInterrupt:
        log.error("Interrupted; re-run the same command to resume.")
        return False
    elapsed = max(time.monotonic() - start, 1e-9)

    for link in links:
        log.info("%s: %d chunks stored this run%s", link, link.sent,
                 "" if link.alive else " (DOWN)")
    if transfer.stored_count:
        sent_bytes = transfer.stored_count * args.chunk_size
        log.info("Sent ~%.2f MiB in %.1fs (%.2f Mbit/s aggregate)",
                 sent_bytes / 2 ** 20, elapsed, sent_bytes * 8 / elapsed / 1e6)

    if not success:
        log.error("Transfer FAILED (%d chunk(s) unstored). Re-run to resume.",
                  len(transfer.lost))
        return False

    if args.no_verify:
        log.info("Transfer completed (verification skipped).")
        return True

    if not verify_transfer(args, remote_name, args.servers, total_chunks,
                           local_hash, path, file_size):
        return False

    log.info("Transfer completed and verified.")
    return True


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------
def parse_size(text):
    """Parse sizes such as 4096, 64K, 1M."""
    text = text.strip().upper()
    mult = {"K": 1024, "M": 1024 ** 2}.get(text[-1:], 1)
    if mult != 1:
        text = text[:-1]
    try:
        value = int(text) * mult
    except ValueError:
        raise argparse.ArgumentTypeError(f"invalid size: {text!r}")
    if not 1 <= value <= MAX_CHUNK_SIZE:
        raise argparse.ArgumentTypeError(
            f"chunk size must be between 1 and {MAX_CHUNK_SIZE} bytes")
    return value


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Send a file in chunks to backup servers.")
    p.add_argument("file", help="path of the file to back up")
    p.add_argument("--servers", nargs="+", default=DEFAULT_SERVERS,
                   metavar="HOST[:PORT]",
                   help="server addresses, space- or comma-separated "
                        f"(default: {' '.join(DEFAULT_SERVERS)})")
    p.add_argument("--port", type=int, default=DEFAULT_PORT,
                   help=f"default server port (default: {DEFAULT_PORT})")
    p.add_argument("--chunk-size", type=parse_size, default=DEFAULT_CHUNK_SIZE,
                   help="chunk size in bytes, K/M suffixes allowed "
                        f"(default: {DEFAULT_CHUNK_SIZE}); keep it identical "
                        "when resuming")
    p.add_argument("--mirror", type=int, default=DEFAULT_MIRROR,
                   help="number of servers that should store each chunk "
                        f"(default: {DEFAULT_MIRROR})")
    p.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT,
                   help="socket timeout in seconds "
                        f"(default: {DEFAULT_TIMEOUT})")
    p.add_argument("--retries", type=int, default=DEFAULT_RETRIES,
                   help="attempts per chunk per server "
                        f"(default: {DEFAULT_RETRIES})")
    p.add_argument("--retry-delay", type=float, default=DEFAULT_RETRY_DELAY,
                   help="base delay between retries in seconds "
                        f"(default: {DEFAULT_RETRY_DELAY})")
    p.add_argument("--name", help="filename to use on the server "
                                  "(default: the file's basename)")
    p.add_argument("--no-verify", action="store_true",
                   help="skip the final verification step")
    p.add_argument("-v", "--verbose", action="store_true",
                   help="log every chunk")
    args = p.parse_args(argv)

    servers, seen = [], set()
    for item in args.servers:
        for entry in item.split(","):
            entry = entry.strip()
            if not entry:
                continue
            host, sep, port = entry.rpartition(":")
            if sep and port.isdigit():
                target = (host, int(port))
            else:
                target = (entry, args.port)
            if target not in seen:
                seen.add(target)
                servers.append(target)
    if not servers:
        p.error("at least one server is required")
    args.servers = servers

    if args.mirror < 1:
        p.error("--mirror must be at least 1")
    if args.mirror > len(servers):
        print(f"Note: --mirror {args.mirror} reduced to {len(servers)} "
              "(number of servers)", file=sys.stderr)
        args.mirror = len(servers)
    if args.retries < 1:
        p.error("--retries must be at least 1")
    if args.timeout <= 0 or args.retry_delay < 0:
        p.error("--timeout must be > 0 and --retry-delay >= 0")
    return args


def main(argv=None):
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S")
    try:
        return 0 if transfer_file(args) else 1
    except KeyboardInterrupt:
        log.error("Interrupted.")
        return 130


if __name__ == "__main__":
    sys.exit(main())
