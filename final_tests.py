#!/usr/bin/env python3
"""
final_tests.py - complete automated test-suite for the Distributed Backup System
(Topic 24).  One Mininet session, seven scenarios, results saved to results/final/.

Scenarios (default: all):
  integrity    corrupted chunk -> BAD, valid -> OK, duplicate -> EXISTS
  baseline     normal transfer, checksum verified
  chunks       throughput for 4 KB / 16 KB / 64 KB / 256 KB chunks
  mirror       --mirror 2: every chunk on BOTH servers
  resume       client killed mid-transfer, re-run resumes
  servercrash  a backup server is killed mid-transfer, chunks are re-routed
  failover     link s2-s4 goes down mid-transfer, SDN controller reroutes

Needs the Ryu controller running (Terminal 1):
    ryu-manager --observe-links controller.py
Run (exit any open mininet> first):
    sudo python3 final_tests.py [--size MB] [scenario ...]
"""
import argparse
import csv
import os
import re
import shutil
import subprocess
import sys
import time
import traceback

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "results", "final")
SERVER_PY = os.path.join(HERE, "server.py")
CLIENT_PY = os.path.join(HERE, "client.py")
VERIFY_PY = os.path.join(HERE, "verify_backup.py")

SERVERS = {"h2": ("10.0.0.2", 5000), "h3": ("10.0.0.3", 5000)}
PATHS = {2: "path A (via s2)", 3: "path B (via s3)"}
DEFAULT_CHUNK = 64 * 1024
T_DOWN, T_UP = 5, 25            # seconds after the client starts (failover scenario)
ALL = ["integrity", "baseline", "chunks", "mirror", "resume", "servercrash", "failover"]

PROBE = '''import hashlib, socket, struct, sys
host, port = sys.argv[1], int(sys.argv[2])
name = b"integrity_probe"
data = b"integrity test chunk " * 100
good = hashlib.sha256(data).hexdigest().encode()
bad = hashlib.sha256(data + b"x").hexdigest().encode()
s = socket.create_connection((host, port), timeout=10)
def rx(n):
    b = b""
    while len(b) < n:
        p = s.recv(n - len(b))
        if not p:
            raise SystemExit("connection closed")
        b += p
    return b
s.sendall(struct.pack("!I", len(name)) + name)
assert rx(5) == b"READY"
count = struct.unpack("!I", rx(4))[0]
rx(4 * count)
def send(cid, chk):
    s.sendall(struct.pack("!II", cid, len(data)) + chk + data)
    return rx(6).decode().strip()
print("corrupted chunk (wrong SHA-256) ->", send(0, bad))
print("same chunk, correct SHA-256     ->", send(0, good))
print("same chunk sent again           ->", send(0, good))
'''


# ---------------------------------------------------------------- helpers
def chunk_ids(folder):
    ids = set()
    if os.path.isdir(folder):
        for entry in os.listdir(folder):
            if entry.startswith("chunk_") and entry[6:].isdigit():
                ids.add(int(entry[6:]))
    return ids


def flow_port(dump_text):
    for line in dump_text.splitlines():
        if "dl_src=00:00:00:00:00:01,dl_dst=00:00:00:00:00:02" in line:
            m = re.search(r'output:"?(?:s1-eth)?(\d+)', line)
            if m:
                return int(m.group(1))
    return None


def cum(samples, t):
    prev_t, prev_n = 0.0, 0
    for st, sn in samples:
        if st >= t:
            if st == prev_t:
                return float(sn)
            return prev_n + (sn - prev_n) * (t - prev_t) / (st - prev_t)
        prev_t, prev_n = st, sn
    return float(samples[-1][1]) if samples else 0.0


def window_mbit(samples, a, b, chunk):
    if b - a < 1:
        return None
    return (cum(samples, b) - cum(samples, a)) * chunk * 8 / (b - a) / 1e6


def fmt(v, unit=" Mbit/s"):
    return "n/a" if v is None else "%.2f%s" % (v, unit)


def parse_client_log(path):
    text = open(path).read()
    d = {"verified": "Transfer completed and verified." in text,
         "retries": len(re.findall(r"failed \(", text)),
         "rerouted": len(re.findall(r"re-routed to", text)),
         "down": re.findall(r"Server (\S+) marked DOWN", text),
         "secs": None, "mbit": None, "resume": None}
    m = re.search(r"Sent ~([\d.]+) MiB in ([\d.]+)s \(([\d.]+) Mbit/s", text)
    if m:
        d["secs"], d["mbit"] = float(m.group(2)), float(m.group(3))
    m = re.search(r"Resume: (\d+) of (\d+) chunks already stored", text)
    if m:
        d["resume"] = (int(m.group(1)), int(m.group(2)))
    return d


# ---------------------------------------------------------------- the lab
class Lab:
    def __init__(self, net, size_mb):
        self.net, self.size_mb = net, size_mb
        self.dirs = {n: os.path.join(OUT, n + "dir") for n in SERVERS}
        self.servers = {}
        self.report = []
        self.flows = {}
        self.marks = {}

    def say(self, text=""):
        print(text, flush=True)
        self.report.append(text)

    def host(self, name):
        return self.net.get(name)

    def make_file(self, name, mb):
        path = os.path.join(OUT, name)
        if not os.path.exists(path):
            with open(path, "wb") as f:
                for _ in range(mb):
                    f.write(os.urandom(1024 * 1024))
        return path

    # servers -----------------------------------------------------------
    def reset(self, tag):
        self.stop_servers()
        for d in self.dirs.values():
            shutil.rmtree(d, ignore_errors=True)
            os.makedirs(d)
        for name, (ip, port) in SERVERS.items():
            lg = open(os.path.join(OUT, "%s_%s.log" % (tag, name)), "w")
            p = self.host(name).popen(
                ["python3", "-u", SERVER_PY, "--dir", self.dirs[name], "--port", str(port)],
                stdout=lg, stderr=subprocess.STDOUT)
            self.servers[name] = (p, lg)
        time.sleep(1)

    def stop_servers(self):
        for name, (p, lg) in list(self.servers.items()):
            if p.poll() is None:
                p.terminate()
                try:
                    p.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    p.kill()
            lg.close()
        self.servers.clear()

    # client ------------------------------------------------------------
    def start_client(self, tag, src, extra=()):
        args = ["python3", "-u", CLIENT_PY, src, "--servers"]
        args += ["%s:%d" % v for v in SERVERS.values()] + list(extra)
        path = os.path.join(OUT, tag + "_client.log")
        lg = open(path, "w")
        return self.host("h1").popen(args, stdout=lg, stderr=subprocess.STDOUT), lg, path

    def run_client(self, tag, src, extra=(), timeout=900):
        p, lg, path = self.start_client(tag, src, extra)
        try:
            p.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            p.kill()
        lg.close()
        return parse_client_log(path), p.returncode

    def stored(self, name):
        ids = set()
        for d in self.dirs.values():
            ids |= chunk_ids(os.path.join(d, name))
        return len(ids)

    def monitor(self, client, t0, name, events=(), tick=0.4, timeout=900):
        """Sample stored-chunk count while the client runs; fire events by time or count."""
        samples, pending = [], list(events)
        while client.poll() is None:
            t = time.time() - t0
            n = self.stored(name)
            samples.append((round(t, 2), n))
            for ev in list(pending):
                kind, val, fn = ev
                if (kind == "t" and t >= val) or (kind == "stored" and n >= val):
                    pending.remove(ev)
                    fn()
            if t > timeout:
                client.kill()
                break
            time.sleep(tick)
        samples.append((round(time.time() - t0, 2), self.stored(name)))
        return samples, pending

    def host_verify(self, src, dirs=None):
        dirs = dirs or list(self.dirs.values())
        r = subprocess.run([sys.executable, VERIFY_PY, src] + dirs, capture_output=True, text=True)
        return r.returncode == 0, (r.stdout.strip() or r.stderr.strip())

    def per_server(self, src):
        name = os.path.basename(src)
        return {n: len(chunk_ids(os.path.join(d, name))) for n, d in self.dirs.items()}

    def dump(self, tag):
        try:
            out = subprocess.run(["ovs-ofctl", "-O", "OpenFlow13", "dump-flows", "s1"],
                                 capture_output=True, text=True).stdout
        except FileNotFoundError:
            out = ""
        with open(os.path.join(OUT, "flows_%s.txt" % tag), "w") as f:
            f.write(out)
        port = flow_port(out)
        self.flows[tag] = PATHS.get(port, "unknown (port %s)" % port)

    def link(self, state):
        try:
            self.net.configLinkStatus("s2", "s4", state)
        except Exception as err:
            self.say("(link %s failed: %s)" % (state, err))

    # ------------------------------------------------------------ scenarios
    def sc_integrity(self):
        self.say("\n## 1. Integrity check (corrupted chunk)")
        self.reset("integrity")
        probe = os.path.join(OUT, "integrity_probe.py")
        with open(probe, "w") as f:
            f.write(PROBE)
        ip, port = SERVERS["h2"]
        out = self.host("h1").cmd("python3 %s %s %d" % (probe, ip, port)).strip()
        for line in out.splitlines():
            self.say("    " + line)
        stored = chunk_ids(os.path.join(self.dirs["h2"], "integrity_probe"))
        self.say("Chunks stored on h2 for the probe file: %s (the corrupted copy was never stored)" % sorted(stored))

    def sc_baseline(self):
        self.say("\n## 2. Baseline transfer (%d MB, 64 KB chunks, no failure)" % self.size_mb)
        src = self.make_file("data_main.bin", self.size_mb)
        self.reset("baseline")
        info, rc = self.run_client("baseline", src)
        ok, msg = self.host_verify(src)
        self.say("Transfer time    : %s s  (%s)" % (info["secs"], fmt(info["mbit"])))
        self.say("Chunks per server: %s" % self.per_server(src))
        self.say("Client check     : %s (exit code %s)" % ("verified" if info["verified"] else "NOT verified", rc))
        self.say("Rebuild check    : %s" % msg)
        self.say("Result           : %s" % ("PASS" if ok and info["verified"] else "FAIL"))

    def sc_chunks(self):
        self.say("\n## 3. Chunk-size comparison (5 MB file)")
        src = self.make_file("data_small.bin", 5)
        rows = []
        for size in (4096, 16384, 65536, 262144):
            self.reset("chunks_%d" % size)
            info, rc = self.run_client("chunks_%d" % size, src, ["--chunk-size", str(size)])
            ok, msg = self.host_verify(src)
            rows.append((size, info["secs"], info["mbit"], ok))
            self.say("chunk %7d B : %s s, %s, rebuilt %s" % (size, info["secs"], fmt(info["mbit"]), "OK" if ok else "FAIL"))
        with open(os.path.join(OUT, "chunk_size_results.csv"), "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["chunk_bytes", "seconds", "mbit_per_s", "verified"])
            w.writerows(rows)
        try:
            import matplotlib
            matplotlib.use("Agg")
            import matplotlib.pyplot as plt
            plt.figure(figsize=(6, 3.5))
            plt.bar([str(r[0] // 1024) + " KB" for r in rows], [r[2] or 0 for r in rows], color="#2a6fb0")
            plt.ylabel("Throughput (Mbit/s)")
            plt.xlabel("Chunk size")
            plt.title("Throughput vs chunk size (5 MB file)")
            plt.tight_layout()
            plt.savefig(os.path.join(OUT, "chunk_size.png"), dpi=150)
            plt.close()
        except Exception:
            self.say("(no chart: install matplotlib with  sudo apt install -y python3-matplotlib )")

    def sc_mirror(self):
        self.say("\n## 4. Mirror mode (--mirror 2)")
        src = self.make_file("data_small.bin", 5)
        self.reset("mirror")
        info, rc = self.run_client("mirror", src, ["--mirror", "2"])
        counts = self.per_server(src)
        total = len(set().union(*[chunk_ids(os.path.join(d, os.path.basename(src))) for d in self.dirs.values()]))
        ok2, m2 = self.host_verify(src, [self.dirs["h2"]])
        ok3, m3 = self.host_verify(src, [self.dirs["h3"]])
        self.say("Chunks per server: %s (file has %d chunks)" % (counts, total))
        self.say("h2 alone can rebuild the file: %s" % m2)
        self.say("h3 alone can rebuild the file: %s" % m3)
        self.say("Result           : %s" % ("PASS" if ok2 and ok3 and info["verified"] else "FAIL"))

    def sc_resume(self):
        self.say("\n## 5. Interrupted transfer + resume (client killed at ~40%)")
        src = self.make_file("data_resume.bin", 10)
        name = os.path.basename(src)
        total = (10 * 1024 * 1024 + DEFAULT_CHUNK - 1) // DEFAULT_CHUNK
        self.reset("resume")
        p, lg, path = self.start_client("resume_run1", src)
        killed = []
        samples, pending = self.monitor(p, time.time(), name,
                                        [("stored", int(total * 0.4), lambda: (killed.append(self.stored(name)), p.kill()))])
        lg.close()
        if pending:
            self.say("NOTE: the client finished before it could be killed; re-run with a bigger --size.")
        at_kill = self.stored(name)
        self.say("Client killed with %d of %d chunks stored" % (at_kill, total))
        info, rc = self.run_client("resume_run2", src)
        ok, msg = self.host_verify(src)
        self.say("Second run        : %s" % ("resumed: %d of %d chunks already stored" % info["resume"] if info["resume"] else "no resume line found"))
        self.say("Client check      : %s (exit code %s)" % ("verified" if info["verified"] else "NOT verified", rc))
        self.say("Rebuild check     : %s" % msg)
        self.say("Result            : %s" % ("PASS" if ok and info["verified"] and info["resume"] else "FAIL"))

    def sc_servercrash(self):
        self.say("\n## 6. Backup server crash mid-transfer (h3 killed at ~30%)")
        src = self.make_file("data_resume.bin", 10)
        name = os.path.basename(src)
        total = (10 * 1024 * 1024 + DEFAULT_CHUNK - 1) // DEFAULT_CHUNK
        self.reset("servercrash")
        p, lg, path = self.start_client("servercrash", src)
        samples, pending = self.monitor(p, time.time(), name,
                                        [("stored", int(total * 0.3), lambda: self.servers["h3"][0].kill())])
        lg.close()
        if pending:
            self.say("NOTE: the client finished before h3 could be killed; re-run with a bigger --size.")
        info = parse_client_log(path)
        ok_all, msg_all = self.host_verify(src)
        ok_h2, msg_h2 = self.host_verify(src, [self.dirs["h2"]])
        self.say("Servers marked DOWN by the client: %s" % (info["down"] or "none"))
        self.say("Chunks re-routed to the live server: %d" % info["rerouted"])
        self.say("Chunks per server: %s of %d" % (self.per_server(src), total))
        self.say("h2 alone can rebuild the file: %s" % msg_h2)
        self.say("Client check: %s" % ("verified" if info["verified"] else "NOT verified"))
        self.say("Result     : %s" % ("PASS" if ok_h2 and info["verified"] else "FAIL"))

    def sc_failover(self):
        self.say("\n## 7. SDN failover during transfer (%d MB; link s2-s4 down at %ds, up at %ds)" % (self.size_mb, T_DOWN, T_UP))
        src = self.make_file("data_main.bin", self.size_mb)
        name = os.path.basename(src)
        total = (self.size_mb * 1024 * 1024 + DEFAULT_CHUNK - 1) // DEFAULT_CHUNK
        self.flows, self.marks = {}, {}
        self.reset("failover")
        p, lg, path = self.start_client("failover", src)
        t0 = time.time()

        def mark(key, state):
            self.link(state)
            self.marks[key] = round(time.time() - t0, 1)
        events = [("t", 3, lambda: self.dump("before")),
                  ("t", T_DOWN, lambda: mark("down", "down")),
                  ("t", T_DOWN + 3, lambda: self.dump("during")),
                  ("t", T_UP, lambda: mark("up", "up")),
                  ("t", T_UP + 3, lambda: self.dump("after"))]
        try:
            samples, pending = self.monitor(p, t0, name, events)
        finally:
            self.link("up")
        lg.close()
        if pending:
            self.say("NOTE: transfer finished before all events fired. Use a bigger --size.")
        end = samples[-1][0]
        info = parse_client_log(path)
        ok, msg = self.host_verify(src)
        with open(os.path.join(OUT, "failover_timeline.csv"), "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["seconds", "chunks_stored", "MB_stored"])
            for t, n in samples:
                w.writerow([t, n, round(n * DEFAULT_CHUNK / 1e6, 2)])
        for tag in ("before", "during", "after"):
            self.say("Controller path, flows %-6s: %s" % (tag, self.flows.get(tag, "not captured")))
        self.say("Link down at %s s, up at %s s" % (self.marks.get("down"), self.marks.get("up")))
        self.say("Transfer finished at %.1f s, client retry lines: %d" % (end, info["retries"]))
        self.say("Throughput before failure : %s" % fmt(window_mbit(samples, 1, T_DOWN, DEFAULT_CHUNK)))
        self.say("Throughput during outage  : %s" % fmt(window_mbit(samples, T_DOWN + 2, min(T_UP, end), DEFAULT_CHUNK)))
        self.say("Throughput after restore  : %s" % fmt(window_mbit(samples, T_UP + 2, end, DEFAULT_CHUNK)))
        self.say("Chunks per server: %s of %d" % (self.per_server(src), total))
        self.say("Client check     : %s" % ("verified" if info["verified"] else "NOT verified"))
        self.say("Rebuild check    : %s" % msg)
        self.say("Result           : %s" % ("PASS" if ok and info["verified"] else "FAIL"))
        try:
            import matplotlib
            matplotlib.use("Agg")
            import matplotlib.pyplot as plt
            plt.figure(figsize=(7, 3.8))
            plt.plot([s[0] for s in samples], [s[1] * DEFAULT_CHUNK / 1e6 for s in samples], color="#2a6fb0", lw=2)
            if "down" in self.marks and "up" in self.marks:
                plt.axvspan(self.marks["down"], self.marks["up"], color="#e06666", alpha=0.2, label="link s2-s4 down (path B)")
                plt.legend(loc="upper left")
            plt.xlabel("Time (s)")
            plt.ylabel("Data stored on servers (MB)")
            plt.title("Backup progress with link failure")
            plt.tight_layout()
            plt.savefig(os.path.join(OUT, "failover_timeline.png"), dpi=150)
            plt.close()
        except Exception:
            self.say("(no chart: install matplotlib with  sudo apt install -y python3-matplotlib )")


def give_back(path):
    uid, gid = os.environ.get("SUDO_UID"), os.environ.get("SUDO_GID")
    if uid is None:
        return
    for root, dirs, files in os.walk(path):
        for name in [root] + [os.path.join(root, x) for x in dirs + files]:
            try:
                os.chown(name, int(uid), int(gid))
            except OSError:
                pass


def run_suite(net, size_mb, names):
    shutil.rmtree(OUT, ignore_errors=True)
    os.makedirs(OUT)
    lab = Lab(net, size_mb)
    lab.say("# Distributed Backup System - test results")
    lab.say("Test file size: %d MB, default chunk size %d KB" % (size_mb, DEFAULT_CHUNK // 1024))
    try:
        for n in names:
            try:
                getattr(lab, "sc_" + n)()
            except Exception:
                lab.say("SCENARIO %s CRASHED:\n%s" % (n, traceback.format_exc()))
    finally:
        lab.link("up")
        lab.stop_servers()
        with open(os.path.join(OUT, "summary.md"), "w") as f:
            f.write("\n".join(lab.report) + "\n")
    return lab


def main():
    ap = argparse.ArgumentParser(description="Distributed Backup System test suite")
    ap.add_argument("scenarios", nargs="*", help="any of: " + " ".join(ALL))
    ap.add_argument("--size", type=int, default=30, help="main test file size in MB (default 30)")
    args = ap.parse_args()
    names = args.scenarios or ALL
    for n in names:
        if n not in ALL:
            sys.exit("Unknown scenario %r. Choose from: %s" % (n, " ".join(ALL)))
    if os.geteuid() != 0:
        sys.exit("Run with sudo:  sudo python3 final_tests.py")

    from mininet.log import setLogLevel
    from topo import build
    setLogLevel("warning")
    net = build()
    net.start()
    try:
        print("Waiting 8 s for the controller to discover the topology ...", flush=True)
        time.sleep(8)
        got = re.search(r"(\d+) received", net.get("h1").cmd("ping -c 3 -W 2 10.0.0.2"))
        if not got or int(got.group(1)) == 0:
            print("Warm-up ping failed. Is the Ryu controller running in Terminal 1?")
            return
        run_suite(net, args.size, names)
    finally:
        net.stop()
        give_back(OUT)
    print("\nAll results saved in %s (summary.md, logs, csv, png)" % OUT)


if __name__ == "__main__":
    main()
