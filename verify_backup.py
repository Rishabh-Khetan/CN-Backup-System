"""Rebuild a backed-up file from the chunk folders of the servers and compare it with the original.

usage: python3 verify_backup.py <original_file> <server_dir> [<server_dir> ...]
Each server_dir is that server's --dir; chunks live in <server_dir>/<filename>/chunk_<n>.
Chunks are merged across the folders, so this works for both spread and mirror mode.
"""
import hashlib
import os
import sys


def sha256_of(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def main():
    if len(sys.argv) < 3:
        print(__doc__)
        return 2
    original, dirs = sys.argv[1], sys.argv[2:]
    name = os.path.basename(original)
    chunks = {}
    for d in dirs:
        folder = os.path.join(d, name)
        if not os.path.isdir(folder):
            continue
        for entry in os.listdir(folder):
            if entry.startswith("chunk_") and entry[6:].isdigit():
                chunks.setdefault(int(entry[6:]), os.path.join(folder, entry))
    if not chunks:
        print("FAIL: no chunks found")
        return 1
    missing = [i for i in range(max(chunks) + 1) if i not in chunks]
    if missing:
        print("FAIL: missing chunks", missing[:20])
        return 1
    h = hashlib.sha256()
    for i in range(max(chunks) + 1):
        with open(chunks[i], "rb") as f:
            h.update(f.read())
    ok = h.hexdigest() == sha256_of(original)
    print("%d chunks rebuilt, SHA-256 %s the original" % (len(chunks), "MATCHES" if ok else "DIFFERS from"))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
