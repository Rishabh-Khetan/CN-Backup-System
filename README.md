# Distributed Backup System (CN Mini-Project, Topic 24)

A client splits a file into chunks, adds a SHA-256 checksum to each chunk and stores them on two backup servers over TCP. The servers run in a Mininet network controlled by a Ryu SDN controller (OpenFlow 1.3) that picks paths by link delay and reroutes when a link fails.

Team: Rishabh Khetan (PES2UG25CS438), Preksha Nehru (PES2UG25CS406), Rishitha Adiga (PES2UG25CS443)

## Topology

h1 (client) connects to s1. h2 and h3 (backup servers) connect to s4.
Path A (s1-s2-s4): 10 Mbit/s, 5 ms per link (primary).
Path B (s1-s3-s4): 5 Mbit/s, 20 ms per link (backup).

## Files

- topo.py: Mininet diamond topology
- controller.py: Ryu controller (shortest path by delay, failover)
- server.py: backup server (per-file folders, atomic chunk storage, SHA-256 check)
- client.py: backup client (parallel per-server workers, resume, mirror, re-routing, verification)
- verify_backup.py: rebuilds a file from the server folders and compares SHA-256
- failover_demo.py: network-level failover test with continuous ping
- final_tests.py: automated seven-scenario test suite
- results/final/: measured results (summary.md, csv, png)

## Requirements

Ubuntu, Mininet, Open vSwitch, Ryu (in a Python 3.9 virtualenv), Python 3, matplotlib (optional, for charts).

## How to run

Terminal 1 (controller):

    source ~/ryu-env/bin/activate
    cd ~/CN_project
    ryu-manager --observe-links controller.py

Terminal 2 (clean up first, then tests):

    sudo mn -c
    sudo python3 final_tests.py

Do not run sudo mn -c while the controller is running; it kills ryu-manager.

Manual run: start sudo python3 topo.py, then at the mininet prompt start the servers on h2 and h3
(python3 server.py --dir <folder> --port 5000), and run on h1:
python3 client.py <file> --chunk-size 4096 (default chunk size is 64 KB; add --mirror 2 for two copies).

## Results (Mininet, 30 MB file)

- Baseline: 8.29 Mbit/s over a 10 Mbit/s path, SHA-256 verified
- Chunk size: 4 KB 1.33, 16 KB 3.04, 64 KB 6.78, 256 KB 6.51 Mbit/s (5 MB file)
- Corrupted chunk rejected (BAD), duplicate acknowledged (EXISTS)
- Resume after the client was killed at 68 of 160 chunks: completed and verified
- Mirror mode: each server alone can rebuild the file
- Server crash: chunks re-routed to the live server, file rebuilt
- Link s2-s4 down for 20 s: controller moved traffic to path B and back, 0 client retries, 7.16 / 3.57 / 6.83 Mbit/s before / during / after
