# Distributed Backup System (CN Mini-Project, Topic 24)

A client splits a file into chunks, adds a SHA-256 checksum to each chunk and stores the chunks on two backup servers over TCP. The servers run in a Mininet network controlled by a Ryu SDN controller (OpenFlow 1.3) that picks paths by link delay and reroutes traffic when a link fails.

## Team

| Name | SRN |
|---|---|
| Rishabh Khetan | PES2UG25CS438 |
| Preksha Nehru | PES2UG25CS406 |
| Rishitha Adiga | PES2UG25CS443 |

## Topology

h1 (client) connects to s1. h2 and h3 (backup servers) connect to s4.

- Path A (s1-s2-s4): 10 Mbit/s, 5 ms per link (primary, cost 10)
- Path B (s1-s3-s4): 5 Mbit/s, 20 ms per link (backup, cost 40)
- Access links (hosts to switches): 100 Mbit/s

## Features

- TCP binary protocol with per-chunk SHA-256 verification (BAD, OK or EXISTS reply)
- Atomic chunk storage on the server (temporary file, fsync, rename)
- Resume: the server lists the chunks it already holds and the client skips them
- Mirror mode (--mirror 2): every chunk stored on both servers
- Server crash handling: chunks are re-routed to the remaining server
- Whole-file verification at the end of a transfer
- SDN controller with Dijkstra shortest path by delay and automatic failover

## Files

- topo.py: Mininet diamond topology
- controller.py: Ryu controller (shortest path by delay, failover)
- server.py: backup server (per-file folders, atomic chunk storage, SHA-256 check)
- client.py: backup client (parallel per-server workers, resume, mirror, re-routing, verification)
- verify_backup.py: rebuilds a file from the server folders and compares its SHA-256
- failover_demo.py: network-level failover test with a continuous ping
- final_tests.py: automated seven-scenario test suite
- results/final/: measured results (summary.md, csv files, charts)

## Requirements

Ubuntu, Mininet, Open vSwitch, Ryu (in a Python virtualenv), Python 3. matplotlib is optional and only used for the charts.

## How to run

Terminal 1 (controller):

    source ~/ryu-env/bin/activate
    cd ~/CN_project
    ryu-manager --observe-links controller.py

Terminal 2 (clean up first, then run the tests):

    sudo mn -c
    sudo python3 final_tests.py

Run sudo mn -c before starting the controller. It also kills ryu-manager if that is already running.

The full suite takes about 5 minutes. To run only some scenarios, name them:

    sudo python3 final_tests.py integrity baseline --size 5

Available scenarios: integrity, baseline, chunks, mirror, resume, servercrash, failover.

Running the suite replaces the contents of results/final/.

### Manual run

Start the topology with sudo python3 topo.py. At the mininet prompt, start a server on each of h2 and h3 with its own folder:

    h2 python3 server.py --dir /tmp/h2dir --port 5000 &
    h3 python3 server.py --dir /tmp/h3dir --port 5000 &

Then send a file from h1:

    h1 python3 client.py FILE --chunk-size 4096

The default chunk size is 64 KB. Add --mirror 2 to store two copies of every chunk. To check the result, run verify_backup.py with the original file followed by the two server folders:

    python3 verify_backup.py FILE /tmp/h2dir /tmp/h3dir

## Results (Mininet)

| Test | Result |
|---|---|
| Baseline (30 MB, 64 KB chunks) | 30.3 s, 8.29 Mbit/s over the 10 Mbit/s path, SHA-256 verified |
| Chunk size 4 KB / 16 KB / 64 KB / 256 KB (5 MB file) | 1.33 / 3.04 / 6.78 / 6.51 Mbit/s |
| Integrity | Corrupted chunk rejected (BAD), valid chunk stored (OK), duplicate acknowledged (EXISTS) |
| Mirror mode | 80 chunks on each server, each server alone rebuilds the file |
| Resume | Client killed at 68 of 160 chunks, second run resumed and verified |
| Server crash | h3 killed at about 30 percent, 80 chunks re-routed to h2, file rebuilt from h2 alone |
| SDN failover (30 MB, link s2-s4 down for 20 s) | Controller moved traffic to path B and back, 0 client retries, 7.16 / 3.57 / 6.83 Mbit/s before / during / after |
| Network-level failover (ping every 0.2 s) | 170 of 170 replies, median RTT 21.5 ms on path A and 82.0 ms on path B, longest gap 264 ms |

Full output: results/final/summary.md. Charts: results/final/chunk_size.png and results/final/failover_timeline.png.
