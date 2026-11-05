# Distributed Backup System - test results
Test file size: 30 MB, default chunk size 64 KB

## 1. Integrity check (corrupted chunk)
    corrupted chunk (wrong SHA-256) -> BAD
    same chunk, correct SHA-256     -> OK
    same chunk sent again           -> EXISTS
Chunks stored on h2 for the probe file: [0] (the corrupted copy was never stored)

## 2. Baseline transfer (30 MB, 64 KB chunks, no failure)
Transfer time    : 30.3 s  (8.29 Mbit/s)
Chunks per server: {'h2': 240, 'h3': 240}
Client check     : verified (exit code 0)
Rebuild check    : 480 chunks rebuilt, SHA-256 MATCHES the original
Result           : PASS

## 3. Chunk-size comparison (5 MB file)
chunk    4096 B : 31.6 s, 1.33 Mbit/s, rebuilt OK
chunk   16384 B : 13.8 s, 3.04 Mbit/s, rebuilt OK
chunk   65536 B : 6.2 s, 6.78 Mbit/s, rebuilt OK
chunk  262144 B : 6.4 s, 6.51 Mbit/s, rebuilt OK

## 4. Mirror mode (--mirror 2)
Chunks per server: {'h2': 80, 'h3': 80} (file has 80 chunks)
h2 alone can rebuild the file: 80 chunks rebuilt, SHA-256 MATCHES the original
h3 alone can rebuild the file: 80 chunks rebuilt, SHA-256 MATCHES the original
Result           : PASS

## 5. Interrupted transfer + resume (client killed at ~40%)
Client killed with 68 of 160 chunks stored
Second run        : resumed: 68 of 160 chunks already stored
Client check      : verified (exit code 0)
Rebuild check     : 160 chunks rebuilt, SHA-256 MATCHES the original
Result            : PASS

## 6. Backup server crash mid-transfer (h3 killed at ~30%)
Servers marked DOWN by the client: ['10.0.0.3:5000']
Chunks re-routed to the live server: 80
Chunks per server: {'h2': 160, 'h3': 24} of 160
h2 alone can rebuild the file: 160 chunks rebuilt, SHA-256 MATCHES the original
Client check: verified
Result     : PASS

## 7. SDN failover during transfer (30 MB; link s2-s4 down at 5s, up at 25s)
Controller path, flows before: path A (via s2)
Controller path, flows during: path B (via s3)
Controller path, flows after : path A (via s2)
Link down at 5.4 s, up at 25.5 s
Transfer finished at 47.9 s, client retry lines: 0
Throughput before failure : 7.16 Mbit/s
Throughput during outage  : 3.57 Mbit/s
Throughput after restore  : 6.83 Mbit/s
Chunks per server: {'h2': 240, 'h3': 240} of 480
Client check     : verified
Rebuild check    : 480 chunks rebuilt, SHA-256 MATCHES the original
Result           : PASS
