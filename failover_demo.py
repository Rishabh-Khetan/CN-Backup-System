#!/usr/bin/env python3
"""
Automated failover test for Topic 24 (Distributed Backup System).

Builds the topology from topo.py, starts a continuous ping h1 -> h2, takes the
fast path (s2-s4) down at a fixed time, brings it back later, and measures
what the traffic did. Nothing to type during the run, so the timing is always
right.

Needs the Ryu controller already running:
    Terminal 1:  ryu-manager --observe-links controller.py
    Terminal 3:  sudo python3 failover_demo.py     (exit any open mininet> first)

Results are saved in ./results/
"""
import os
import re
import statistics
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "results")

PING_COUNT = 170          # 170 x 0.2 s = 34 s of traffic
PING_INTERVAL = 0.2
T_DUMP_BEFORE = 8         # all times are seconds after the ping starts
T_LINK_DOWN = 10
T_DUMP_DURING = 12
T_LINK_UP = 22
T_DUMP_AFTER = 24
T_END = 37

PATHS = {2: "path A (via s2, cost 10)", 3: "path B (via s3, cost 40)"}
REPLY = re.compile(r"^\[(\d+\.\d+)\].*icmp_seq=(\d+).*time=([\d.]+) ms")


def analyze(ping_text, t0, t_down, t_up, count):
    """Turn `ping -D` output into numbers for before / during / after."""
    replies = []
    for line in ping_text.splitlines():
        m = REPLY.match(line)
        if m:
            replies.append((float(m.group(1)), int(m.group(2)), float(m.group(3))))
    replies.sort()
    lost = sorted(set(range(1, count + 1)) - {r[1] for r in replies})

    def median(lo, hi):
        vals = [r[2] for r in replies if lo <= r[0] < hi]
        return statistics.median(vals) if vals else None

    def max_gap(center):
        ts = [r[0] for r in replies if center - 1 <= r[0] <= center + 5]
        return max((b - a for a, b in zip(ts, ts[1:])), default=None)

    return {
        "sent": count, "received": len(replies), "lost": lost,
        "rtt_before": median(t0, t_down - 0.5),
        "rtt_during": median(t_down + 2, t_up - 0.5),
        "rtt_after": median(t_up + 2, float("inf")),
        "gap_down": max_gap(t_down), "gap_up": max_gap(t_up),
    }


def fmt(v, unit=" ms", digits=1):
    return "n/a" if v is None else ("%." + str(digits) + "f%s") % (v, unit)


def report(res):
    lost = res["lost"]
    lost_txt = "none" if not lost else "%d (seq %s%s)" % (
        len(lost), ", ".join(map(str, lost[:10])), " ..." if len(lost) > 10 else "")
    return [
        "Packets sent / received    : %d / %d" % (res["sent"], res["received"]),
        "Packets lost               : %s" % lost_txt,
        "Median RTT before failure  : %s   (expect ~22 ms, path A)" % fmt(res["rtt_before"]),
        "Median RTT during outage   : %s   (expect ~85 ms, path B)" % fmt(res["rtt_during"]),
        "Median RTT after restore   : %s   (expect ~22 ms, path A)" % fmt(res["rtt_after"]),
        "Longest gap between replies at failure : %s   (normal is 200 ms)"
        % fmt(res["gap_down"] and res["gap_down"] * 1000, " ms", 0),
        "Longest gap between replies at restore : %s"
        % fmt(res["gap_up"] and res["gap_up"] * 1000, " ms", 0),
    ]


def h1_to_h2_port(dump_text):
    """Output port of the s1 flow for h1 -> h2 (2 = towards s2, 3 = towards s3)."""
    for line in dump_text.splitlines():
        if "dl_src=00:00:00:00:00:01,dl_dst=00:00:00:00:00:02" in line:
            m = re.search(r'output:"?(?:s1-eth)?(\d+)', line)
            if m:
                return int(m.group(1))
    return None


def at(t0, seconds):
    time.sleep(max(0.0, t0 + seconds - time.time()))


def run():
    from mininet.log import setLogLevel
    from topo import build

    setLogLevel("warning")
    os.makedirs(OUT, exist_ok=True)
    net = build()
    net.start()
    summary = []
    try:
        h1 = net.get("h1")
        print("Waiting 8 s for the controller to discover the topology ...")
        time.sleep(8)
        warm = h1.cmd("ping -c 3 -W 2 10.0.0.2")
        got = re.search(r"(\d+) received", warm)
        if not got or int(got.group(1)) == 0:
            print("Warm-up ping failed. Is the Ryu controller running in Terminal 1?")
            return

        def dump(tag):
            out = subprocess.run(
                ["ovs-ofctl", "-O", "OpenFlow13", "dump-flows", "s1"],
                capture_output=True, text=True).stdout
            with open(os.path.join(OUT, "flows_%s.txt" % tag), "w") as f:
                f.write(out)
            port = h1_to_h2_port(out)
            line = "s1 flow h1->h2, %-6s: %s" % (tag, PATHS.get(port, "output port %s" % port))
            print(line)
            summary.append(line)

        print("Test running (about 40 s) ...")
        ping_path = os.path.join(OUT, "ping.txt")
        t0 = time.time()
        with open(ping_path, "w") as f:
            ping = h1.popen("ping -D -c %d -i %s 10.0.0.2" % (PING_COUNT, PING_INTERVAL),
                            stdout=f, stderr=subprocess.STDOUT)
            at(t0, T_DUMP_BEFORE)
            dump("before")
            at(t0, T_LINK_DOWN)
            t_down = time.time()
            net.configLinkStatus("s2", "s4", "down")
            print(">>> link s2-s4 DOWN")
            at(t0, T_DUMP_DURING)
            dump("during")
            at(t0, T_LINK_UP)
            t_up = time.time()
            net.configLinkStatus("s2", "s4", "up")
            print(">>> link s2-s4 UP")
            at(t0, T_DUMP_AFTER)
            dump("after")
            at(t0, T_END)
            try:
                ping.wait(timeout=10)
            except subprocess.TimeoutExpired:
                ping.kill()

        with open(ping_path) as f:
            res = analyze(f.read(), t0, t_down, t_up, PING_COUNT)
        summary += [""] + report(res)
        print("\n" + "\n".join(summary))
        with open(os.path.join(OUT, "summary.txt"), "w") as f:
            f.write("\n".join(summary) + "\n")
        print("\nSaved in %s" % OUT)
    finally:
        net.stop()


if __name__ == "__main__":
    if os.geteuid() != 0:
        sys.exit("Run with sudo:  sudo python3 failover_demo.py")
    run()
