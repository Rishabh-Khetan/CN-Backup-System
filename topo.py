#!/usr/bin/env python3
"""
Topology for Topic 24: Distributed Backup System

            +-- s2 --+            path A: s1-s2-s4  (10 Mbps, 5 ms)   <- faster
  h1 -- s1 -+        +-- s4 -+-- h2 (backup server 1)
            +-- s3 --+       +-- h3 (backup server 2)
                              path B: s1-s3-s4  (5 Mbps, 20 ms)   <- slower

Run (controller must already be running):
    Terminal 1:  ryu-manager --observe-links controller.py
    Terminal 2:  sudo python3 topo.py
"""

from mininet.net import Mininet
from mininet.node import RemoteController, OVSSwitch
from mininet.link import TCLink
from mininet.cli import CLI
from mininet.log import setLogLevel, info


def build():
    net = Mininet(
        controller=None,          # we attach Ryu ourselves below
        switch=OVSSwitch,
        link=TCLink,              # needed for bw / delay on links
        autoSetMacs=True,         # h1 -> 00:00:00:00:00:01, etc.
        autoStaticArp=True,       # static ARP: avoids broadcast storms on the loop
    )

    info("*** Adding controller\n")
    net.addController("c0", controller=RemoteController, ip="127.0.0.1", port=6653)

    info("*** Adding switches\n")
    sw_opts = dict(cls=OVSSwitch, protocols="OpenFlow13", failMode="secure")
    s1 = net.addSwitch("s1", **sw_opts)
    s2 = net.addSwitch("s2", **sw_opts)
    s3 = net.addSwitch("s3", **sw_opts)
    s4 = net.addSwitch("s4", **sw_opts)

    info("*** Adding hosts\n")
    h1 = net.addHost("h1", ip="10.0.0.1/24")   # client
    h2 = net.addHost("h2", ip="10.0.0.2/24")   # backup server 1
    h3 = net.addHost("h3", ip="10.0.0.3/24")   # backup server 2

    info("*** Adding links\n")
    # access links
    net.addLink(h1, s1, bw=100)
    net.addLink(h2, s4, bw=100)
    net.addLink(h3, s4, bw=100)

    # path A: faster, low delay
    net.addLink(s1, s2, bw=10, delay="5ms")
    net.addLink(s2, s4, bw=10, delay="5ms")

    # path B: slower, higher delay
    net.addLink(s1, s3, bw=5, delay="20ms")
    net.addLink(s3, s4, bw=5, delay="20ms")

    return net


def run():
    net = build()
    net.start()

    info("\n*** Topology is up.\n")
    info("    Useful CLI commands:\n")
    info("      pingall\n")
    info("      h1 ping -c 3 h2\n")
    info("      link s2 s4 down      (simulate path A failure)\n")
    info("      link s2 s4 up        (restore it)\n")
    info("      sh ovs-ofctl -O OpenFlow13 dump-flows s1\n\n")

    CLI(net)
    net.stop()


if __name__ == "__main__":
    setLogLevel("info")
    run()
