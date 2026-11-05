#!/usr/bin/env python3
"""
Ryu SDN controller for Topic 24: Distributed Backup System

What it does
  * Discovers the switch topology (LLDP, needs --observe-links).
  * Learns where hosts (client / backup servers) are attached.
  * Picks the lowest-cost path between two hosts (Dijkstra) and installs
    flow rules along it, in both directions.
  * When a link goes down or comes back up, it recomputes the paths and
    reinstalls the flows (failover / fail-back).

Run:
    source ~/ryu-env/bin/activate
    ryu-manager --observe-links controller.py
"""

import heapq

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, set_ev_cls
from ryu.lib.packet import ether_types, ethernet, packet
from ryu.ofproto import ofproto_v1_3
from ryu.topology import event

# Link cost = one-way delay in ms. Mirrors the delays set in topo.py:
#   path A: s1-s2 (5), s2-s4 (5)    -> total 10
#   path B: s1-s3 (20), s3-s4 (20)  -> total 40
# Edit this table if you change the topology. Unknown links cost DEFAULT_COST.
LINK_COST = {(1, 2): 5, (2, 4): 5, (1, 3): 20, (3, 4): 20}
DEFAULT_COST = 10

FLOW_PRIORITY = 10


def link_cost(a, b):
    return LINK_COST.get(tuple(sorted((a, b))), DEFAULT_COST)


def is_multicast(mac):
    return int(mac.split(":")[0], 16) & 1 == 1


def host_name(mac):
    # Mininet autoSetMacs: h1 -> 00:00:00:00:00:01, h2 -> ...:02, etc.
    if mac.startswith("00:00:00:00:00:"):
        return "h%d" % int(mac[-2:], 16)
    return mac


def fmt_path(path):
    return " -> ".join("s%d" % d for d in path)


class BackupPathController(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dps = {}          # dpid -> datapath
        self.sw_ports = {}     # dpid -> [port_no, ...]
        self.adj = {}          # dpid -> {neighbour dpid: out port towards it}
        self.link_ports = {}   # dpid -> set of ports that connect to other switches
        self.hosts = {}        # mac -> (dpid, port)
        self.pairs = set()     # (mac_a, mac_b) sorted: host pairs with flows

    # ------------------------------------------------------------------
    # Switch connection
    # ------------------------------------------------------------------
    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def _features(self, ev):
        dp = ev.msg.datapath
        ofp, parser = dp.ofproto, dp.ofproto_parser
        self.dps[dp.id] = dp

        # clear stale flows from a previous controller run
        dp.send_msg(parser.OFPFlowMod(
            datapath=dp, table_id=ofp.OFPTT_ALL, command=ofp.OFPFC_DELETE,
            out_port=ofp.OFPP_ANY, out_group=ofp.OFPG_ANY,
            match=parser.OFPMatch()))

        # table-miss: send unknown packets to the controller
        actions = [parser.OFPActionOutput(ofp.OFPP_CONTROLLER,
                                          ofp.OFPCML_NO_BUFFER)]
        self._add_flow(dp, 0, parser.OFPMatch(), actions)

    @set_ev_cls(event.EventSwitchEnter)
    def _switch_enter(self, ev):
        dp = ev.switch.dp
        self.dps[dp.id] = dp
        self.sw_ports[dp.id] = [p.port_no for p in ev.switch.ports]
        self.logger.info("SWITCH s%d connected", dp.id)

    @set_ev_cls(event.EventSwitchLeave)
    def _switch_leave(self, ev):
        dpid = ev.switch.dp.id
        self.dps.pop(dpid, None)
        self.sw_ports.pop(dpid, None)
        self.logger.info("SWITCH s%d left", dpid)

    # ------------------------------------------------------------------
    # Topology changes -> recompute paths
    # ------------------------------------------------------------------
    @set_ev_cls(event.EventLinkAdd)
    def _link_add(self, ev):
        s, d = ev.link.src, ev.link.dst
        self.adj.setdefault(s.dpid, {})[d.dpid] = s.port_no
        self.link_ports.setdefault(s.dpid, set()).add(s.port_no)
        self.link_ports.setdefault(d.dpid, set()).add(d.port_no)

        # a host "learned" on what turns out to be a switch-to-switch port
        # was a mistake; forget it
        bad = {(s.dpid, s.port_no), (d.dpid, d.port_no)}
        for mac, loc in list(self.hosts.items()):
            if loc in bad:
                del self.hosts[mac]

        self.logger.info("LINK UP    s%d -> s%d", s.dpid, d.dpid)
        self._reroute_all()

    @set_ev_cls(event.EventLinkDelete)
    def _link_delete(self, ev):
        s, d = ev.link.src, ev.link.dst
        self.adj.get(s.dpid, {}).pop(d.dpid, None)
        self.adj.get(d.dpid, {}).pop(s.dpid, None)  # link is down both ways
        self.logger.info("LINK DOWN  s%d -> s%d", s.dpid, d.dpid)
        self._reroute_all()

    # ------------------------------------------------------------------
    # Packets from the switches
    # ------------------------------------------------------------------
    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in(self, ev):
        msg = ev.msg
        dp = msg.datapath
        parser = dp.ofproto_parser
        in_port = msg.match["in_port"]

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocol(ethernet.ethernet)
        if eth is None or eth.ethertype not in (ether_types.ETH_TYPE_IP,
                                                ether_types.ETH_TYPE_ARP):
            return  # ignore LLDP, IPv6 chatter, etc.
        if in_port in self.link_ports.get(dp.id, ()):
            return  # packet arrived on a switch-to-switch port: not a host

        src, dst = eth.src, eth.dst

        # learn / update where the sender lives
        loc = (dp.id, in_port)
        if self.hosts.get(src) != loc:
            moved = src in self.hosts
            self.hosts[src] = loc
            self.logger.info("HOST %s at s%d port %d", host_name(src),
                             dp.id, in_port)
            if moved:
                self._reroute_all()

        # unknown / broadcast destination: flood on host-facing ports only
        if is_multicast(dst) or dst not in self.hosts:
            self._flood_edges(msg, dp.id, in_port)
            return

        path = self._install_pair(src, dst)
        if path is None:
            return

        # forward the packet that triggered this
        out_port = (self.adj[path[0]][path[1]] if len(path) > 1
                    else self.hosts[dst][1])
        dp.send_msg(parser.OFPPacketOut(
            datapath=dp, buffer_id=dp.ofproto.OFP_NO_BUFFER, in_port=in_port,
            actions=[parser.OFPActionOutput(out_port)], data=msg.data))

    def _flood_edges(self, msg, src_dpid, src_port):
        """Send the packet out of every host-facing port (never onto
        switch-to-switch links, so the loop in the topology is harmless)."""
        for dpid, dp in self.dps.items():
            ofp, parser = dp.ofproto, dp.ofproto_parser
            actions = []
            for port_no in self.sw_ports.get(dpid, []):
                if port_no > ofp.OFPP_MAX:
                    continue
                if port_no in self.link_ports.get(dpid, ()):
                    continue
                if dpid == src_dpid and port_no == src_port:
                    continue
                actions.append(parser.OFPActionOutput(port_no))
            if actions:
                dp.send_msg(parser.OFPPacketOut(
                    datapath=dp, buffer_id=ofp.OFP_NO_BUFFER,
                    in_port=ofp.OFPP_CONTROLLER, actions=actions,
                    data=msg.data))

    # ------------------------------------------------------------------
    # Path computation and flow installation
    # ------------------------------------------------------------------
    def _dijkstra(self, src, dst):
        """Lowest-cost switch path src -> dst. Returns (path, cost) or None."""
        if src == dst:
            return [src], 0
        dist = {src: 0}
        prev = {}
        visited = set()
        heap = [(0, src)]
        while heap:
            d, u = heapq.heappop(heap)
            if u in visited:
                continue
            visited.add(u)
            if u == dst:
                break
            for v in self.adj.get(u, {}):
                if u not in self.adj.get(v, {}):
                    continue  # use a link only if it is up in BOTH directions
                nd = d + link_cost(u, v)
                if nd < dist.get(v, float("inf")):
                    dist[v] = nd
                    prev[v] = u
                    heapq.heappush(heap, (nd, v))
        if dst not in visited:
            return None
        path = [dst]
        while path[-1] != src:
            path.append(prev[path[-1]])
        return path[::-1], dist[dst]

    def _install_pair(self, a, b):
        """Install flows a->b and b->a along the best path. Returns the
        switch path from a's switch to b's switch, or None if unreachable."""
        key = tuple(sorted((a, b)))
        self.pairs.add(key)  # stays tracked so a restored link can fix it

        (da, pa), (db, pb) = self.hosts[a], self.hosts[b]
        res = self._dijkstra(da, db)
        if res is None:
            self.logger.warning("NO PATH %s <-> %s", host_name(a), host_name(b))
            return None
        path, cost = res

        self._install_direction(a, b, path, pb)
        self._install_direction(b, a, path[::-1], pa)
        self.logger.info("PATH %s <-> %s : %s  (cost %d)",
                         host_name(a), host_name(b), fmt_path(path), cost)
        return path

    def _install_direction(self, src, dst, path, dst_port):
        for i, dpid in enumerate(path):
            dp = self.dps.get(dpid)
            if dp is None:
                continue
            parser = dp.ofproto_parser
            out_port = (dst_port if i == len(path) - 1
                        else self.adj[dpid][path[i + 1]])
            match = parser.OFPMatch(eth_src=src, eth_dst=dst)
            actions = [parser.OFPActionOutput(out_port)]
            self._add_flow(dp, FLOW_PRIORITY, match, actions)

    def _remove_pair_flows(self, a, b):
        for dp in self.dps.values():
            ofp, parser = dp.ofproto, dp.ofproto_parser
            for s, d in ((a, b), (b, a)):
                dp.send_msg(parser.OFPFlowMod(
                    datapath=dp, command=ofp.OFPFC_DELETE,
                    out_port=ofp.OFPP_ANY, out_group=ofp.OFPG_ANY,
                    match=parser.OFPMatch(eth_src=s, eth_dst=d)))

    def _reroute_all(self):
        """Topology changed: recompute every known host pair's path."""
        for key in list(self.pairs):
            a, b = key
            self._remove_pair_flows(a, b)
            if a in self.hosts and b in self.hosts:
                self._install_pair(a, b)
            else:
                self.pairs.discard(key)

    def _add_flow(self, dp, priority, match, actions):
        ofp, parser = dp.ofproto, dp.ofproto_parser
        inst = [parser.OFPInstructionActions(ofp.OFPIT_APPLY_ACTIONS, actions)]
        dp.send_msg(parser.OFPFlowMod(datapath=dp, priority=priority,
                                      match=match, instructions=inst))
