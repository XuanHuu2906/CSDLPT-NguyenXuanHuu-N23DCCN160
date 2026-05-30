import asyncio
import json
import sys
import time
import uuid

HEARTBEAT_INTERVAL = 5
PONG_TIMEOUT = 2.0


class P2PNode:
    def __init__(self, peer_id, port, neighbors, local_files):
        self.peer_id = peer_id
        self.port = port
        self.neighbors = neighbors
        self.local_files = local_files

        # --- QUERY state ---
        self.seen_queries = set()
        self.query_route_table = {}
        self.processed_queryhits = set()

        # --- Churn / dead-neighbor detection ---
        self.dead_neighbors = set()
        self.failed_forward_count = 0
        self.last_seen = {}         # neighbor_port -> timestamp

        # --- Metrics (origin) ---
        self.matched_peer_ports = set()
        self.matched_peers_count = 0
        self.queryhit_count = 0
        self.duplicate_queryhits_dropped = 0
        self.latencies = []         # ms per QUERYHIT (at origin)
        self.hops = []              # hop count per QUERYHIT (at origin)

        # --- Metrics (global) ---
        self.messages_sent = 0
        self.duplicate_queries_dropped = 0

        # --- Latency measurement ---
        self.query_start_time = {}  # query_id -> perf_counter()

        # --- Control ---
        self.shutdown_event = asyncio.Event()

    # -----------------------------------------------------------------
    # Lifecycle
    # -----------------------------------------------------------------
    async def start(self):
        server = await asyncio.start_server(
            self.handle_connection,
            host='127.0.0.1',
            port=self.port,
            reuse_address=True
        )
        print(f"[Peer {self.port}] Listening on port {self.port}")

        heartbeat_task = asyncio.create_task(self.heartbeat_loop())

        async with server:
            await self.shutdown_event.wait()

        heartbeat_task.cancel()
        try:
            await heartbeat_task
        except asyncio.CancelledError:
            pass
        print(f"[Peer {self.port}] Stopped.")

    # -----------------------------------------------------------------
    # Connection / message dispatch
    # -----------------------------------------------------------------
    async def handle_connection(self, reader, writer):
        try:
            data = await reader.readline()
            if not data:
                return
            message = json.loads(data.decode().strip())
            msg_type = message.get("type")

            if msg_type == "PING":
                # Respond on the same connection
                pong = {"type": "PONG", "sender_port": self.port}
                writer.write((json.dumps(pong) + "\n").encode())
                await writer.drain()
            else:
                await self.handle_message(message)

        except json.JSONDecodeError:
            print(f"[Peer {self.port}] Invalid JSON — ignored")
        except Exception as e:
            print(f"[Peer {self.port}] Error: {e}")
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def handle_message(self, message):
        msg_type = message.get("type")
        if msg_type == "QUERY":
            await self.handle_query(message)
        elif msg_type == "QUERYHIT":
            await self.handle_queryhit(message)
        elif msg_type == "METRICS_REQUEST":
            await self.handle_metrics_request(message)
        elif msg_type == "RESET":
            self.reset_state()
        elif msg_type == "SHUTDOWN":
            print(f"[Peer {self.port}] Shutting down...")
            self.shutdown_event.set()

    # -----------------------------------------------------------------
    # PING / PONG heartbeat
    # -----------------------------------------------------------------
    async def ping_neighbor(self, neighbor_port):
        """Returns True if neighbor responds with PONG within timeout."""
        try:
            reader, writer = await asyncio.open_connection(
                '127.0.0.1', neighbor_port
            )
            msg = {"type": "PING", "sender_port": self.port}
            writer.write((json.dumps(msg) + "\n").encode())
            await writer.drain()

            data = await asyncio.wait_for(
                reader.readline(), timeout=PONG_TIMEOUT
            )
            writer.close()
            await writer.wait_closed()

            response = json.loads(data.decode().strip())
            return response.get("type") == "PONG"
        except Exception:
            return False

    async def heartbeat_loop(self):
        """Periodically PING all neighbors to detect dead nodes."""
        while not self.shutdown_event.is_set():
            for nb in list(self.neighbors):
                alive = await self.ping_neighbor(nb)
                now = time.time()
                if alive:
                    self.last_seen[nb] = now
                    if nb in self.dead_neighbors:
                        # revived
                        print(
                            f"[Peer {self.port}] Neighbor {nb} is alive again. "
                            f"Removed from dead list."
                        )
                        self.dead_neighbors.discard(nb)
                else:
                    if nb not in self.dead_neighbors:
                        self.dead_neighbors.add(nb)
                        print(
                            f"[Peer {self.port}] Neighbor {nb} failed heartbeat. "
                            f"Marked as dead."
                        )
            await asyncio.sleep(HEARTBEAT_INTERVAL)

    # -----------------------------------------------------------------
    # QUERY handling (flooding)
    # -----------------------------------------------------------------
    async def handle_query(self, message):
        query_id = message["query_id"]
        keyword = message["keyword"]
        ttl = message["ttl"]
        origin_port = message["origin_port"]
        sender_port = message["sender_port"]
        current_path = message.get("path", [])

        short_qid = query_id.split("-")[0]

        # Duplicate suppression
        if query_id in self.seen_queries:
            self.duplicate_queries_dropped += 1
            print(
                f"[Peer {self.port}] Dropped duplicate QUERY "
                f"qid={short_qid}"
            )
            return

        self.seen_queries.add(query_id)
        self.query_route_table[query_id] = sender_port

        print(
            f"[Peer {self.port}] Received QUERY qid={short_qid} "
            f"ttl={ttl} from={sender_port} keyword={keyword}"
        )

        # Record start time at origin
        if self.port == origin_port and query_id not in self.query_start_time:
            self.query_start_time[query_id] = time.perf_counter()

        # Check local files
        matched_files = [f for f in self.local_files if keyword in f]
        for file in matched_files:
            hop_count = len(current_path)
            queryhit = {
                "type": "QUERYHIT",
                "query_id": query_id,
                "found_file": file,
                "found_at_port": self.port,
                "origin_port": origin_port,
                "hop_count": hop_count,
                "path": current_path,
                "timestamp_ms": int(time.time() * 1000)
            }
            await self.send_message(sender_port, queryhit)
            print(
                f"[Peer {self.port}] MATCH {file} for qid={short_qid}, "
                f"sending QUERYHIT back to={sender_port} hops={hop_count}"
            )

        # Forward if TTL > 0
        if ttl > 0:
            new_path = list(current_path) + [self.port]
            forward_msg = {
                "type": "QUERY",
                "query_id": query_id,
                "keyword": keyword,
                "ttl": ttl - 1,
                "initial_ttl": message.get("initial_ttl", ttl),
                "origin_port": origin_port,
                "sender_port": self.port,
                "path": new_path
            }
            for nb in self.neighbors:
                if nb == sender_port:
                    continue
                if nb in self.dead_neighbors:
                    print(
                        f"[Peer {self.port}] Skip dead neighbor {nb} "
                        f"while forwarding qid={short_qid}"
                    )
                    continue

                ok = await self.send_message(nb, forward_msg)
                if ok:
                    self.messages_sent += 1
                else:
                    self.failed_forward_count += 1
                    if nb not in self.dead_neighbors:
                        self.dead_neighbors.add(nb)
                        print(
                            f"[Peer {self.port}] Neighbor {nb} unreachable. "
                            f"Marked as dead."
                        )

    # -----------------------------------------------------------------
    # QUERYHIT handling (reverse-path routing)
    # -----------------------------------------------------------------
    async def handle_queryhit(self, message):
        query_id = message["query_id"]
        found_at = message["found_at_port"]
        origin_port = message["origin_port"]

        short_qid = query_id.split("-")[0]

        # Dedup key = (query_id, found_at_port)
        key = (query_id, found_at)
        if key in self.processed_queryhits:
            self.duplicate_queryhits_dropped += 1
            return
        self.processed_queryhits.add(key)

        if self.port == origin_port:
            if query_id not in self.seen_queries:
                return

            # ----- origin receives result -----
            if found_at not in self.matched_peer_ports:
                self.matched_peer_ports.add(found_at)
                self.matched_peers_count += 1

            self.queryhit_count += 1

            hop_count = message.get("hop_count", 0)
            self.hops.append(hop_count)

            # Latency from query start
            start = self.query_start_time.get(query_id)
            if start is not None:
                latency_ms = (time.perf_counter() - start) * 1000
                self.latencies.append(latency_ms)
            else:
                latency_ms = 0.0

            print(
                f"[Peer {self.port}] RESULT qid={short_qid} "
                f"found_at={found_at} hops={hop_count} "
                f"latency={latency_ms:.1f}ms "
                f"| total matched: {self.matched_peers_count}"
            )
            return

        # Intermediate peer — route QUERYHIT back toward origin
        next_hop = self.query_route_table.get(query_id)
        if next_hop is not None:
            await self.send_message(next_hop, message)
            print(
                f"[Peer {self.port}] Routing QUERYHIT qid={short_qid} "
                f"found_at={found_at} back to={next_hop}"
            )
        else:
            print(
                f"[Peer {self.port}] WARNING: No route for QUERYHIT "
                f"qid={short_qid}"
            )

    # -----------------------------------------------------------------
    # Metrics
    # -----------------------------------------------------------------
    async def handle_metrics_request(self, message):
        reply_port = message["reply_port"]
        response = {
            "type": "METRICS_RESPONSE",
            "peer_id": self.peer_id,
            "metrics": {
                "messages_sent": self.messages_sent,
                "duplicate_queries_dropped": self.duplicate_queries_dropped,
                "matched_peers_count": self.matched_peers_count,
                "queryhit_count": self.queryhit_count,
                "duplicate_queryhits_dropped": self.duplicate_queryhits_dropped,
                "failed_forward_count": self.failed_forward_count,
                "dead_neighbors_detected": len(self.dead_neighbors),
                "latencies": self.latencies,
                "hops": self.hops,
                "matched_peer_ports": list(self.matched_peer_ports),
            }
        }
        await self.send_message(reply_port, response)

    def reset_state(self):
        self.seen_queries.clear()
        self.query_route_table.clear()
        self.processed_queryhits.clear()
        self.matched_peer_ports.clear()
        self.matched_peers_count = 0
        self.queryhit_count = 0
        self.duplicate_queryhits_dropped = 0
        self.messages_sent = 0
        self.duplicate_queries_dropped = 0
        self.failed_forward_count = 0
        self.latencies.clear()
        self.hops.clear()
        self.query_start_time.clear()
        self.dead_neighbors.clear()
        print(f"[Peer {self.port}] State reset.")

    # -----------------------------------------------------------------
    # Low-level send
    # -----------------------------------------------------------------
    async def send_message(self, port, message):
        for attempt in range(3):
            try:
                reader, writer = await asyncio.open_connection(
                    '127.0.0.1', port
                )
                payload = json.dumps(message) + "\n"
                writer.write(payload.encode())
                await writer.drain()
                writer.close()
                await writer.wait_closed()
                return True
            except ConnectionRefusedError:
                if port not in self.dead_neighbors:
                    self.dead_neighbors.add(port)
                    print(
                        f"[Peer {self.port}] Connection refused to {port} — "
                        f"marked as dead"
                    )
                return False
            except OSError as e:
                if getattr(e, 'winerror', None) == 52 and attempt < 2:
                    await asyncio.sleep(0.2 * (attempt + 1))
                    continue
                if port not in self.dead_neighbors:
                    self.dead_neighbors.add(port)
                return False
            except Exception as e:
                print(f"[Peer {self.port}] Send error to {port}: {e}")
                return False


# -----------------------------------------------------------------
# Script entry point
# -----------------------------------------------------------------
if __name__ == "__main__":
    peer_id = int(sys.argv[1])
    topology = json.load(open("topology.json"))
    port = topology["peers"][peer_id]["port"]
    neighbors = []
    for a, b in topology["edges"]:
        if a == peer_id:
            neighbors.append(topology["peers"][b]["port"])
        if b == peer_id:
            neighbors.append(topology["peers"][a]["port"])
    local_files = topology["files"][str(peer_id)]
    node = P2PNode(peer_id, port, neighbors, local_files)
    try:
        asyncio.run(node.start())
    except KeyboardInterrupt:
        pass
