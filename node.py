import asyncio
import json
import random
import sys
import time
import uuid

HEARTBEAT_INTERVAL = 15
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
        self.query_alternate_routes = {}
        self.processed_queryhits = set()
        self.current_query_id = None   # origin-only: guards against stale QUERYHITs

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
        self.fallback_used = 0      # count of QUERYHITs delivered via alternate route

        # --- Metrics (global) ---
        self.messages_sent = 0
        self.duplicate_queries_dropped = 0

        # --- Latency measurement ---
        self.query_start_time = {}  # query_id -> perf_counter()

        # --- Control ---
        self.shutdown_event = asyncio.Event()
        self.last_reset_time = time.time()

        # --- Persistent outbound connections (one per neighbor) ---
        self.out_writers = {}       # neighbor_port -> (StreamWriter, StreamReader)
        self.in_writers = set()     # active inbound StreamWriter objects

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

        await self.shutdown_event.wait()

        # Cancel heartbeat and close outbound writers BEFORE
        # server.wait_closed().  Persistent outbound connections keep
        # neighbours' inbound handlers alive (blocked on readline()).
        # Without closing them first, server.wait_closed() deadlocks
        # because every peer waits for its neighbours to close first.
        heartbeat_task.cancel()
        try:
            await heartbeat_task
        except asyncio.CancelledError:
            pass
        await self._close_all_writers()
        await self._close_inbound_writers()

        server.close()
        try:
            await asyncio.wait_for(server.wait_closed(), timeout=5.0)
        except (asyncio.TimeoutError, AssertionError):
            pass

        print(f"[Peer {self.port}] Stopped.")

    # -----------------------------------------------------------------
    # Connection / message dispatch
    # -----------------------------------------------------------------
    async def handle_connection(self, reader, writer):
        """Read JSON-line messages until the peer disconnects (EOF)."""
        peer_addr = writer.get_extra_info('peername')
        self.in_writers.add(writer)
        try:
            while not self.shutdown_event.is_set():
                data = await reader.readline()
                if not data:
                    break
                try:
                    message = json.loads(data.decode().strip())
                except json.JSONDecodeError:
                    print(f"[Peer {self.port}] Invalid JSON from {peer_addr} - ignored")
                    continue
                msg_type = message.get("type")

                if msg_type == "PING":
                    # Neighbour heartbeat: the sender's send_message success
                    # already confirms liveness.  No PONG is needed - writing
                    # one would accumulate in the persistent connection's
                    # receive buffer (which nobody drains), causing TCP
                    # backpressure and eventual connection stall.
                    pass
                else:
                    try:
                        await self.handle_message(message)
                    except Exception as e:
                        print(f"[Peer {self.port}] Handler error: {e}")
        except Exception as e:
            print(f"[Peer {self.port}] Connection error from {peer_addr}: {e}")
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            self.in_writers.discard(writer)

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
        """Returns True if neighbor responds with PONG within timeout.

        Uses persistent connection via send_message to avoid ephemeral
        port exhaustion on Windows (TIME_WAIT accumulates when opening
        a fresh TCP connection every 5 seconds per neighbour).
        """
        try:
            msg = {"type": "PING", "sender_port": self.port}
            return await self.send_message(neighbor_port, msg)
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
                    if time.time() - self.last_reset_time > 10:
                        if nb not in self.dead_neighbors:
                            self.dead_neighbors.add(nb)
                            self.failed_forward_count += 1
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

        # --- Origin self-clearing: each new own-query resets origin state ---
        if self.port == origin_port and query_id != self.current_query_id:
            self.current_query_id = query_id
            self.matched_peer_ports.clear()
            self.matched_peers_count = 0
            self.queryhit_count = 0
            self.duplicate_queryhits_dropped = 0
            self.latencies.clear()
            self.hops.clear()
            self.fallback_used = 0
            self.query_start_time.clear()
            # Revive all neighbors for the new query (stale dead_neighbors
            # from a failed RESET would block forwarding and cause TTL
            # monotonicity violations).
            if self.dead_neighbors:
                print(
                    f"[Peer {self.port}] Clearing {len(self.dead_neighbors)} "
                    f"stale dead_neighbors for new query qid={short_qid}"
                )
                self.dead_neighbors.clear()

        # Duplicate suppression - still drop, but save alternate route
        if query_id in self.seen_queries:
            self.duplicate_queries_dropped += 1
            primary = self.query_route_table.get(query_id)
            # Only save alternate route if the sender is not us and
            # we are NOT already in the query's path (would create a loop).
            if (sender_port != primary
                    and sender_port != self.port
                    and self.port not in current_path):
                self.query_alternate_routes.setdefault(query_id, set()).add(sender_port)
                print(
                    f"[Peer {self.port}] Duplicate QUERY qid={short_qid} "
                    f"from={sender_port} saved as alternate route"
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
            ok = await self.send_queryhit_with_fallback(query_id, queryhit)
            if ok:
                print(
                    f"[Peer {self.port}] MATCH {file} for qid={short_qid}, "
                    f"hops={hop_count}"
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
            # Forward to all non-sender neighbors concurrently.
            # Dead-marked neighbors are NOT skipped - the dead mark is often
            # from a transient error.  Semaphore provides backpressure so
            # the outbound write burst doesn't overwhelm the event loop.
            alive = [nb for nb in self.neighbors
                     if nb != sender_port and nb not in self.dead_neighbors]
            dead_candidates = [nb for nb in self.neighbors
                               if nb != sender_port and nb in self.dead_neighbors]

            fwd_sem = asyncio.Semaphore(10)

            async def _forward_one(nb):
                # Small random jitter spreads connection attempts across
                # peers, reducing synchronised bursts that trigger WinError 52.
                await asyncio.sleep(random.uniform(0, 0.002))
                async with fwd_sem:
                    return await self.send_message(nb, forward_msg)

            results = await asyncio.gather(
                *[_forward_one(nb) for nb in alive + dead_candidates]
            )
            for ok in results:
                if ok:
                    self.messages_sent += 1
                else:
                    self.failed_forward_count += 1

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
            # Strict guard: only accept QUERYHITs matching our CURRENT query.
            # This prevents stale QUERYHITs from previous experiments (which
            # survive double-RESET) from contaminating matched_peers_count.
            if query_id != self.current_query_id:
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

        # Intermediate peer - route QUERYHIT back toward origin
        next_hop = self.query_route_table.get(query_id)
        if next_hop is not None:
            ok = await self.send_queryhit_with_fallback(query_id, message)
            if ok:
                print(
                    f"[Peer {self.port}] Routed QUERYHIT qid={short_qid} "
                    f"found_at={found_at}"
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
                "last_reset_time": self.last_reset_time,
                "fallback_used": self.fallback_used,
            }
        }
        await self.send_message(reply_port, response)

    def reset_state(self):
        self.seen_queries.clear()
        self.query_route_table.clear()
        self.query_alternate_routes.clear()
        self.processed_queryhits.clear()
        self.current_query_id = None
        self.matched_peer_ports.clear()
        self.matched_peers_count = 0
        self.queryhit_count = 0
        self.duplicate_queryhits_dropped = 0
        self.messages_sent = 0
        self.duplicate_queries_dropped = 0
        self.failed_forward_count = 0
        self.fallback_used = 0
        self.latencies.clear()
        self.hops.clear()
        self.query_start_time.clear()
        self.dead_neighbors.clear()
        self.last_reset_time = time.time()
        print(f"[Peer {self.port}] State reset.")

    # -----------------------------------------------------------------
    # QUERYHIT routing with alternate fallback
    # -----------------------------------------------------------------
    async def send_queryhit_with_fallback(self, query_id, queryhit):
        """Try primary route first; fall back to alternates sequentially.

        Sequential avoids the TCP connection leak that occurs when
        asyncio.wait(FIRST_COMPLETED) cancels tasks mid-connection
        (CancelledError bypasses except Exception, leaving sockets open).
        """
        short_qid = query_id.split("-")[0]
        primary = self.query_route_table.get(query_id)

        # Collect: primary first, then alternates (dedup)
        seen = set()
        candidates = []
        dead_candidates = []
        if primary is not None:
            if primary not in self.dead_neighbors:
                candidates.append(primary)
                seen.add(primary)
            else:
                dead_candidates.append(primary)
                seen.add(primary)
        for alt in self.query_alternate_routes.get(query_id, set()):
            if alt not in seen:
                if alt not in self.dead_neighbors:
                    candidates.append(alt)
                    seen.add(alt)
                else:
                    dead_candidates.append(alt)
                    seen.add(alt)

        # All routes dead - try them anyway (mirrors QUERY forwarding)
        if not candidates and dead_candidates:
            print(
                f"[Peer {self.port}] QUERYHIT qid={short_qid}: "
                f"all routes dead - retrying {len(dead_candidates)} candidate(s)"
            )
            candidates = dead_candidates

        if not candidates:
            print(
                f"[Peer {self.port}] QUERYHIT qid={short_qid} lost: "
                f"no reverse route"
            )
            return False

        for candidate in candidates:
            ok = await self.send_message(candidate, queryhit)
            if ok:
                if candidate != primary and primary is not None:
                    self.fallback_used += 1
                    print(
                        f"[Peer {self.port}] Fallback route {candidate} used "
                        f"for QUERYHIT qid={short_qid} (primary={primary})"
                    )
                return True
            else:
                self.failed_forward_count += 1
                if candidate not in self.dead_neighbors:
                    self.dead_neighbors.add(candidate)
                    print(
                        f"[Peer {self.port}] Candidate {candidate} unreachable "
                        f"for QUERYHIT qid={short_qid}, marked dead"
                    )

        print(
            f"[Peer {self.port}] QUERYHIT qid={short_qid} lost: "
            f"all routes failed"
        )
        return False

    # -----------------------------------------------------------------
    # Persistent outbound connections
    # -----------------------------------------------------------------
    async def _get_writer(self, port):
        """Return an open StreamWriter to *port*, creating one if needed.

        Stores (writer, reader) so we can detect half-open connections
        (remote close) via reader.at_eof() - writer.is_closing() only
        catches local closes.
        """
        entry = self.out_writers.get(port)
        if entry is not None:
            writer, reader = entry
            if not writer.is_closing() and not reader.at_eof():
                return writer
            # Stale - close and recreate below
            try:
                writer.close()
            except Exception:
                pass
            self.out_writers.pop(port, None)

        reader, writer = await asyncio.wait_for(
            asyncio.open_connection('127.0.0.1', port),
            timeout=3.0
        )
        self.out_writers[port] = (writer, reader)
        return writer

    async def _close_all_writers(self):
        """Close all persistent outbound connections."""
        for port, (writer, _) in list(self.out_writers.items()):
            try:
                writer.close()
            except Exception:
                pass
        self.out_writers.clear()

    async def _close_inbound_writers(self):
        """Close inbound connections so a shut down peer cannot process stale sends."""
        writers = list(self.in_writers)
        for writer in writers:
            try:
                writer.close()
            except Exception:
                pass
        for writer in writers:
            try:
                await asyncio.wait_for(writer.wait_closed(), timeout=1.0)
            except (Exception, AssertionError):
                pass
        self.in_writers.clear()

    # -----------------------------------------------------------------
    # Low-level send
    # -----------------------------------------------------------------
    async def send_message(self, port, message):
        """Send one JSON-line message to *port* via persistent connection.

        On any write error the cached writer is discarded and one retry
        with a fresh connection is attempted.
        """
        for attempt in range(2):
            try:
                writer = await self._get_writer(port)
                payload = json.dumps(message) + "\n"
                writer.write(payload.encode())
                await writer.drain()
                # Successful write - neighbour is alive
                if port in self.dead_neighbors:
                    self.dead_neighbors.discard(port)
                return True
            except ConnectionRefusedError:
                self.out_writers.pop(port, None)
                if port not in self.dead_neighbors:
                    self.dead_neighbors.add(port)
                    print(
                        f"[Peer {self.port}] Connection refused to {port} - "
                        f"marked as dead"
                    )
                return False
            except asyncio.TimeoutError:
                # Connection timed out - neighbour is busy, not dead.
                self.out_writers.pop(port, None)
                return False
            except OSError as e:
                self.out_writers.pop(port, None)
                if getattr(e, 'winerror', None) == 52 and attempt < 1:
                    await asyncio.sleep(0.1 + random.uniform(0, 0.05))
                    continue
                if port not in self.dead_neighbors:
                    self.dead_neighbors.add(port)
                return False
            except Exception:
                self.out_writers.pop(port, None)
                if attempt < 1:
                    await asyncio.sleep(0.1)
                    continue
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
