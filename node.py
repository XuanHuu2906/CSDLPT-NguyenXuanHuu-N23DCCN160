import asyncio
import json
import random
import sys
import time
import uuid

HEARTBEAT_INTERVAL = 15
PONG_TIMEOUT = 2.0
REJOIN_COOLDOWN = 10.0
REJOIN_MAX_CANDIDATES = 12
REJOIN_NEIGHBOR_TARGET = 3
REJOIN_RESPONSE_TIMEOUT = 1.5


class P2PNode:
    def __init__(self, peer_id, port, neighbors, local_files, known_ports=None):
        self.peer_id = peer_id
        self.port = port
        self.neighbors = list(dict.fromkeys(neighbors))
        self.static_neighbors = set(self.neighbors)
        self.known_ports = list(dict.fromkeys(known_ports or []))
        if self.port not in self.known_ports:
            self.known_ports.append(self.port)
        self.local_files = local_files

        # --- Trạng thái QUERY ---
        self.seen_queries = set()
        self.query_route_table = {}
        self.query_alternate_routes = {}
        self.processed_queryhits = set()
        self.current_query_id = None   # Chỉ node gốc dùng: chặn QUERYHIT cũ

        # --- Phát hiện node rời mạng / neighbor chết ---
        self.dead_neighbors = set()
        self.failed_forward_count = 0
        self.last_seen = {}         # neighbor_port -> timestamp

        self.isolated = False
        self.isolated_since = None
        self.alive_neighbors_count = len(self.neighbors)
        self.runtime_neighbors = set()
        self.rejoin_attempts = 0
        self.rejoin_success_count = 0
        self.rejoin_in_progress = False
        self.last_rejoin_attempt = 0.0

        # --- Metrics (node gốc) ---
        self.matched_peer_ports = set()
        self.matched_peers_count = 0
        self.queryhit_count = 0
        self.duplicate_queryhits_dropped = 0
        self.latencies = []         # ms cho mỗi QUERYHIT (tại node gốc)
        self.hops = []              # số hop cho mỗi QUERYHIT (tại node gốc)
        self.fallback_used = 0      # số QUERYHIT đi qua route thay thế

        # --- Metrics (toàn cục) ---
        self.messages_sent = 0
        self.duplicate_queries_dropped = 0

        # --- Đo latency ---
        self.query_start_time = {}  # query_id -> perf_counter()

        # --- Điều khiển ---
        self.shutdown_event = asyncio.Event()
        self.last_reset_time = time.time()

        # --- Kết nối gửi ra duy trì lâu dài (mỗi neighbor một kết nối) ---
        self.out_writers = {}       # neighbor_port -> (StreamWriter, StreamReader)
        self.in_writers = set()     # các StreamWriter nhận vào đang hoạt động

    # -----------------------------------------------------------------
    # Vòng đời
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

        # Hủy heartbeat và đóng writer gửi ra TRƯỚC
        # server.wait_closed(). Kết nối gửi ra duy trì lâu dài giữ handler
        # nhận vào của neighbor còn sống (kẹt ở readline()).
        # Nếu không đóng trước, server.wait_closed() có thể deadlock
        # vì mỗi peer chờ neighbor đóng trước.
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
    # Kết nối / điều phối message
    # -----------------------------------------------------------------
    async def handle_connection(self, reader, writer):
        """Đọc message JSON-line cho tới khi peer ngắt kết nối (EOF)."""
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
                    # Heartbeat của neighbor: send_message thành công từ sender
                    # đã xác nhận liveness. Không cần PONG vì ghi PONG sẽ tích
                    # tụ trong receive buffer của kết nối persistent (không ai
                    # drain), gây backpressure TCP và cuối cùng làm kẹt kết nối.
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
        elif msg_type == "REJOIN_REQUEST":
            await self.handle_rejoin_request(message)
        elif msg_type == "CHECK_ISOLATION":
            await self.handle_check_isolation(message)
        elif msg_type == "RESET":
            self.reset_state()
        elif msg_type == "SHUTDOWN":
            print(f"[Peer {self.port}] Shutting down...")
            self.shutdown_event.set()

    # -----------------------------------------------------------------
    # Heartbeat PING / PONG
    # -----------------------------------------------------------------
    async def ping_neighbor(self, neighbor_port):
        """Trả về True nếu neighbor phản hồi trong timeout.

        Dùng kết nối persistent qua send_message để tránh cạn ephemeral
        port trên Windows (TIME_WAIT tích tụ khi mở kết nối TCP mới
        mỗi 5 giây cho từng neighbor).
        """
        try:
            msg = {"type": "PING", "sender_port": self.port}
            return await self.send_message(neighbor_port, msg)
        except Exception:
            return False

    async def heartbeat_loop(self):
        """PING định kỳ toàn bộ neighbor để phát hiện node chết."""
        while not self.shutdown_event.is_set():
            for nb in list(self.neighbors):
                alive = await self.ping_neighbor(nb)
                now = time.time()
                if alive:
                    self.last_seen[nb] = now
                    if nb in self.dead_neighbors:
                        # Đã sống lại
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
            self.update_isolation_state()
            if self.isolated:
                await self.attempt_rejoin()
            await asyncio.sleep(HEARTBEAT_INTERVAL)

    async def handle_check_isolation(self, message):
        """Actively verify all neighbors now, then rejoin if isolated."""
        reply_port = message.get("reply_port")
        request_id = message.get("request_id")

        isolated_before = self.isolated
        rejoin_attempts_before = self.rejoin_attempts
        rejoin_success_before = self.rejoin_success_count

        await self.refresh_neighbor_liveness()
        isolated_after_scan = self.isolated
        if self.isolated:
            await self.attempt_rejoin()
        self.update_isolation_state()

        if reply_port is not None:
            response = {
                "type": "CHECK_ISOLATION_RESPONSE",
                "request_id": request_id,
                "peer_id": self.peer_id,
                "port": self.port,
                "isolated_before": isolated_before,
                "isolated_after_scan": isolated_after_scan,
                "isolated_after_rejoin": self.isolated,
                "alive_neighbors_count": self.alive_neighbors_count,
                "neighbor_count": len(self.neighbors),
                "runtime_neighbors_count": len(self.runtime_neighbors),
                "rejoin_attempted": (
                    self.rejoin_attempts - rejoin_attempts_before
                ),
                "rejoin_succeeded": (
                    self.rejoin_success_count - rejoin_success_before
                ),
                "rejoin_attempts": self.rejoin_attempts,
                "rejoin_success_count": self.rejoin_success_count,
                "dead_neighbors_detected": len(self.dead_neighbors),
            }
            await self.send_message(int(reply_port), response)

    async def refresh_neighbor_liveness(self):
        neighbors = list(self.neighbors)
        sem = asyncio.Semaphore(10)

        async def _ping_one(nb):
            async with sem:
                return nb, await self.ping_neighbor(nb)

        results = await asyncio.gather(*[_ping_one(nb) for nb in neighbors])
        now = time.time()
        for nb, alive in results:
            if alive:
                self.last_seen[nb] = now
                self.dead_neighbors.discard(nb)
            else:
                if nb not in self.dead_neighbors:
                    print(
                        f"[Peer {self.port}] Neighbor {nb} failed active "
                        "isolation check. Marked as dead."
                    )
                self.dead_neighbors.add(nb)

        self.update_isolation_state()
        return not self.isolated

    def alive_neighbors(self):
        return [nb for nb in self.neighbors if nb not in self.dead_neighbors]

    def update_isolation_state(self):
        self.alive_neighbors_count = len(self.alive_neighbors())
        now = time.time()
        if self.alive_neighbors_count == 0:
            if not self.isolated:
                self.isolated = True
                self.isolated_since = now
                print(
                    f"[Peer {self.port}] Isolated: no alive neighbors "
                    f"(known={len(self.neighbors)})"
                )
        else:
            if self.isolated:
                print(
                    f"[Peer {self.port}] Reconnected: "
                    f"{self.alive_neighbors_count} alive neighbor(s)"
                )
            self.isolated = False
            self.isolated_since = None
        return self.isolated

    def add_runtime_neighbor(self, port):
        if port == self.port:
            return False
        added = False
        if port not in self.neighbors:
            self.neighbors.append(port)
            added = True
        if port not in self.static_neighbors:
            self.runtime_neighbors.add(port)
        self.dead_neighbors.discard(port)
        self.last_seen[port] = time.time()
        return added

    def choose_rejoin_candidates(self):
        non_neighbors = [
            p for p in self.known_ports
            if p != self.port and p not in self.neighbors
        ]
        candidates = [p for p in non_neighbors if p not in self.dead_neighbors]
        if not candidates:
            candidates = [
                p for p in self.known_ports
                if p != self.port and p not in self.dead_neighbors
            ]
        if not candidates:
            candidates = [p for p in self.known_ports if p != self.port]
        random.shuffle(candidates)
        return candidates[:REJOIN_MAX_CANDIDATES]

    async def attempt_rejoin(self):
        now = time.time()
        if self.rejoin_in_progress:
            return False
        if now - self.last_rejoin_attempt < REJOIN_COOLDOWN:
            return False

        candidates = self.choose_rejoin_candidates()
        if not candidates:
            return False

        self.rejoin_attempts += 1
        self.last_rejoin_attempt = now
        self.rejoin_in_progress = True

        response = {}
        received = asyncio.Event()
        expected = {"request_id": None}

        async def receive_rejoin_response(reader, writer):
            try:
                data = await asyncio.wait_for(
                    reader.readline(), timeout=REJOIN_RESPONSE_TIMEOUT
                )
                msg = json.loads(data.decode().strip())
                if (msg.get("type") == "REJOIN_RESPONSE"
                        and msg.get("request_id") == expected["request_id"]):
                    response.clear()
                    response.update(msg)
                    received.set()
            except Exception:
                pass
            finally:
                writer.close()

        server = await asyncio.start_server(receive_rejoin_response, '127.0.0.1', 0)
        reply_port = server.sockets[0].getsockname()[1]

        try:
            print(
                f"[Peer {self.port}] Rejoin attempt "
                f"{self.rejoin_attempts}: trying {len(candidates)} candidate(s)"
            )
            for candidate in candidates:
                request_id = str(uuid.uuid4())
                expected["request_id"] = request_id
                response.clear()
                received.clear()

                request = {
                    "type": "REJOIN_REQUEST",
                    "request_id": request_id,
                    "peer_id": self.peer_id,
                    "port": self.port,
                    "reply_port": reply_port,
                }
                ok = await self.send_message(candidate, request)
                if not ok:
                    continue

                try:
                    await asyncio.wait_for(
                        received.wait(), timeout=REJOIN_RESPONSE_TIMEOUT
                    )
                except asyncio.TimeoutError:
                    continue

                suggested = response.get("neighbors", [])
                added = []
                for nb in suggested:
                    try:
                        nb = int(nb)
                    except (TypeError, ValueError):
                        continue
                    if nb == self.port:
                        continue
                    if await self.ping_neighbor(nb):
                        self.add_runtime_neighbor(nb)
                        added.append(nb)

                if added:
                    self.rejoin_success_count += 1
                    self.update_isolation_state()
                    print(
                        f"[Peer {self.port}] Rejoin success via {candidate}: "
                        f"neighbors={added}"
                    )
                    return True

            print(f"[Peer {self.port}] Rejoin failed: no reachable candidates")
            return False
        finally:
            server.close()
            try:
                await asyncio.wait_for(server.wait_closed(), timeout=2.0)
            except (Exception, AssertionError):
                pass
            self.rejoin_in_progress = False

    async def handle_rejoin_request(self, message):
        requester_port = int(message["port"])
        reply_port = message.get("reply_port")
        request_id = message.get("request_id")

        if requester_port != self.port:
            self.add_runtime_neighbor(requester_port)
            self.update_isolation_state()

        alive = [nb for nb in self.alive_neighbors() if nb != requester_port]
        random.shuffle(alive)
        suggested = [self.port]
        suggested.extend(alive[:max(0, REJOIN_NEIGHBOR_TARGET - 1)])

        if reply_port is not None:
            response = {
                "type": "REJOIN_RESPONSE",
                "request_id": request_id,
                "sender_port": self.port,
                "neighbors": suggested,
            }
            await self.send_message(int(reply_port), response)

    # -----------------------------------------------------------------
    # Xử lý QUERY (flooding)
    # -----------------------------------------------------------------
    async def handle_query(self, message):
        query_id = message["query_id"]
        keyword = message["keyword"]
        ttl = message["ttl"]
        origin_port = message["origin_port"]
        sender_port = message["sender_port"]
        current_path = message.get("path", [])

        short_qid = query_id.split("-")[0]

        # --- Node gốc tự dọn: mỗi query do nó khởi tạo sẽ reset state của node gốc ---
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
            # Khôi phục toàn bộ neighbor cho query mới (dead_neighbors cũ
            # do RESET lỗi có thể chặn chuyển tiếp và làm TTL không đơn điệu).
            if self.dead_neighbors:
                print(
                    f"[Peer {self.port}] Clearing {len(self.dead_neighbors)} "
                    f"stale dead_neighbors for new query qid={short_qid}"
                )
                self.dead_neighbors.clear()

        # Chặn duplicate - vẫn bỏ qua, nhưng lưu route thay thế
        if query_id in self.seen_queries:
            self.duplicate_queries_dropped += 1
            primary = self.query_route_table.get(query_id)
            # Chỉ lưu route thay thế nếu sender không phải chính mình và
            # ta CHƯA nằm trong path của query (nếu có sẽ tạo vòng lặp).
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

        # Ghi nhận thời điểm bắt đầu tại node gốc
        if self.port == origin_port and query_id not in self.query_start_time:
            self.query_start_time[query_id] = time.perf_counter()

        # Kiểm tra file cục bộ
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

        # Chuyển tiếp nếu TTL > 0
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
            # Chuyển tiếp đồng thời tới toàn bộ neighbor không phải sender.
            # Neighbor bị đánh dấu dead KHÔNG bị bỏ qua vì dấu dead thường
            # đến từ lỗi tạm thời. Semaphore tạo backpressure để burst ghi
            # gửi ra không làm quá tải event loop.
            alive = [nb for nb in self.neighbors
                     if nb != sender_port and nb not in self.dead_neighbors]
            dead_candidates = [nb for nb in self.neighbors
                               if nb != sender_port and nb in self.dead_neighbors]

            fwd_sem = asyncio.Semaphore(10)

            async def _forward_one(nb):
                # Jitter ngẫu nhiên nhỏ giúp phân tán lượt kết nối giữa
                # các peer, giảm burst đồng bộ gây WinError 52.
                await asyncio.sleep(random.uniform(0, 0.002))
                async with fwd_sem:
                    return await self.send_message(nb, forward_msg)

            results = await asyncio.gather(
                *[_forward_one(nb) for nb in alive + dead_candidates]
            )
            sent_ok = 0
            for ok in results:
                if ok:
                    sent_ok += 1
                    self.messages_sent += 1
                else:
                    self.failed_forward_count += 1
            self.update_isolation_state()
            if sent_ok == 0 and self.isolated:
                asyncio.create_task(self.attempt_rejoin())

    # -----------------------------------------------------------------
    # Xử lý QUERYHIT (routing theo đường ngược)
    # -----------------------------------------------------------------
    async def handle_queryhit(self, message):
        query_id = message["query_id"]
        found_at = message["found_at_port"]
        origin_port = message["origin_port"]

        short_qid = query_id.split("-")[0]

        # Khóa chống trùng = (query_id, found_at_port)
        key = (query_id, found_at)
        if key in self.processed_queryhits:
            self.duplicate_queryhits_dropped += 1
            return
        self.processed_queryhits.add(key)

        if self.port == origin_port:
            # Guard nghiêm ngặt: chỉ nhận QUERYHIT khớp query HIỆN TẠI.
            # Điều này chặn QUERYHIT cũ từ thí nghiệm trước (sống sót qua
            # double-RESET) làm bẩn matched_peers_count.
            if query_id != self.current_query_id:
                return

            # ----- node gốc nhận kết quả -----
            if found_at not in self.matched_peer_ports:
                self.matched_peer_ports.add(found_at)
                self.matched_peers_count += 1

            self.queryhit_count += 1

            hop_count = message.get("hop_count", 0)
            self.hops.append(hop_count)

            # Latency tính từ lúc query bắt đầu
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

        # Peer trung gian - route QUERYHIT ngược về node gốc
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
                "isolated": self.isolated,
                "isolated_since": self.isolated_since,
                "alive_neighbors_count": self.alive_neighbors_count,
                "neighbor_count": len(self.neighbors),
                "runtime_neighbors_count": len(self.runtime_neighbors),
                "rejoin_attempts": self.rejoin_attempts,
                "rejoin_success_count": self.rejoin_success_count,
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
        self.rejoin_attempts = 0
        self.rejoin_success_count = 0
        self.last_rejoin_attempt = 0.0
        self.latencies.clear()
        self.hops.clear()
        self.query_start_time.clear()
        self.dead_neighbors.clear()
        self.update_isolation_state()
        self.last_reset_time = time.time()
        print(f"[Peer {self.port}] State reset.")

    # -----------------------------------------------------------------
    # Routing QUERYHIT với route dự phòng thay thế
    # -----------------------------------------------------------------
    async def send_queryhit_with_fallback(self, query_id, queryhit):
        """Thử route chính trước; fallback tuần tự sang route thay thế.

        Cách tuần tự tránh leak kết nối TCP khi
        asyncio.wait(FIRST_COMPLETED) hủy task giữa kết nối
        (CancelledError bỏ qua except Exception, làm socket còn mở).
        """
        short_qid = query_id.split("-")[0]
        primary = self.query_route_table.get(query_id)

        # Gom route: route chính trước, sau đó route thay thế (chống trùng)
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

        # Mọi route đều chết - vẫn thử lại (giống chuyển tiếp QUERY)
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
    # Kết nối gửi ra duy trì lâu dài
    # -----------------------------------------------------------------
    async def _get_writer(self, port):
        """Trả về StreamWriter đang mở tới *port*, tạo mới nếu cần.

        Lưu (writer, reader) để phát hiện kết nối nửa mở
        (đóng từ phía xa) qua reader.at_eof(); writer.is_closing() chỉ
        bắt được đóng cục bộ.
        """
        entry = self.out_writers.get(port)
        if entry is not None:
            writer, reader = entry
            if not writer.is_closing() and not reader.at_eof():
                return writer
            # Kết nối cũ - đóng và tạo lại bên dưới
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
        """Đóng toàn bộ kết nối gửi ra duy trì lâu dài."""
        for port, (writer, _) in list(self.out_writers.items()):
            try:
                writer.close()
            except Exception:
                pass
        self.out_writers.clear()

    async def _close_inbound_writers(self):
        """Đóng kết nối inbound để peer đã shutdown không xử lý send cũ."""
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
    # Gửi message mức thấp
    # -----------------------------------------------------------------
    async def send_message(self, port, message):
        """Gửi một message JSON-line tới *port* qua kết nối persistent.

        Nếu có lỗi ghi, bỏ writer cache và retry một lần bằng kết nối mới.
        """
        for attempt in range(2):
            try:
                writer = await self._get_writer(port)
                payload = json.dumps(message) + "\n"
                writer.write(payload.encode())
                await writer.drain()
                # Ghi thành công - neighbor còn sống
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
                # Kết nối timeout - neighbor đang bận, không hẳn là chết.
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
# Điểm vào script
# -----------------------------------------------------------------
if __name__ == "__main__":
    peer_id = int(sys.argv[1])
    topology = json.load(open("topology.json"))
    port = topology["peers"][peer_id]["port"]
    known_ports = [p["port"] for p in topology["peers"]]
    neighbors = []
    for a, b in topology["edges"]:
        if a == peer_id:
            neighbors.append(topology["peers"][b]["port"])
        if b == peer_id:
            neighbors.append(topology["peers"][a]["port"])
    local_files = topology["files"][str(peer_id)]
    node = P2PNode(peer_id, port, neighbors, local_files, known_ports)
    try:
        asyncio.run(node.start())
    except KeyboardInterrupt:
        pass
