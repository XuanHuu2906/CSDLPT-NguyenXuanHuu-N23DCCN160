import asyncio
import json
import random
import socket
import subprocess
import sys
import time
import uuid

ALL_PORTS = [p["port"] for p in json.load(open("topology.json"))["peers"]]


def is_port_free(port, host="127.0.0.1"):
    """Trả về True nếu một process peer có thể bind host:port."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind((host, port))
        return True
    except OSError:
        return False
    finally:
        sock.close()


async def restart_all_peers():
    """Dừng mọi peer bằng SHUTDOWN, xác minh đã chết, rồi khởi động lại.

    Đảm bảo danh sách file của peer khớp với topology.json (sửa lỗi
    coverage > 1 khi topology được sinh lại nhưng peer chưa restart).
    Chờ process cũ nhả port trước khi tạo process mới để tránh rò rỉ
    process (cạn port / runtime cũ còn sót).
    """
    # 1. Gửi SHUTDOWN tới toàn bộ peer
    results = await asyncio.gather(*[
        send_message(port, {"type": "SHUTDOWN"})
        for port in ALL_PORTS
    ])
    delivered = sum(1 for r in results if r)
    print(f"[query.py] SHUTDOWN delivered to {delivered}/{len(ALL_PORTS)} peers")

    # 2. Chờ port thật sự được nhả
    # Cho peer một chút thời gian để hủy heartbeat task và đóng writer
    # trước khi bắt đầu polling (node hiện đóng writer trước
    # server.wait_closed(), nhưng 100 shutdown đồng thời vẫn cạnh tranh).
    await asyncio.sleep(1.0)

    # Poll tất cả port song song (tối đa 8 lần, cách 0.5s = 4s)
    stuck = set(ALL_PORTS)
    for attempt in range(8):
        ports_to_check = list(stuck)
        results = [is_port_free(p) for p in ports_to_check]
        stuck = {p for p, free in zip(ports_to_check, results) if not free}
        if not stuck:
            break
        if attempt < 7:
            await asyncio.sleep(0.5)

    if stuck:
        print(
            f"[query.py] WARNING: {len(stuck)} port(s) still occupied "
            f"after SHUTDOWN: {sorted(stuck)[:10]}..."
        )

    # 3. Khởi động peer mới
    n = len(json.load(open("topology.json"))["peers"])
    started = 0
    for peer_id in range(n):
        port = ALL_PORTS[peer_id]
        if port in stuck:
            print(f"[query.py] SKIPPING peer {peer_id} - port {port} still occupied")
            continue
        subprocess.Popen(
            [sys.executable, "node.py", str(peer_id)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        started += 1
    print(f"[query.py] Started {started}/{n} peers")
    await asyncio.sleep(2)


async def send_message(port, message, connect_timeout=3.0):
    for attempt in range(3):
        writer = None
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection('127.0.0.1', port),
                timeout=connect_timeout
            )
            payload = json.dumps(message) + "\n"
            writer.write(payload.encode())
            await writer.drain()
            writer.close()
            await writer.wait_closed()
            writer = None
            return True
        except (ConnectionRefusedError, asyncio.TimeoutError):
            if attempt < 2:
                await asyncio.sleep(0.3 * (attempt + 1))
                continue
            return False
        except OSError as e:
            if getattr(e, 'winerror', None) == 52 and attempt < 2:
                backoff = 0.1 * (2 ** attempt) + random.uniform(0, 0.1)
                await asyncio.sleep(backoff)
                continue
            return False
        except Exception as e:
            print(f"[query.py] Send error to {port}: {e}")
            return False
        finally:
            if writer is not None:
                try:
                    writer.close()
                except Exception:
                    pass


async def collect_metrics_from(port, timeout=1.0):
    """Thu metrics từ một peer. Trả về dict chứa metrics của peer đó."""
    result = {}
    received = asyncio.Event()

    async def receive(reader, writer, r=result, e=received):
        try:
            data = await reader.readline()
            msg = json.loads(data.decode().strip())
            if msg["type"] == "METRICS_RESPONSE":
                r.update(msg["metrics"])
                e.set()
        except Exception:
            pass
        finally:
            writer.close()

    server = await asyncio.start_server(receive, '127.0.0.1', 0)
    reply_port = server.sockets[0].getsockname()[1]

    await send_message(port, {"type": "METRICS_REQUEST", "reply_port": reply_port})

    try:
        await asyncio.wait_for(received.wait(), timeout=timeout)
    except asyncio.TimeoutError:
        pass

    server.close()
    try:
        await asyncio.wait_for(server.wait_closed(), timeout=2.0)
    except (Exception, AssertionError):
        pass
    return result


async def reset_all_peers(ports=None):
    """Gửi RESET tới *ports* (mặc định: ALL_PORTS) với giới hạn concurrency.

    Trả về danh sách port còn sống (RESET thành công).
    """
    if ports is None:
        ports = ALL_PORTS
    sem = asyncio.Semaphore(20)

    async def _reset_one(p):
        async with sem:
            return await send_message(p, {"type": "RESET"})

    results = await asyncio.gather(*[_reset_one(p) for p in ports])
    await asyncio.sleep(1.0)

    # Lượt hai: chỉ gửi tới peer còn sống (bắt các message cũ còn đang truyền)
    alive = [p for p, ok in zip(ports, results) if ok]
    if alive:
        await asyncio.gather(*[_reset_one(p) for p in alive])
        await asyncio.sleep(0.5)

    return alive


async def check_isolation_all(ports=None, wait=8.0, return_detail=False):
    """Ask peers to detect isolation, wait until each completed the scan."""
    if ports is None:
        ports = ALL_PORTS
    ports = list(dict.fromkeys(ports))
    if not ports:
        if return_detail:
            return {
                "requested_ports": [],
                "delivered_ports": [],
                "completed_ports": [],
                "responses": {},
                "requested_count": 0,
                "delivered_count": 0,
                "completed_count": 0,
            }
        return []

    sem = asyncio.Semaphore(20)
    request_id = str(uuid.uuid4())
    responses = {}
    response_event = asyncio.Event()

    async def _receive_response(reader, writer):
        try:
            data = await asyncio.wait_for(reader.readline(), timeout=wait)
            msg = json.loads(data.decode().strip())
            if (msg.get("type") == "CHECK_ISOLATION_RESPONSE"
                    and msg.get("request_id") == request_id):
                responses[msg["port"]] = msg
                response_event.set()
        except Exception:
            pass
        finally:
            writer.close()

    server = await asyncio.start_server(_receive_response, '127.0.0.1', 0)
    reply_port = server.sockets[0].getsockname()[1]

    try:
        async def _check_one(p):
            async with sem:
                return await send_message(p, {
                    "type": "CHECK_ISOLATION",
                    "request_id": request_id,
                    "reply_port": reply_port,
                })

        results = await asyncio.gather(*[_check_one(p) for p in ports])
        delivered = [p for p, ok in zip(ports, results) if ok]

        deadline = time.time() + wait
        while len(responses) < len(delivered):
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            try:
                await asyncio.wait_for(response_event.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                break
            response_event.clear()

        completed = [p for p in delivered if p in responses]
        if return_detail:
            return {
                "requested_ports": ports,
                "delivered_ports": delivered,
                "completed_ports": completed,
                "responses": responses,
                "requested_count": len(ports),
                "delivered_count": len(delivered),
                "completed_count": len(completed),
            }
        return completed
    finally:
        server.close()
        try:
            await asyncio.wait_for(server.wait_closed(), timeout=2.0)
        except (Exception, AssertionError):
            pass


async def collect_all_metrics():
    """Thu metrics mở rộng từ toàn bộ 100 peer.

    Trả về dict có các field số đã cộng và các list đã nối.
    Dùng một shared server để tránh crash assertion của Python 3.13
    ProactorEventLoop khi tạo đồng thời 100 server tạm.
    """
    total = {
        "messages_sent": 0,
        "duplicate_queries_dropped": 0,
        "matched_peers_count": 0,
        "queryhit_count": 0,
        "duplicate_queryhits_dropped": 0,
        "failed_forward_count": 0,
        "dead_neighbors_detected": 0,
        "latencies": [],
        "hops": [],
        "fallback_used": 0,
        "isolated_nodes_count": 0,
        "rejoin_attempts": 0,
        "rejoin_success_count": 0,
    }

    # Một server dùng chung thu tất cả phản hồi theo peer_id
    peer_results = {}  # peer_id -> dict

    async def shared_receive(reader, writer):
        try:
            data = await asyncio.wait_for(reader.readline(), timeout=1.0)
            msg = json.loads(data.decode().strip())
            if msg["type"] == "METRICS_RESPONSE":
                pid = msg["peer_id"]
                peer_results[pid] = msg["metrics"]
        except Exception:
            pass
        finally:
            try:
                writer.close()
            except Exception:
                pass

    server = await asyncio.start_server(shared_receive, '127.0.0.1', 0)
    reply_port = server.sockets[0].getsockname()[1]

    try:
        # Gửi tất cả request đồng thời với timeout tổng
        # để peer bị treo không chặn việc thu metrics mãi.
        try:
            await asyncio.wait_for(
                asyncio.gather(*[
                    send_message(p, {"type": "METRICS_REQUEST", "reply_port": reply_port})
                    for p in ALL_PORTS
                ]),
                timeout=5.0
            )
        except asyncio.TimeoutError:
            pass

        # Cho peer một chút thời gian để phản hồi
        await asyncio.sleep(1.0)
    finally:
        server.close()
        try:
            await asyncio.wait_for(server.wait_closed(), timeout=2.0)
        except (Exception, AssertionError):
            pass

    for pid, r in peer_results.items():
        total["messages_sent"] += r.get("messages_sent", 0)
        total["duplicate_queries_dropped"] += r.get("duplicate_queries_dropped", 0)
        total["matched_peers_count"] += r.get("matched_peers_count", 0)
        total["queryhit_count"] += r.get("queryhit_count", 0)
        total["duplicate_queryhits_dropped"] += r.get("duplicate_queryhits_dropped", 0)
        total["failed_forward_count"] += r.get("failed_forward_count", 0)
        total["dead_neighbors_detected"] += r.get("dead_neighbors_detected", 0)
        total["fallback_used"] += r.get("fallback_used", 0)
        if r.get("isolated", False):
            total["isolated_nodes_count"] += 1
        total["rejoin_attempts"] += r.get("rejoin_attempts", 0)
        total["rejoin_success_count"] += r.get("rejoin_success_count", 0)
        total["latencies"].extend(r.get("latencies", []))
        total["hops"].extend(r.get("hops", []))

    return total


async def send_query(source_port, keyword, ttl, timeout=3, valid_ports=None):
    query_id = str(uuid.uuid4())
    query_msg = {
        "type": "QUERY",
        "query_id": query_id,
        "keyword": keyword,
        "ttl": ttl,
        "initial_ttl": ttl,
        "origin_port": source_port,
        "sender_port": source_port,
        "path": []
    }
    await send_message(source_port, query_msg)
    await asyncio.sleep(timeout)
    metrics = await collect_all_metrics()
    # Ghi đè metrics chỉ thuộc origin bằng dữ liệu trực tiếp từ source peer.
    # Cộng matched_peers_count trên mọi peer luôn bị phóng đại
    # (mỗi file-holder tự đếm). Không dùng tổng đó làm fallback.
    source_metrics = await collect_metrics_from(source_port, timeout=2.0)
    if not source_metrics:
        # Thử lại một lần; source có thể đang bận xử lý QUERYHIT
        await asyncio.sleep(0.3)
        source_metrics = await collect_metrics_from(source_port, timeout=2.0)
    if source_metrics:
        matched_ports = source_metrics.get("matched_peer_ports", None)
        if matched_ports is not None:
            # Dùng tập port duy nhất (đáng tin hơn counter)
            matched_ports_set = set(matched_ports)
            metrics["matched_peers_count"] = len(matched_ports_set)
            metrics["queryhit_count"] = source_metrics.get("queryhit_count", 0)
            metrics["latencies"] = source_metrics.get("latencies", [])
            metrics["hops"] = source_metrics.get("hops", [])
        else:
            # Dự phòng: node.py cũ không có matched_peer_ports; dùng bộ đếm trực tiếp
            metrics["matched_peers_count"] = source_metrics.get("matched_peers_count", 0)
            metrics["queryhit_count"] = source_metrics.get("queryhit_count", 0)
            metrics["latencies"] = source_metrics.get("latencies", [])
            metrics["hops"] = source_metrics.get("hops", [])
    else:
        # Source chết hoặc không phản hồi; tổng không đáng tin
        print(
            f"[query.py] WARNING: source port {source_port} not responding "
            f"- setting matched_peers_count=0 (was {metrics['matched_peers_count']} from sum)"
        )
        metrics["matched_peers_count"] = 0
        metrics["queryhit_count"] = 0
        metrics["latencies"] = []
        metrics["hops"] = []
        metrics["source_isolated"] = True
        metrics["source_alive_neighbors_count"] = 0

    if source_metrics:
        metrics["source_isolated"] = bool(source_metrics.get("isolated", False))
        metrics["source_alive_neighbors_count"] = source_metrics.get(
            "alive_neighbors_count", 0
        )

    # Nếu valid_ports được cung cấp, lọc matched để chỉ đếm peer
    # vừa là node giữ từ khóa vừa đang còn sống.
    # Dùng trong thí nghiệm lỗi (Hub Attack) để loại peer chết
    # và node không giữ từ khóa khỏi số matched.
    if valid_ports is not None and source_metrics:
        matched_ports_set = set(source_metrics.get("matched_peer_ports", []))
        valid_set = set(valid_ports)
        metrics["matched_peers_count"] = len(matched_ports_set & valid_set)

    # Debug: nếu matched vượt kỳ vọng, log các port đang nằm trong tập
    topo = json.load(open("topology.json"))
    total_file = sum(
        1 for files in topo["files"].values() if keyword in files
    )
    matched = metrics["matched_peers_count"]
    if matched > total_file:
        port_to_id = {p["port"]: p["id"] for p in topo["peers"]}
        matched_ids = []
        matched_ports_set = set(source_metrics.get("matched_peer_ports", [])) if source_metrics else set()
        for p in sorted(matched_ports_set):
            nid = port_to_id.get(p, "?")
            has = keyword in topo["files"].get(str(nid), [])
            marker = "" if has else " *NO FILE*"
            matched_ids.append(f"peer{nid}(port{p}){marker}")
        print(
            f"[DEBUG] matched={matched} > total={total_file} "
            f"(keyword='{keyword}', ttl={ttl})"
        )
        print(f"[DEBUG] matched_ports: {matched_ids}")

    # Chẩn đoán lỗi gửi im lặng
    if metrics["messages_sent"] == 0 and matched == 0:
        dead = source_metrics.get("dead_neighbors_detected", 0) if source_metrics else 0
        neighbors = source_metrics.get("failed_forward_count", 0) if source_metrics else 0
        print(
            f"[DEBUG] messages_sent=0 for source port {source_port}, "
            f"ttl={ttl}, keyword='{keyword}'. "
            f"dead_neighbors={dead}, failed_forward={neighbors}. "
            f"Source may have all neighbors marked dead from stale state."
        )
    return metrics


async def main():
    source_id = int(sys.argv[1])
    keyword = sys.argv[2]
    ttl = int(sys.argv[3])
    topology = json.load(open("topology.json"))
    source_port = topology["peers"][source_id]["port"]

    print("Resetting all peers...")
    await reset_all_peers()

    print(f"Query: keyword='{keyword}', TTL={ttl}, source=Peer {source_id}")
    metrics = await send_query(source_port, keyword, ttl)

    dup_ratio = (
        metrics["duplicate_queries_dropped"]
        / max(metrics["messages_sent"], 1)
        * 100
    )

    print(f"\n=== Results ===")
    print(f"TTL:                       {ttl}")
    print(f"matched_peers_count:       {metrics['matched_peers_count']}")
    print(f"messages_sent (total):     {metrics['messages_sent']}")
    print(f"duplicate_queries_dropped: {metrics['duplicate_queries_dropped']}")
    print(f"duplicate_ratio:           {dup_ratio:.1f}%")
    print(f"queryhit_count:            {metrics['queryhit_count']}")
    print(f"failed_forward_count:      {metrics['failed_forward_count']}")
    print(f"dead_neighbors_detected:   {metrics['dead_neighbors_detected']}")
    print(f"isolated_nodes_count:      {metrics.get('isolated_nodes_count', 0)}")
    print(f"source_isolated:           {metrics.get('source_isolated', False)}")
    print(f"source_alive_neighbors:    {metrics.get('source_alive_neighbors_count', 0)}")
    print(f"rejoin_attempts:           {metrics.get('rejoin_attempts', 0)}")
    print(f"rejoin_success_count:      {metrics.get('rejoin_success_count', 0)}")
    hops = metrics.get("hops", [])
    if hops:
        print(f"avg_hops:                  {sum(hops)/len(hops):.2f}")
    lats = metrics.get("latencies", [])
    if lats:
        print(f"avg_latency_ms:            {sum(lats)/len(lats):.2f}")


if __name__ == "__main__":
    asyncio.run(main())
