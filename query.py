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
    """Return True if a peer process can bind host:port."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind((host, port))
        return True
    except OSError:
        return False
    finally:
        sock.close()


async def restart_all_peers():
    """Stop all peers via SHUTDOWN, verify they're dead, then restart.

    Ensures peer file lists match topology.json (fixes coverage > 1
    when topology was regenerated without restarting peers).
    Waits for old processes to release their ports before spawning new
    ones so we don't leak processes (port exhaustion / stale runtime).
    """
    # 1. Send SHUTDOWN to all peers
    results = await asyncio.gather(*[
        send_message(port, {"type": "SHUTDOWN"})
        for port in ALL_PORTS
    ])
    delivered = sum(1 for r in results if r)
    print(f"[query.py] SHUTDOWN delivered to {delivered}/{len(ALL_PORTS)} peers")

    # 2. Wait for ports to actually free up
    # Give peers a moment to cancel heartbeat tasks and close writers
    # before we start polling (nodes now close writers before
    # server.wait_closed(), but 100 concurrent shutdowns still contend).
    await asyncio.sleep(1.0)

    # Poll all ports in parallel (up to 8 attempts, 0.5s gap = 4s)
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

    # 3. Start fresh peers
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
    """Collect metrics from a single peer. Returns dict with peer's metrics."""
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
    """Send RESET to *ports* (default: ALL_PORTS) with concurrency limit.

    Returns the list of ports that were alive (RESET succeeded).
    """
    if ports is None:
        ports = ALL_PORTS
    sem = asyncio.Semaphore(20)

    async def _reset_one(p):
        async with sem:
            return await send_message(p, {"type": "RESET"})

    results = await asyncio.gather(*[_reset_one(p) for p in ports])
    await asyncio.sleep(1.0)

    # Second pass: only to peers that were alive (catches stale in-flight msgs)
    alive = [p for p, ok in zip(ports, results) if ok]
    if alive:
        await asyncio.gather(*[_reset_one(p) for p in alive])
        await asyncio.sleep(0.5)

    return alive


async def collect_all_metrics():
    """Collect extended metrics from all 100 peers.

    Returns a dict with summed numeric fields and concatenated lists.
    Uses a single shared server to avoid the Python 3.13 ProactorEventLoop
    assertion crash that occurs when creating 100 temporary servers concurrently.
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
    }

    # Single shared server collects all responses keyed by peer_id
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
        # Send all requests concurrently with an aggregate timeout
        # so a hung peer doesn't block collection forever.
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

        # Give peers a moment to respond
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
    # Override origin-only metrics from the source peer directly.
    # Summing matched_peers_count across all peers is always inflated
    # (every file-holder self-counts).  Never use the sum as fallback.
    source_metrics = await collect_metrics_from(source_port, timeout=2.0)
    if not source_metrics:
        # Retry once; source may be busy processing QUERYHITs
        await asyncio.sleep(0.3)
        source_metrics = await collect_metrics_from(source_port, timeout=2.0)
    if source_metrics:
        matched_ports = source_metrics.get("matched_peer_ports", None)
        if matched_ports is not None:
            # Use the set of unique ports (more reliable than the counter)
            matched_ports_set = set(matched_ports)
            metrics["matched_peers_count"] = len(matched_ports_set)
            metrics["queryhit_count"] = source_metrics.get("queryhit_count", 0)
            metrics["latencies"] = source_metrics.get("latencies", [])
            metrics["hops"] = source_metrics.get("hops", [])
        else:
            # Fallback: old node.py without matched_peer_ports; use counter directly
            metrics["matched_peers_count"] = source_metrics.get("matched_peers_count", 0)
            metrics["queryhit_count"] = source_metrics.get("queryhit_count", 0)
            metrics["latencies"] = source_metrics.get("latencies", [])
            metrics["hops"] = source_metrics.get("hops", [])
    else:
        # Source is dead or unresponsive; sum is unreliable
        print(
            f"[query.py] WARNING: source port {source_port} not responding "
            f"- setting matched_peers_count=0 (was {metrics['matched_peers_count']} from sum)"
        )
        metrics["matched_peers_count"] = 0
        metrics["queryhit_count"] = 0
        metrics["latencies"] = []
        metrics["hops"] = []

    # If valid_ports is provided, filter matched to only count
    # peers that are both keyword-holders AND currently alive.
    # Used by failure experiment (Hub Attack) to exclude dead peers
    # and non-holders from the matched count.
    if valid_ports is not None and source_metrics:
        matched_ports_set = set(source_metrics.get("matched_peer_ports", []))
        valid_set = set(valid_ports)
        metrics["matched_peers_count"] = len(matched_ports_set & valid_set)

    # Debug: if matched exceeds expected, log which ports are in the set
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

    # Diagnose silent send failure
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
    hops = metrics.get("hops", [])
    if hops:
        print(f"avg_hops:                  {sum(hops)/len(hops):.2f}")
    lats = metrics.get("latencies", [])
    if lats:
        print(f"avg_latency_ms:            {sum(lats)/len(lats):.2f}")


if __name__ == "__main__":
    asyncio.run(main())
