import asyncio
import json
import subprocess
import sys
import time
import uuid

ALL_PORTS = [p["port"] for p in json.load(open("topology.json"))["peers"]]


async def restart_all_peers():
    """Stop all peers via SHUTDOWN, then restart with current topology.json.

    Ensures peer file lists match topology.json (fixes coverage > 1
    when topology was regenerated without restarting peers).
    """
    await asyncio.gather(*[
        send_message(port, {"type": "SHUTDOWN"})
        for port in ALL_PORTS
    ])
    await asyncio.sleep(1)

    # Start fresh peers
    n = len(json.load(open("topology.json"))["peers"])
    for peer_id in range(n):
        subprocess.Popen(
            [sys.executable, "node.py", str(peer_id)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    await asyncio.sleep(2)


async def send_message(port, message):
    for attempt in range(3):
        try:
            reader, writer = await asyncio.open_connection('127.0.0.1', port)
            payload = json.dumps(message) + "\n"
            writer.write(payload.encode())
            await writer.drain()
            writer.close()
            await writer.wait_closed()
            return True
        except ConnectionRefusedError:
            return False
        except OSError as e:
            if getattr(e, 'winerror', None) == 52 and attempt < 2:
                await asyncio.sleep(0.2 * (attempt + 1))
                continue
            return False
        except Exception as e:
            print(f"[query.py] Send error to {port}: {e}")
            return False


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
    await server.wait_closed()
    return result


async def reset_all_peers():
    tasks = [send_message(port, {"type": "RESET"}) for port in ALL_PORTS]
    await asyncio.gather(*tasks)
    await asyncio.sleep(0.5)
    # Second reset to catch any stale messages that arrived during drain
    tasks = [send_message(port, {"type": "RESET"}) for port in ALL_PORTS]
    await asyncio.gather(*tasks)
    await asyncio.sleep(0.2)


async def collect_all_metrics():
    """Collect extended metrics from all 100 peers.

    Returns a dict with summed numeric fields and concatenated lists.
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
        "hops": []
    }

    async def collect_one(port):
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

        await send_message(
            port, {"type": "METRICS_REQUEST", "reply_port": reply_port}
        )

        try:
            await asyncio.wait_for(received.wait(), timeout=1.0)
        except asyncio.TimeoutError:
            pass

        server.close()
        await server.wait_closed()
        return result

    results = await asyncio.gather(*[collect_one(p) for p in ALL_PORTS])

    for r in results:
        total["messages_sent"] += r.get("messages_sent", 0)
        total["duplicate_queries_dropped"] += r.get(
            "duplicate_queries_dropped", 0
        )
        total["matched_peers_count"] += r.get("matched_peers_count", 0)
        total["queryhit_count"] += r.get("queryhit_count", 0)
        total["duplicate_queryhits_dropped"] += r.get(
            "duplicate_queryhits_dropped", 0
        )
        total["failed_forward_count"] += r.get("failed_forward_count", 0)
        total["dead_neighbors_detected"] += r.get(
            "dead_neighbors_detected", 0
        )
        total["latencies"].extend(r.get("latencies", []))
        total["hops"].extend(r.get("hops", []))

    return total


async def send_query(source_port, keyword, ttl, timeout=3):
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
    # Summing matched_peers_count across all peers can inflate due to
    # stale QUERYHITs from previous runs arriving after reset.
    source_metrics = await collect_metrics_from(source_port)
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
            # Fallback: old node.py without matched_peer_ports — use counter directly
            metrics["matched_peers_count"] = source_metrics.get("matched_peers_count", 0)
            metrics["queryhit_count"] = source_metrics.get("queryhit_count", 0)
            metrics["latencies"] = source_metrics.get("latencies", [])
            metrics["hops"] = source_metrics.get("hops", [])

        # Debug: if matched exceeds expected, log which ports are in the set
        topo = json.load(open("topology.json"))
        total_file = sum(
            1 for files in topo["files"].values() if keyword in files
        )
        matched = metrics["matched_peers_count"]
        if matched > total_file:
            port_to_id = {p["port"]: p["id"] for p in topo["peers"]}
            matched_ids = []
            for p in sorted(matched_ports_set if matched_ports is not None else []):
                nid = port_to_id.get(p, "?")
                has = keyword in topo["files"].get(str(nid), [])
                marker = "" if has else " *NO FILE*"
                matched_ids.append(f"peer{nid}(port{p}){marker}")
            print(
                f"[DEBUG] matched={matched} > total={total_file} "
                f"(keyword='{keyword}', ttl={ttl})"
            )
            print(f"[DEBUG] matched_ports: {matched_ids}")
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
