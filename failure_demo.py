import asyncio
import json
import os
import random
import time

from query import (
    ALL_PORTS, send_query, reset_all_peers,
    collect_all_metrics, send_message, restart_all_peers, is_port_free,
    check_isolation_all
)

BLOCKED_PEERS = set()  # Không chặn peer nào trong dải port 6000-6099
K = 5  # Số node degree cao cần kill


def load_topology(path="topology.json"):
    return json.load(open(path))


def build_adjacency(topology):
    n = len(topology["peers"])
    adj = {i: [] for i in range(n)}
    for a, b in topology["edges"]:
        adj[a].append(b)
        adj[b].append(a)
    return adj


def get_file_pool(topology):
    return list(set(f for files in topology["files"].values() for f in files))


def count_peers_with_file(topology, keyword):
    return sum(
        1 for files in topology["files"].values() if keyword in files
    )


async def run_query(label, source_port, keyword, ttl=5, alive_ports=None,
                     valid_ports=None):
    """Chạy một query và thu metrics mở rộng.

    Nếu có *alive_ports*, chỉ RESET các peer đó.
    Nếu có *valid_ports*, matched được lọc để chỉ đếm các port đó.
    """
    await reset_all_peers(ports=alive_ports)
    await check_isolation_all(ports=alive_ports)
    metrics = await send_query(source_port, keyword, ttl, valid_ports=valid_ports)

    total_with_file = count_peers_with_file(load_topology(), keyword)
    coverage_ratio = metrics["matched_peers_count"] / max(total_with_file, 1)
    dup_ratio = (
        metrics["duplicate_queries_dropped"]
        / max(metrics["messages_sent"], 1)
    )

    hops = metrics.get("hops", [])
    avg_hops = sum(hops) / len(hops) if hops else 0.0
    lats = metrics.get("latencies", [])
    avg_lat = sum(lats) / len(lats) if lats else 0.0

    print(f"  [{label}]")
    print(f"    matched_peers_count:       {metrics['matched_peers_count']}")
    print(f"    total_peers_having_file:   {total_with_file}")
    print(f"    coverage_ratio:            {coverage_ratio:.2f}")
    print(f"    messages_sent:             {metrics['messages_sent']}")
    print(f"    duplicate_queries_dropped: {metrics['duplicate_queries_dropped']}")
    print(f"    duplicate_ratio:           {dup_ratio:.3f}")
    print(f"    avg_hops:                  {avg_hops:.2f}")
    print(f"    avg_latency_ms:            {avg_lat:.1f}")
    print(f"    failed_forward_count:      {metrics['failed_forward_count']}")
    print(f"    dead_neighbors_detected:   {metrics['dead_neighbors_detected']}")
    print(f"    isolated_nodes_count:      {metrics.get('isolated_nodes_count', 0)}")
    print(f"    source_isolated:           {metrics.get('source_isolated', False)}")
    print(f"    rejoin_success_count:      {metrics.get('rejoin_success_count', 0)}")

    return {
        "matched_peers_count": metrics["matched_peers_count"],
        "total_peers_having_file": total_with_file,
        "coverage_ratio": coverage_ratio,
        "messages_sent": metrics["messages_sent"],
        "duplicate_ratio": dup_ratio,
        "avg_hops": avg_hops,
        "avg_latency_ms": avg_lat,
        "failed_forward_count": metrics["failed_forward_count"],
        "dead_neighbors_detected": metrics["dead_neighbors_detected"],
        "isolated_nodes_count": metrics.get("isolated_nodes_count", 0),
        "source_isolated": metrics.get("source_isolated", False),
        "source_alive_neighbors_count": metrics.get(
            "source_alive_neighbors_count", 0
        ),
        "rejoin_attempts": metrics.get("rejoin_attempts", 0),
        "rejoin_success_count": metrics.get("rejoin_success_count", 0),
    }


async def main():
    """Tấn công hub: kill các node degree cao KHÔNG giữ keyword.

    Minh họa rằng kill hub làm phân mảnh overlay, giảm độ bao phủ query
    dù mọi bản sao keyword vẫn còn trong mạng.
    """
    topology = load_topology()
    adj = build_adjacency(topology)
    pool = get_file_pool(topology)
    all_ports = [p["port"] for p in topology["peers"]]

    node_degree = {i: len(adj[i]) for i in range(100)}
    degree_rank = sorted(range(100), key=lambda i: node_degree[i], reverse=True)

    # ---- Khởi động lại sạch toàn bộ peer ----
    print("Restarting all peers for clean state...")
    await restart_all_peers()

    # ---- Chọn keyword có nhiều bản sao nhất (>=10) ----
    keyword = None
    keyword_holders = set()
    for kw in sorted(pool, key=lambda k: -sum(
            1 for i in range(100)
            if k in topology["files"].get(str(i), [])
            and i not in BLOCKED_PEERS
    )):
        holders = {i for i in range(100)
                   if kw in topology["files"].get(str(i), [])
                   and i not in BLOCKED_PEERS}
        if len(holders) >= 10:
            keyword = kw
            keyword_holders = holders
            break

    if keyword is None:
        print("ERROR: no keyword with >=10 copies found. Aborting.")
        return

    total_holders = len(keyword_holders)
    keyword_holder_ports = {topology["peers"][i]["port"] for i in keyword_holders}

    # ---- Tạo danh sách kill: top K hub, loại trừ node giữ keyword ----
    kill_ids = []
    for node_id in degree_rank:
        if len(kill_ids) >= K:
            break
        if node_id in keyword_holders:
            continue
        if node_id in BLOCKED_PEERS:
            continue
        kill_ids.append(node_id)

    if len(kill_ids) < 1:
        print("ERROR: no non-holder hubs to kill. Aborting.")
        return

    # ---- Chọn source: không nằm trong danh sách kill, không phải node giữ keyword ----
    source_id = next(
        (i for i in range(100)
         if i not in kill_ids
         and i not in keyword_holders
         and i not in BLOCKED_PEERS),
        None
    )
    if source_id is None:
        print("ERROR: no valid source found. Aborting.")
        return
    source_port = topology["peers"][source_id]["port"]

    print("=" * 60)
    print("P2P FAILURE DEMO - Hub Attack (Churn Resilience Test)")
    print("=" * 60)
    print(f"\nTopology: 100 peers")
    print(f"Nodes to kill (hubs, non-holders): {kill_ids}")
    print(f"Source peer:               {source_id} (port {source_port})")
    print(f"Keyword:                   '{keyword}'")
    print(f"Total holders:             {total_holders}")

    # ---- Pha 1: Trước lỗi ----
    print(f"\n--- Phase 1: Before failure ---")
    valid_ports = list(keyword_holder_ports)
    before = await run_query("BEFORE", source_port, keyword,
                             valid_ports=valid_ports)
    if before["matched_peers_count"] == 0:
        print("ERROR: baseline=0. Aborting.")
        return

    # ---- Pha 2: Kill hub ----
    print(f"\n--- Phase 2: Killing {len(kill_ids)} hub nodes "
          f"(non-holders of '{keyword}') ---")
    killed_ports = set()

    for node_id in kill_ids:
        port = topology["peers"][node_id]["port"]
        print(f"  Sending SHUTDOWN to Peer {node_id} (port {port})...")
        ok = await send_message(port, {"type": "SHUTDOWN"})
        if not ok:
            print(f"  WARNING: SHUTDOWN delivery failed for Peer {node_id}")

    print(f"  Verifying nodes are dead...")
    actually_dead = []
    for node_id in kill_ids:
        port = topology["peers"][node_id]["port"]
        dead = False
        for _ in range(10):
            if is_port_free(port):
                dead = True
                break
            await asyncio.sleep(0.5)
        if dead:
            killed_ports.add(port)
            actually_dead.append(node_id)
        else:
            print(f"  WARNING: Peer {node_id} (port {port}) still alive - excluded")
    print(f"  {len(actually_dead)}/{len(kill_ids)} nodes confirmed dead: {actually_dead}")

    # ---- Pha 3: Sau lỗi ----
    print(f"\n--- Phase 3: After failure ---")
    alive_ports = [p for p in all_ports if p not in killed_ports]
    alive_set = set(alive_ports)
    valid_ports = list(keyword_holder_ports & alive_set)
    after = await run_query("AFTER", source_port, keyword,
                            alive_ports=alive_ports,
                            valid_ports=valid_ports)

    # ---- Tóm tắt ----
    print(f"\n" + "=" * 60)
    print("FAILURE DEMO SUMMARY - Hub Attack")
    print("=" * 60)
    print(f"Holders left (constant): {total_holders}")
    print(f"{'Metric':<30} {'Before':>10} {'After':>10} {'Change':>10}")
    print("-" * 60)
    print(
        f"{'matched_peers_count':<30} {before['matched_peers_count']:>10} "
        f"{after['matched_peers_count']:>10} "
        f"{after['matched_peers_count'] - before['matched_peers_count']:>+10}"
    )
    print(
        f"{'coverage_ratio':<30} {before['coverage_ratio']:.3f}{'':>8} "
        f"{after['coverage_ratio']:.3f}{'':>8} "
        f"{after['coverage_ratio'] - before['coverage_ratio']:+.3f}{'':>8}"
    )
    print(
        f"{'messages_sent':<30} {before['messages_sent']:>10} "
        f"{after['messages_sent']:>10} "
        f"{after['messages_sent'] - before['messages_sent']:>+10}"
    )
    print(
        f"{'duplicate_ratio':<30} {before['duplicate_ratio']:.3f}{'':>8} "
        f"{after['duplicate_ratio']:.3f}{'':>8} "
        f"{after['duplicate_ratio'] - before['duplicate_ratio']:+.3f}{'':>8}"
    )
    print(
        f"{'avg_hops':<30} {before['avg_hops']:.2f}{'':>9} "
        f"{after['avg_hops']:.2f}{'':>9} "
        f"{after['avg_hops'] - before['avg_hops']:+.2f}{'':>9}"
    )
    print(
        f"{'avg_latency_ms':<30} {before['avg_latency_ms']:.1f}{'':>8} "
        f"{after['avg_latency_ms']:.1f}{'':>8} "
        f"{after['avg_latency_ms'] - before['avg_latency_ms']:+.1f}{'':>8}"
    )
    print(
        f"{'failed_forward_count':<30} {before['failed_forward_count']:>10} "
        f"{after['failed_forward_count']:>10} "
        f"{after['failed_forward_count'] - before['failed_forward_count']:>+10}"
    )
    print(
        f"{'dead_neighbors_detected':<30} {before['dead_neighbors_detected']:>10} "
        f"{after['dead_neighbors_detected']:>10} "
        f"{after['dead_neighbors_detected'] - before['dead_neighbors_detected']:>+10}"
    )
    print("-" * 60)

    # ---- Lưu báo cáo ----
    os.makedirs("results", exist_ok=True)
    with open("results/failure_report.txt", "w") as f:
        f.write("Failure Demo Report - Hub Attack\n")
        f.write("-" * 50 + "\n")
        f.write(f"Killed hubs (non-holders): {kill_ids}\n")
        f.write(f"Source peer:  {source_id}\n")
        f.write(f"Keyword:      '{keyword}'\n")
        f.write(f"Total holders: {total_holders} (constant)\n")
        f.write(f"TTL:          5\n\n")
        f.write(f"{'Metric':<30} {'Before':>10} {'After':>10}\n")
        f.write("-" * 50 + "\n")
        for key in ["matched_peers_count", "coverage_ratio", "messages_sent",
                      "duplicate_ratio", "avg_hops", "avg_latency_ms",
                      "failed_forward_count", "dead_neighbors_detected",
                      "isolated_nodes_count", "source_isolated",
                      "rejoin_attempts", "rejoin_success_count"]:
            b = before[key]
            a = after[key]
            if isinstance(b, float):
                f.write(f"{key:<30} {b:>10.3f} {a:>10.3f}\n")
            else:
                f.write(f"{key:<30} {b:>10} {a:>10}\n")
        f.write("\nExplanation:\n")
        f.write(
            "Killing high-degree hub nodes fragments the overlay, making it "
            "harder for queries to reach keyword-holders.\n"
            "File copies still exist (holders_left is constant), but the "
            "TTL-limited flood can no longer reach them all.\n"
            "Coverage decreases as more hubs are removed.\n"
        )
    print(f"\nSaved: results/failure_report.txt")

    # ---- Sinh biểu đồ nếu có matplotlib ----
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt

        labels = ['Before\n(0 killed)', f'After\n({K} killed)']
        values = [before['matched_peers_count'], after['matched_peers_count']]
        covs = [before['coverage_ratio'], after['coverage_ratio']]

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

        # Peer match
        bars1 = ax1.bar(labels, values, color=['#4CAF50', '#F44336'], width=0.5)
        for bar, v in zip(bars1, values):
            ax1.text(bar.get_x() + bar.get_width() / 2,
                     bar.get_height() + 0.3, str(v),
                     ha='center', fontsize=12, fontweight='bold')
        ax1.set_ylabel("Matched Peers Count", fontsize=12)
        ax1.set_title("Search Coverage Before/After Failure", fontsize=13)
        ax1.grid(True, alpha=0.3, axis='y')

        # Tỉ lệ coverage
        bars2 = ax2.bar(labels, covs, color=['#4CAF50', '#F44336'], width=0.5)
        for bar, v in zip(bars2, covs):
            ax2.text(bar.get_x() + bar.get_width() / 2,
                     bar.get_height() + 0.005, f"{v:.2f}",
                     ha='center', fontsize=12, fontweight='bold')
        ax2.set_ylabel("Coverage Ratio", fontsize=12)
        ax2.set_title("Coverage Ratio Before/After Failure", fontsize=13)
        ax2.grid(True, alpha=0.3, axis='y')

        plt.suptitle(
            f"Failure Demo: {len(kill_ids)} Keyword-Holding Nodes Killed\n"
            f"(Nodes {kill_ids})",
            fontsize=14
        )
        plt.tight_layout()
        plt.savefig("results/failure_before_after.png", dpi=150)
        plt.close()
        print("Saved: results/failure_before_after.png (chart)")
    except ImportError:
        print("[failure_demo.py] matplotlib not installed - chart skipped")

    print("\nDone.")


if __name__ == "__main__":
    asyncio.run(main())
