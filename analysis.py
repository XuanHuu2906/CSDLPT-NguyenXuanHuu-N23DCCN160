import asyncio
import csv
import json
import os
import random
import shutil
import statistics as st

from query import (send_query, reset_all_peers,
                   collect_all_metrics, send_message, ALL_PORTS,
                   restart_all_peers)

BLOCKED_PEERS = set()  # No blocked peers with port range 6000-6099
TTLS = [3, 5, 7]
RUNS_PER_TTL = 10
QUERY_TIMEOUT = 5


# =====================================================================
# Topology helpers
# =====================================================================
def load_topology(path="topology.json"):
    return json.load(open(path))


def build_adjacency(topology):
    n = len(topology["peers"])
    adj = {i: [] for i in range(n)}
    for a, b in topology["edges"]:
        adj[a].append(b)
        adj[b].append(a)
    return adj


def count_peers_with_file(topology, keyword):
    """Ground-truth: how many peers have this keyword in their file list."""
    return sum(
        1 for files in topology["files"].values() if keyword in files
    )


def count_peers_with_file_by_id(topology, peer_ids, keyword):
    """How many peers from a given set have the keyword."""
    return sum(
        1 for pid in peer_ids
        if keyword in topology["files"].get(str(pid), [])
    )


def bfs_reachable(topology, origin_id, ttl):
    """Return set of peer IDs reachable within *ttl* hops from origin_id."""
    adj = build_adjacency(topology)
    visited = {origin_id}
    queue = [(origin_id, 0)]
    for node, dist in queue:
        if dist < ttl:
            for nb in adj[node]:
                if nb not in visited:
                    visited.add(nb)
                    queue.append((nb, dist + 1))
    return visited


def get_file_pool(topology):
    return list(set(f for files in topology["files"].values() for f in files))


def peer_id_from_port(topology, port):
    """Convert a peer's port number to its 0-based peer ID using topology."""
    for p in topology["peers"]:
        if p["port"] == port:
            return p["id"]
    return port - topology["peers"][0]["port"]  # fallback


def peer_port(topology, peer_id):
    """Convert a 0-based peer ID to its port number."""
    return topology["peers"][peer_id]["port"]


# =====================================================================
# Single experiment runner
# =====================================================================
async def run_single_experiment(source_port, keyword, ttl, timeout=QUERY_TIMEOUT):
    await reset_all_peers()
    metrics = await send_query(source_port, keyword, ttl, timeout)

    # Compute derived metrics
    total_with_file = count_peers_with_file(load_topology(), keyword)

    metrics["ttl"] = ttl
    metrics["keyword"] = keyword
    metrics["origin_port"] = source_port
    metrics["total_peers_having_file"] = total_with_file

    matched = metrics["matched_peers_count"]
    if matched > total_with_file:
        raise AssertionError(
            f"BUG: matched_peers_count={matched} > "
            f"total_peers_having_file={total_with_file} "
            f"(keyword='{keyword}', ttl={ttl}, origin_port={source_port})"
        )

    metrics["coverage_ratio"] = (
        matched / max(total_with_file, 1)
    )
    metrics["duplicate_ratio"] = (
        metrics["duplicate_queries_dropped"] / max(metrics["messages_sent"], 1)
    )

    hops = metrics.get("hops", [])
    metrics["avg_hops"] = st.mean(hops) if hops else 0.0
    metrics["min_hops"] = min(hops) if hops else 0
    metrics["max_hops"] = max(hops) if hops else 0

    lats = metrics.get("latencies", [])
    metrics["avg_latency_ms"] = st.mean(lats) if lats else 0.0
    metrics["min_latency_ms"] = min(lats) if lats else 0.0
    metrics["max_latency_ms"] = max(lats) if lats else 0.0

    metrics["overhead_per_match"] = (
        metrics["messages_sent"] / max(metrics["matched_peers_count"], 1)
    )

    return metrics


# =====================================================================
# Multi-experiment runner
# =====================================================================
async def run_experiments(ttl_values=None, n_runs=None):
    if ttl_values is None:
        ttl_values = TTLS
    if n_runs is None:
        n_runs = RUNS_PER_TTL

    topology = load_topology()
    pool = get_file_pool(topology)
    valid_sources = [i for i in range(100) if i not in BLOCKED_PEERS]
    all_results = []

    # Pre-generate test cases so each case uses the same source & keyword for all TTLs
    test_cases = []
    for run in range(n_runs):
        source_id = random.choice(valid_sources)
        source_port = topology["peers"][source_id]["port"]
        keyword = random.choice(pool)
        total_with_file = count_peers_with_file(topology, keyword)
        test_cases.append({
            "source_id": source_id,
            "source_port": source_port,
            "keyword": keyword,
            "total_with_file": total_with_file,
        })

    for case_idx, case in enumerate(test_cases):
        print(f"\nCASE {case_idx + 1}/{n_runs} | "
              f"source={case['source_id']} | "
              f"keyword='{case['keyword']}' | "
              f"total_peers_having_file={case['total_with_file']}")
        for ttl in ttl_values:
            metrics = await run_single_experiment(
                case["source_port"], case["keyword"], ttl
            )
            metrics["run"] = case_idx
            all_results.append(metrics)

            hops_str = (
                f"hops={metrics['avg_hops']:.1f}"
                if metrics['avg_hops'] > 0 else "hops=N/A"
            )
            lat_str = (
                f"lat={metrics['avg_latency_ms']:.1f}ms"
                if metrics['avg_latency_ms'] > 0 else "lat=N/A"
            )
            print(
                f"  TTL={ttl} "
                f"matched={metrics['matched_peers_count']:3d}/"
                f"{case['total_with_file']:2d} "
                f"cov={metrics['coverage_ratio']:.2f} "
                f"sent={metrics['messages_sent']:6d} "
                f"dup={metrics['duplicate_queries_dropped']:6d} "
                f"{hops_str} {lat_str}"
            )
    return all_results


# =====================================================================
# Compute per-TTL statistics
# =====================================================================
def compute_statistics(all_results):
    """Group results by TTL and compute mean/std for key metrics."""
    by_ttl = {}
    for r in all_results:
        by_ttl.setdefault(r["ttl"], []).append(r)

    stats = {}
    for ttl, runs in by_ttl.items():
        coverage = [r["coverage_ratio"] for r in runs]
        matched = [r["matched_peers_count"] for r in runs]
        sent = [r["messages_sent"] for r in runs]
        dropped = [r["duplicate_queries_dropped"] for r in runs]
        dup_ratio = [r["duplicate_ratio"] for r in runs]
        avg_hops = [r["avg_hops"] for r in runs]
        avg_lat = [r["avg_latency_ms"] for r in runs]
        overhead = [r["overhead_per_match"] for r in runs]
        failed = [r["failed_forward_count"] for r in runs]
        dead = [r["dead_neighbors_detected"] for r in runs]

        def mean_std(values):
            return st.mean(values), st.stdev(values) if len(values) > 1 else 0

        stats[ttl] = {
            "coverage_mean": st.mean(coverage),
            "coverage_std": st.stdev(coverage) if len(coverage) > 1 else 0,
            "matched_mean": st.mean(matched),
            "matched_std": st.stdev(matched) if len(matched) > 1 else 0,
            "matched_min": min(matched),
            "matched_max": max(matched),
            "sent_mean": st.mean(sent),
            "sent_std": st.stdev(sent) if len(sent) > 1 else 0,
            "dropped_mean": st.mean(dropped),
            "dup_ratio_mean": st.mean(dup_ratio),
            "avg_hops_mean": st.mean(avg_hops),
            "avg_lat_mean": st.mean(avg_lat),
            "overhead_per_match_mean": st.mean(overhead),
            "failed_mean": st.mean(failed),
            "dead_mean": st.mean(dead),
        }
    return stats


# =====================================================================
# CSV export
# =====================================================================
CSV_COLUMNS = [
    "ttl", "run", "keyword", "origin_port",
    "matched_peers_count", "total_peers_having_file", "coverage_ratio",
    "messages_sent", "duplicate_queries_dropped", "duplicate_ratio",
    "avg_hops", "min_hops", "max_hops",
    "avg_latency_ms", "min_latency_ms", "max_latency_ms",
    "failed_forward_count", "dead_neighbors_detected", "overhead_per_match"
]


def save_csv(all_results, path="results/metrics_ttl.csv"):
    os.makedirs("results", exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        w.writeheader()
        w.writerows(all_results)
    print(f"Saved: {path}")


def save_summary_csv(stats, path="results/metrics_summary.csv"):
    os.makedirs("results", exist_ok=True)
    rows = []
    for ttl in sorted(stats.keys()):
        s = stats[ttl]
        rows.append({
            "ttl": ttl,
            "coverage_mean": f"{s['coverage_mean']:.3f}",
            "coverage_std": f"{s['coverage_std']:.3f}",
            "matched_mean": f"{s['matched_mean']:.1f}",
            "matched_std": f"{s['matched_std']:.1f}",
            "sent_mean": f"{s['sent_mean']:.1f}",
            "sent_std": f"{s['sent_std']:.1f}",
            "dup_ratio_mean": f"{s['dup_ratio_mean']:.3f}",
            "avg_hops_mean": f"{s['avg_hops_mean']:.2f}",
            "avg_lat_mean": f"{s['avg_lat_mean']:.2f}",
            "overhead_per_match_mean": f"{s['overhead_per_match_mean']:.1f}",
            "failed_mean": f"{s['failed_mean']:.1f}",
            "dead_mean": f"{s['dead_mean']:.1f}",
        })
    cols = [
        "ttl", "coverage_mean", "coverage_std",
        "matched_mean", "matched_std",
        "sent_mean", "sent_std",
        "dup_ratio_mean",
        "avg_hops_mean", "avg_lat_mean",
        "overhead_per_match_mean",
        "failed_mean", "dead_mean"
    ]
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    print(f"Saved: {path}")


# =====================================================================
# Print stats table
# =====================================================================
def print_stats_table(stats):
    sep = "=" * 90
    print(f"\n{sep}")
    h = (
        f"{'TTL':>4} | {'coverage':>9} | {'matched':>7} | "
        f"{'sent':>7} | {'dup_ratio':>9} | {'hops':>5} | "
        f"{'lat_ms':>7} | {'overhead':>8}"
    )
    print(h)
    print("-" * 90)
    for ttl in sorted(stats.keys()):
        s = stats[ttl]
        print(
            f"{ttl:>4} | "
            f"{s['coverage_mean']:.2f}+-{s['coverage_std']:.2f} | "
            f"{s['matched_mean']:.1f}+-{s['matched_std']:.1f} | "
            f"{s['sent_mean']:.0f}+-{s['sent_std']:.0f} | "
            f"{s['dup_ratio_mean']:.3f} | "
            f"{s['avg_hops_mean']:.2f} | "
            f"{s['avg_lat_mean']:.1f} | "
            f"{s['overhead_per_match_mean']:.1f}"
        )
    print(sep)


# =====================================================================
# Charts (matplotlib)
# =====================================================================
try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    HAVE_MPL = True
except ImportError:
    HAVE_MPL = False
    print("[analysis.py] matplotlib not installed — charts will be skipped")

# NetworkX for topology graphs
try:
    import networkx as nx
    HAVE_NX = True
except ImportError:
    HAVE_NX = False
    print("[analysis.py] networkx not installed — topology graphs will be skipped")


def plot_coverage_vs_overhead(stats):
    """Coverage ratio vs messages sent (errorbar chart)."""
    colors = {3: '#2196F3', 5: '#4CAF50', 7: '#F44336'}
    fig, ax = plt.subplots(figsize=(8, 6))
    for ttl in [3, 5, 7]:
        if ttl not in stats:
            continue
        s = stats[ttl]
        ax.errorbar(
            x=s["sent_mean"], y=s["coverage_mean"],
            xerr=s["sent_std"], yerr=s["coverage_std"],
            label=f"TTL={ttl}", color=colors[ttl],
            marker='o', markersize=12, capsize=5
        )
        ax.annotate(
            f"TTL={ttl}",
            xy=(s["sent_mean"], s["coverage_mean"]),
            xytext=(10, 8), textcoords='offset points', fontsize=11
        )
    ax.set_xlabel("Total Messages Sent — Network Overhead", fontsize=12)
    ax.set_ylabel("Coverage Ratio — Search Coverage", fontsize=12)
    ax.set_title(
        "Search Coverage vs Network Overhead\n"
        "(Gnutella-style Flooding, 100 peers)",
        fontsize=13
    )
    ax.set_ylim(0, 1.05)
    ax.legend()
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig("results/chart_coverage_vs_overhead.png", dpi=150)
    plt.close()
    print("Saved: results/chart_coverage_vs_overhead.png")


def plot_ttl_metrics(stats):
    """2×2 subplot: coverage, messages, duplicate ratio, latency."""
    ttls = sorted(stats.keys())
    fig, axes = plt.subplots(2, 2, figsize=(12, 10))

    # Coverage
    ax = axes[0, 0]
    cov_means = [stats[t]["coverage_mean"] for t in ttls]
    cov_stds = [stats[t]["coverage_std"] for t in ttls]
    ax.bar([str(t) for t in ttls], cov_means, yerr=cov_stds,
           color=['#2196F3', '#4CAF50', '#F44336'], capsize=5)
    ax.set_xlabel("TTL")
    ax.set_ylabel("Coverage Ratio")
    ax.set_title("Search Coverage")
    ax.set_ylim(0, 1.05)
    ax.grid(True, alpha=0.3, axis='y')

    # Messages
    ax = axes[0, 1]
    msg_means = [stats[t]["sent_mean"] for t in ttls]
    msg_stds = [stats[t]["sent_std"] for t in ttls]
    ax.bar([str(t) for t in ttls], msg_means, yerr=msg_stds,
           color=['#2196F3', '#4CAF50', '#F44336'], capsize=5)
    ax.set_xlabel("TTL")
    ax.set_ylabel("Messages Sent")
    ax.set_title("Network Overhead")
    ax.grid(True, alpha=0.3, axis='y')

    # Duplicate ratio
    ax = axes[1, 0]
    dup_means = [stats[t]["dup_ratio_mean"] for t in ttls]
    ax.bar([str(t) for t in ttls], dup_means,
           color=['#2196F3', '#4CAF50', '#F44336'])
    ax.set_xlabel("TTL")
    ax.set_ylabel("Duplicate Ratio")
    ax.set_title("Duplicate Query Ratio")
    ax.grid(True, alpha=0.3, axis='y')

    # Latency
    ax = axes[1, 1]
    lat_means = [stats[t]["avg_lat_mean"] for t in ttls]
    ax.bar([str(t) for t in ttls], lat_means,
           color=['#2196F3', '#4CAF50', '#F44336'])
    ax.set_xlabel("TTL")
    ax.set_ylabel("Avg Latency (ms)")
    ax.set_title("Average Query Latency")
    ax.grid(True, alpha=0.3, axis='y')

    plt.suptitle("TTL vs Key Metrics", fontsize=14, y=1.02)
    plt.tight_layout()
    plt.savefig("results/chart_ttl_metrics.png", dpi=150)
    plt.close()
    print("Saved: results/chart_ttl_metrics.png")


def plot_duplicate_ratio(stats):
    """Duplicate ratio bar chart (kept for backward compat)."""
    ttl_values = sorted(stats.keys())
    ratios = [stats[t]["dup_ratio_mean"] for t in ttl_values]
    colors = ['#2196F3', '#4CAF50', '#F44336']
    fig, ax = plt.subplots(figsize=(7, 5))
    bars = ax.bar(
        [str(t) for t in ttl_values], ratios,
        color=colors[:len(ttl_values)], width=0.5
    )
    for bar, r in zip(bars, ratios):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 0.005,
            f"{r:.1%}", ha='center', fontsize=11
        )
    ax.set_xlabel("TTL", fontsize=12)
    ax.set_ylabel("Duplicate Query Ratio", fontsize=12)
    ax.set_title("Duplicate Suppression Effectiveness\n(% of QUERY messages dropped)",
                 fontsize=13)
    plt.tight_layout()
    plt.savefig("results/chart_duplicate_ratio.png", dpi=150)
    plt.close()
    print("Saved: results/chart_duplicate_ratio.png")


def plot_failure_case(results):
    """Failure case: matched peers vs number of killed high-degree nodes."""
    killed = [r["killed"] for r in results]
    matched = [r["matched"] for r in results]
    fig, ax = plt.subplots(figsize=(8, 6))
    ax.plot(killed, matched, '-o', color='#F44336', lw=2, ms=10)
    ax.fill_between(killed, matched, alpha=0.1, color='#F44336')
    for k, m in zip(killed, matched):
        ax.annotate(
            str(m), xy=(k, m), xytext=(5, 5),
            textcoords='offset points', fontsize=10
        )
    ax.set_xlabel("Number of High-Degree Nodes Killed", fontsize=12)
    ax.set_ylabel("Matched Peers Count (TTL=5)", fontsize=12)
    ax.set_title(
        "Failure Case: Hub Attack on Unstructured Overlay\n"
        "(Killing high-degree nodes partitions the network)",
        fontsize=13
    )
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig("results/chart_failure_case.png", dpi=150)
    plt.close()
    print("Saved: results/chart_failure_case.png")


# =====================================================================
# Topology graph (NetworkX)
# =====================================================================
def save_topology_graph(topology):
    if not HAVE_NX or not HAVE_MPL:
        return
    G = nx.Graph()
    for p in topology["peers"]:
        G.add_node(p["id"])
    for a, b in topology["edges"]:
        G.add_edge(a, b)

    fig, ax = plt.subplots(figsize=(14, 10))
    pos = nx.spring_layout(G, seed=42, k=0.3)
    nx.draw_networkx_nodes(G, pos, node_size=30, node_color='#2196F3',
                           alpha=0.8, ax=ax)
    nx.draw_networkx_edges(G, pos, alpha=0.3, ax=ax)
    ax.set_title(
        f"P2P Network Topology\n"
        f"({len(topology['peers'])} nodes, {len(topology['edges'])} edges)",
        fontsize=14
    )
    ax.axis('off')
    plt.tight_layout()
    plt.savefig("results/topology_graph.png", dpi=150)
    plt.close()
    print("Saved: results/topology_graph.png")


def save_query_propagation_graph(topology, origin_id, keyword, ttl, suffix):
    """BFS from origin to find reachable nodes, highlight matches."""
    if not HAVE_NX or not HAVE_MPL:
        return

    G = nx.Graph()
    for p in topology["peers"]:
        G.add_node(p["id"])
    for a, b in topology["edges"]:
        G.add_edge(a, b)

    reachable = bfs_reachable(topology, origin_id, ttl)
    matched = set()
    for nid in reachable:
        if keyword in topology["files"].get(str(nid), []):
            matched.add(nid)

    fig, ax = plt.subplots(figsize=(14, 10))
    pos = nx.spring_layout(G, seed=42, k=0.3)

    # Draw full topology in light gray
    all_ids = set(p["id"] for p in topology["peers"])
    unreachable = all_ids - reachable
    nx.draw_networkx_nodes(G, pos, nodelist=list(unreachable),
                           node_size=15, node_color='#e0e0e0', alpha=0.4, ax=ax)

    # Reachable nodes
    reachable_no_origin_matched = reachable - matched - {origin_id}
    nx.draw_networkx_nodes(G, pos, nodelist=list(reachable_no_origin_matched),
                           node_size=30, node_color='#90CAF9', alpha=0.8, ax=ax)

    # Matched nodes
    if matched:
        nx.draw_networkx_nodes(G, pos, nodelist=list(matched),
                               node_size=50, node_color='#4CAF50', alpha=0.9,
                               ax=ax)

    # Origin
    nx.draw_networkx_nodes(G, pos, nodelist=[origin_id],
                           node_size=80, node_color='#F44336', alpha=0.9,
                           ax=ax)

    # Edges within reachable subgraph
    reachable_edges = [
        (a, b) for a, b in topology["edges"]
        if a in reachable and b in reachable
    ]
    nx.draw_networkx_edges(G, pos, edgelist=list(reachable_edges),
                           alpha=0.5, edge_color='#2196F3', ax=ax)

    # Legend
    from matplotlib.lines import Line2D
    legend_elements = [
        Line2D([0], [0], marker='o', color='w', label='Origin',
               markerfacecolor='#F44336', markersize=10),
        Line2D([0], [0], marker='o', color='w', label='Matched (has file)',
               markerfacecolor='#4CAF50', markersize=10),
        Line2D([0], [0], marker='o', color='w', label='Reached by query',
               markerfacecolor='#90CAF9', markersize=10),
        Line2D([0], [0], marker='o', color='w', label='Unreached',
               markerfacecolor='#e0e0e0', markersize=10),
    ]
    ax.legend(handles=legend_elements, loc='upper right', fontsize=10)

    ax.set_title(
        f"Query Propagation — TTL={ttl}\n"
        f"Origin=Peer {origin_id}, Keyword='{keyword}'",
        fontsize=14
    )
    ax.axis('off')
    plt.tight_layout()
    path = f"results/query_path_ttl_{suffix}.png"
    plt.savefig(path, dpi=150)
    plt.close()
    print(f"Saved: {path}")


# =====================================================================
# Topology stats
# =====================================================================
def save_topology_stats(topology):
    if not HAVE_NX:
        print("[analysis.py] networkx not installed — topology stats will be basic")
        _save_topology_stats_basic(topology)
        return

    G = nx.Graph()
    for p in topology["peers"]:
        G.add_node(p["id"])
    for a, b in topology["edges"]:
        G.add_edge(a, b)

    degrees = sorted(G.degree(), key=lambda x: x[1], reverse=True)
    is_conn = nx.is_connected(G)
    diameter = nx.diameter(G) if is_conn else "N/A (disconnected)"
    avg_spl = nx.average_shortest_path_length(G) if is_conn else "N/A"
    density = nx.density(G)

    with open("results/topology_stats.txt", "w") as f:
        f.write("Topology Statistics\n")
        f.write("-" * 50 + "\n")
        f.write(f"Number of peers:     {G.number_of_nodes()}\n")
        f.write(f"Number of edges:     {G.number_of_edges()}\n")
        f.write(f"Average degree:      {sum(d for _, d in degrees) / len(degrees):.2f}\n")
        f.write(f"Min degree:          {min(d for _, d in degrees)}\n")
        f.write(f"Max degree:          {max(d for _, d in degrees)}\n")
        f.write(f"Network density:     {density:.6f}\n")
        f.write(f"Is connected:        {is_conn}\n")
        f.write(f"Diameter:            {diameter}\n")
        f.write(f"Avg shortest path:   {avg_spl}\n\n")
        f.write("High-degree nodes (top 10):\n")
        for i, (nid, deg) in enumerate(degrees[:10], 1):
            f.write(f"  {i}. Peer {nid} (degree={deg})\n")
    print("Saved: results/topology_stats.txt")


def _save_topology_stats_basic(topology):
    n = len(topology["peers"])
    adj = build_adjacency(topology)
    degrees = sorted(
        [(i, len(adj[i])) for i in range(n)], key=lambda x: x[1], reverse=True
    )
    avg_deg = sum(d for _, d in degrees) / n
    min_deg = min(d for _, d in degrees)
    max_deg = max(d for _, d in degrees)
    density = len(topology["edges"]) / (n * (n - 1) / 2)

    # BFS helpers
    def bfs_visited(adj, src):
        visited = {src}
        q = [src]
        for node in q:
            for nb in adj[node]:
                if nb not in visited:
                    visited.add(nb)
                    q.append(nb)
        return visited

    def bfs_max_dist(adj, src):
        dist = {src: 0}
        q = [src]
        for node in q:
            for nb in adj[node]:
                if nb not in dist:
                    dist[nb] = dist[node] + 1
                    q.append(nb)
        return max(dist.values())

    is_conn = len(bfs_visited(adj, 0)) == n

    diameter = 0
    if is_conn:
        for src in range(n):
            diameter = max(diameter, bfs_max_dist(adj, src))

    with open("results/topology_stats.txt", "w") as f:
        f.write("Topology Statistics\n")
        f.write("-" * 50 + "\n")
        f.write(f"Number of peers:     {n}\n")
        f.write(f"Number of edges:     {len(topology['edges'])}\n")
        f.write(f"Average degree:      {avg_deg:.2f}\n")
        f.write(f"Min degree:          {min_deg}\n")
        f.write(f"Max degree:          {max_deg}\n")
        f.write(f"Network density:     {density:.6f}\n")
        f.write(f"Is connected:        {is_conn}\n")
        f.write(f"Diameter:            {diameter} hops\n\n")
        f.write("High-degree nodes (top 10):\n")
        for i, (nid, deg) in enumerate(degrees[:10], 1):
            f.write(f"  {i}. Peer {nid} (degree={deg})\n")
    print("Saved: results/topology_stats.txt")


# =====================================================================
# Failure experiment
# =====================================================================
async def run_failure_experiment(topology):
    adj = build_adjacency(topology)
    degrees = sorted(range(100), key=lambda i: len(adj[i]), reverse=True)
    high_degree_ids = degrees[:20]

    source_id = next(
        i for i in range(100)
        if i not in high_degree_ids and i not in BLOCKED_PEERS
    )
    source_port = topology["peers"][source_id]["port"]
    keyword = random.choice(get_file_pool(topology))

    results = []

    # Baseline (0 killed)
    await reset_all_peers()
    metrics = await send_query(source_port, keyword, ttl=5)
    results.append({"killed": 0, "matched": metrics["matched_peers_count"]})
    print(f"Baseline (0 killed): matched={metrics['matched_peers_count']}")

    # Incrementally kill high-degree nodes (5 per batch)
    killed = []
    for step in range(4):
        to_kill = high_degree_ids[step * 5: (step + 1) * 5]
        for node_id in to_kill:
            await send_message(topology["peers"][node_id]["port"], {"type": "SHUTDOWN"})
            killed.append(node_id)
        await asyncio.sleep(1.0)

        await reset_all_peers()
        metrics = await send_query(source_port, keyword, ttl=5)
        results.append({
            "killed": len(killed),
            "matched": metrics["matched_peers_count"]
        })
        print(f"Killed {len(killed):2d} nodes: matched={metrics['matched_peers_count']}")

    return results


def save_failure_report(results, killed_ids):
    os.makedirs("results", exist_ok=True)
    with open("results/failure_report.txt", "w") as f:
        f.write("Failure Demo Report\n")
        f.write("-" * 50 + "\n")
        f.write(f"Killed nodes: {sorted(killed_ids)}\n\n")
        f.write("Step-by-step results:\n")
        for r in results:
            f.write(
                f"  Killed={r['killed']:2d}: matched_peers_count={r['matched']}\n"
            )
        f.write("\nExplanation:\n")
        f.write(
            "After high-degree nodes leave, the overlay loses important "
            "routing paths.\n"
            "Peers detect failed neighbors via heartbeat and stop forwarding "
            "messages to them.\n"
            "Search still works if the remaining overlay is connected, "
            "but coverage decreases.\n"
        )
    print("Saved: results/failure_report.txt")


# =====================================================================
# Main
# =====================================================================
async def main():
    topology = load_topology()
    os.makedirs("results", exist_ok=True)

    # Clean old output before starting fresh analysis
    for f in os.listdir("results"):
        if f.endswith((".csv", ".png", ".txt")):
            os.remove(os.path.join("results", f))
    print("Cleaned old results.")

    # ---- Topology graphs & stats ----
    print("\nSaving topology graph...")
    save_topology_graph(topology)

    print("\nSaving topology statistics...")
    save_topology_stats(topology)

    # ---- Standard experiments ----
    print("\nRunning standard experiments (TTL=3,5,7)...")
    random.seed(42)
    all_results = await run_experiments(ttl_values=TTLS, n_runs=RUNS_PER_TTL)

    stats = compute_statistics(all_results)
    print_stats_table(stats)

    # CSV export
    save_csv(all_results)
    save_summary_csv(stats)

    # Charts
    if HAVE_MPL:
        plot_coverage_vs_overhead(stats)
        plot_ttl_metrics(stats)
        plot_duplicate_ratio(stats)

    # Query propagation graphs: use same sample (first test case) for all TTLs
    if HAVE_MPL:
        print("\nSaving query propagation graphs...")
        first_case_results = [r for r in all_results if r["run"] == 0]
        if first_case_results:
            sample = first_case_results[0]
            sample_origin_id = peer_id_from_port(topology, sample["origin_port"])
            sample_keyword = sample["keyword"]
            for ttl in TTLS:
                save_query_propagation_graph(
                    topology,
                    sample_origin_id,
                    sample_keyword,
                    ttl,
                    str(ttl)
                )

    # ---- Failure experiment ----
    print("\nRunning failure experiment...")
    adj = build_adjacency(topology)
    degrees = sorted(range(100), key=lambda i: len(adj[i]), reverse=True)
    high_degree_ids = degrees[:20]
    failure_results = await run_failure_experiment(topology)
    if HAVE_MPL:
        plot_failure_case(failure_results)
    save_failure_report(failure_results, high_degree_ids)

    print("\nAll experiments complete. Results saved in results/")


if __name__ == "__main__":
    # Only restart peers if none are currently running (to avoid TIME_WAIT on Windows)
    async def _ensure_peers():
        topo = json.load(open("topology.json"))
        for p in topo["peers"][:3]:
            if await send_message(p["port"], {"type": "PING"}):
                print("Peers already running — no restart needed.")
                return
        print("No peers detected — starting fresh...")
        await restart_all_peers()

    asyncio.run(_ensure_peers())
    asyncio.run(main())
