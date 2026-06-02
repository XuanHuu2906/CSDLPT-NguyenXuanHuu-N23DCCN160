import asyncio
import json
import os
import queue
import random
import statistics as st
import subprocess
import sys
import threading
import time
import traceback

import flask

from query import (ALL_PORTS, send_query, reset_all_peers,
                   collect_all_metrics, send_message,
                   restart_all_peers)
from analysis import (count_peers_with_file, get_file_pool,
                      compute_statistics, TTLS, RUNS_PER_TTL,
                      peer_id_from_port, build_adjacency)

BLOCKED_PEERS = set()  # No blocked peers with port range 6000-6099

app = flask.Flask(__name__)

_event_queues = []
_analysis_stats = None
_analysis_charts = []
_analysis_lock = threading.Lock()
_analysis_running = False


def _get_topology():
    return json.load(open("topology.json"))


# =====================================================================
# Routes
# =====================================================================

@app.route("/")
def index():
    return flask.render_template("dashboard.html")


@app.route("/api/peers/status")
def peer_status():
    topology = _get_topology()
    n = len(topology["peers"])
    file_count = {str(i): len(topology["files"].get(str(i), [])) for i in range(n)}

    async def _check():
        results = {}

        # Single shared server for all 100 peer checks - avoids
        # Python 3.13 ProactorEventLoop assertion when closing
        # 100 temporary servers in rapid succession.
        current_waiter = {}  # shared mutable: {"event": Event, "data": {}}

        async def collect(reader, writer):
            try:
                data = await asyncio.wait_for(
                    reader.readline(), timeout=1.0
                )
                msg = json.loads(data.decode().strip())
                if msg["type"] == "METRICS_RESPONSE":
                    m = msg.get("metrics", {})
                    d = current_waiter.get("data")
                    if d is not None:
                        d.update(m)
                    ev = current_waiter.get("event")
                    if ev is not None:
                        ev.set()
            except Exception:
                pass
            finally:
                writer.close()

        shared_server = await asyncio.start_server(
            collect, "127.0.0.1", 0
        )
        reply_port = shared_server.sockets[0].getsockname()[1]

        try:
            for i in range(n):
                port = topology["peers"][i]["port"]
                info = {
                    "peer_id": i,
                    "port": port,
                    "online": False,
                    "messages_sent": 0,
                    "duplicate_queries_dropped": 0,
                    "matched_peers_count": 0,
                    "failed_forward_count": 0,
                    "dead_neighbors_detected": 0,
                    "file_count": file_count.get(str(i), 0),
                    "blocked": i in BLOCKED_PEERS,
                }
                if i in BLOCKED_PEERS:
                    results[str(i)] = info
                    continue

                received_data = {}
                received = asyncio.Event()
                current_waiter["event"] = received
                current_waiter["data"] = received_data

                try:
                    ok = await asyncio.wait_for(
                        send_message(port, {"type": "METRICS_REQUEST", "reply_port": reply_port}),
                        timeout=1.5
                    )
                except asyncio.TimeoutError:
                    ok = False
                if ok:
                    try:
                        await asyncio.wait_for(received.wait(), timeout=1.0)
                        info["online"] = True
                        info["messages_sent"] = received_data.get("messages_sent", 0)
                        info["duplicate_queries_dropped"] = received_data.get("duplicate_queries_dropped", 0)
                        info["matched_peers_count"] = received_data.get("matched_peers_count", 0)
                        info["failed_forward_count"] = received_data.get("failed_forward_count", 0)
                        info["dead_neighbors_detected"] = received_data.get("dead_neighbors_detected", 0)
                    except asyncio.TimeoutError:
                        pass

                current_waiter.clear()
                results[str(i)] = info
        finally:
            shared_server.close()
            try:
                await shared_server.wait_closed()
            except Exception:
                pass

        return results

    loop = asyncio.new_event_loop()
    status = loop.run_until_complete(_check())
    loop.close()
    return flask.jsonify(status)


@app.route("/api/peers/revive", methods=["POST"])
def revive_peers():
    topology = _get_topology()
    revived = []

    async def _check(port):
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection("127.0.0.1", port), timeout=0.5
            )
            writer.close()
            await writer.wait_closed()
            return True  # online
        except Exception:
            return False  # offline

    async def _scan():
        results = []
        for p in topology["peers"]:
            pid, port = p["id"], p["port"]
            online = await _check(port)
            results.append((pid, port, online))
        return results

    loop = asyncio.new_event_loop()
    statuses = loop.run_until_complete(_scan())
    loop.close()

    for pid, port, online in statuses:
        if not online:
            try:
                subprocess.Popen(
                    [sys.executable, "node.py", str(pid)],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                revived.append(pid)
            except Exception as e:
                print(f"Failed to revive peer {pid}: {e}")

    if revived:
        import time as _time
        _time.sleep(0.3)  # brief wait for processes to bind

    return flask.jsonify({"revived": revived, "count": len(revived)})


@app.route("/api/query", methods=["POST"])
def run_query():
    data = flask.request.get_json()
    source_id = int(data["source_id"])
    keyword = data["keyword"]
    ttl = int(data.get("ttl", 5))
    topology = _get_topology()
    source_port = topology["peers"][source_id]["port"]

    async def _run():
        await reset_all_peers()
        metrics = await send_query(source_port, keyword, ttl, timeout=3)
        return metrics

    loop = asyncio.new_event_loop()
    metrics = loop.run_until_complete(_run())
    loop.close()

    total_with_file = count_peers_with_file(_get_topology(), keyword)
    coverage_ratio = (
        metrics["matched_peers_count"] / max(total_with_file, 1)
    )
    dup_ratio = (
        metrics["duplicate_queries_dropped"]
        / max(metrics["messages_sent"], 1)
    )
    hops = metrics.get("hops", [])
    avg_hops = st.mean(hops) if hops else 0.0
    lats = metrics.get("latencies", [])
    avg_lat = st.mean(lats) if lats else 0.0

    return flask.jsonify({
        "matched_peers_count": metrics["matched_peers_count"],
        "total_peers_having_file": total_with_file,
        "coverage_ratio": round(coverage_ratio, 3),
        "messages_sent": metrics["messages_sent"],
        "duplicate_queries_dropped": metrics["duplicate_queries_dropped"],
        "duplicate_ratio": round(dup_ratio, 3),
        "queryhit_count": metrics.get("queryhit_count", 0),
        "avg_hops": round(avg_hops, 2),
        "avg_latency_ms": round(avg_lat, 1),
        "failed_forward_count": metrics.get("failed_forward_count", 0),
        "dead_neighbors_detected": metrics.get("dead_neighbors_detected", 0),
        "source_id": source_id,
        "keyword": keyword,
        "ttl": ttl,
    })


# =====================================================================
# SSE / Background Analysis
# =====================================================================

def _emit(event, data=None):
    msg = {"event": event, "data": data}
    for q in _event_queues[:]:
        try:
            q.put_nowait(msg)
        except queue.Full:
            pass


def _run_analysis_background():
    global _analysis_stats, _analysis_charts, _analysis_running

    try:
        _emit("log", "Restarting all peers with fresh topology...")
        asyncio.run(restart_all_peers())

        _emit("log", "Cleaning old output...")
        os.makedirs("results", exist_ok=True)
        for f in os.listdir("results"):
            if f.endswith((".csv", ".png", ".txt")):
                os.remove(os.path.join("results", f))

        _emit("log", "Loading topology...")
        topology = _get_topology()

        _emit("log", "Verifying all 100 peers are alive...")
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        ping_results = loop.run_until_complete(asyncio.gather(*[
            send_message(p, {"type": "PING"}, connect_timeout=1.0)
            for p in ALL_PORTS
        ]))
        loop.close()
        alive = sum(1 for r in ping_results if r)
        if alive < len(ALL_PORTS):
            _emit("log",
                f"WARNING: only {alive}/{len(ALL_PORTS)} peers alive - restarting"
            )
            asyncio.run(restart_all_peers())
        else:
            _emit("log", f"All {alive}/{len(ALL_PORTS)} peers OK")

        ttl_values = TTLS
        n_runs = RUNS_PER_TTL
        all_results = []

        # Pre-generate test cases so each case runs all TTLs with same source/keyword
        random.seed(42)
        pool = get_file_pool(topology)
        valid_sources = [i for i in range(100) if i not in BLOCKED_PEERS]
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

        # Single event loop for the entire experiment phase - avoids
        # ProactorEventLoop resource exhaustion from creating/destroying
        # 30 loops (10 cases x 3 TTLs) on Windows.
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            for case_idx, case in enumerate(test_cases):
                src = case["source_id"]
                kw = case["keyword"]
                twf = case["total_with_file"]
                _emit("log",
                    f"CASE {case_idx+1}/{n_runs} | "
                    f"source={src} | keyword='{kw}' | "
                    f"total_peers_having_file={twf}")

                for ttl in ttl_values:
                    alive = loop.run_until_complete(reset_all_peers())
                    if len(alive) < 90:
                        _emit("log",
                            f"WARNING: only {len(alive)}/100 peers "
                            f"alive after RESET - results may be incomplete"
                        )
                    metrics = loop.run_until_complete(
                        send_query(case["source_port"], kw, ttl,
                                   timeout=5 if ttl <= 5 else 12)
                    )

                    matched = metrics["matched_peers_count"]
                    coverage_ratio = matched / max(twf, 1)

                    if matched > twf:
                        raise AssertionError(
                            f"BUG: matched={matched} > total={twf} "
                            f"(keyword='{kw}', ttl={ttl})"
                        )

                    messages_sent = metrics["messages_sent"]
                    duplicate_queries_dropped = metrics["duplicate_queries_dropped"]
                    dup_ratio = duplicate_queries_dropped / max(messages_sent, 1)
                    hops = metrics.get("hops", [])
                    avg_hops = st.mean(hops) if hops else 0.0
                    lats = metrics.get("latencies", [])
                    avg_lat = st.mean(lats) if lats else 0.0

                    result = {
                        "ttl": ttl,
                        "run": case_idx,
                        "keyword": kw,
                        "origin_port": case["source_port"],
                        "matched_peers_count": matched,
                        "total_peers_having_file": twf,
                        "coverage_ratio": coverage_ratio,
                        "messages_sent": messages_sent,
                        "duplicate_queries_dropped": duplicate_queries_dropped,
                        "duplicate_ratio": dup_ratio,
                        "avg_hops": avg_hops,
                        "avg_latency_ms": avg_lat,
                        "failed_forward_count": metrics.get("failed_forward_count", 0),
                        "dead_neighbors_detected": metrics.get("dead_neighbors_detected", 0),
                        "overhead_per_match": (
                            messages_sent / max(matched, 1)
                        ),
                    }
                    all_results.append(result)

                    _emit("log",
                        f"  TTL={ttl} "
                        f"matched={matched}/{twf} cov={coverage_ratio:.2f} "
                        f"sent={messages_sent} "
                        f"hops={avg_hops:.1f} lat={avg_lat:.1f}ms"
                    )

                    # Drain between TTL iterations: let the network settle
                    # before RESET + next query (avoids stale-state bleed).
                    if ttl != ttl_values[-1]:
                        loop.run_until_complete(asyncio.sleep(1.0))
        finally:
            loop.close()

        stats = compute_statistics(all_results)
        with _analysis_lock:
            _analysis_stats = stats

        _emit("stats", _serialize_stats(stats))

        # Charts
        try:
            import matplotlib
            matplotlib.use("Agg")
            import matplotlib.pyplot as plt

            from analysis import (
                plot_coverage_vs_overhead,
                plot_ttl_metrics,
                plot_duplicate_ratio,
                plot_failure_case,
                save_topology_graph,
                save_query_propagation_graph,
                save_topology_stats,
                save_csv,
                save_summary_csv,
                run_failure_experiment,
            )

            plot_coverage_vs_overhead(stats)
            _emit("log", "Saved: chart_coverage_vs_overhead.png")

            plot_ttl_metrics(stats)
            _emit("log", "Saved: chart_ttl_metrics.png")

            plot_duplicate_ratio(stats)
            _emit("log", "Saved: chart_duplicate_ratio.png")

            save_topology_graph(topology)
            save_topology_stats(topology)

            # Query propagation: use first test case for all TTLs
            first_case_results = [r for r in all_results if r["run"] == 0]
            if first_case_results:
                sample = first_case_results[0]
                sample_origin_id = peer_id_from_port(topology, sample["origin_port"])
                sample_keyword = sample["keyword"]
                for ttl in ttl_values:
                    save_query_propagation_graph(
                        topology,
                        sample_origin_id,
                        sample_keyword,
                        ttl,
                        str(ttl)
                    )

            # Save CSV
            save_csv(all_results)
            save_summary_csv(stats)

            charts = [
                "chart_coverage_vs_overhead.png",
                "chart_ttl_metrics.png",
                "chart_duplicate_ratio.png",
                "topology_graph.png",
            ]
            for ttl in ttl_values:
                charts.append(f"query_path_ttl_{ttl}.png")

            # Only emit charts that actually exist on disk
            charts = [name for name in charts if os.path.exists(f"results/{name}")]
            expected_main = [
                "chart_coverage_vs_overhead.png",
                "chart_ttl_metrics.png",
                "chart_duplicate_ratio.png",
                "topology_graph.png",
            ]
            missing = [n for n in expected_main if n not in charts]
            if missing:
                _emit("log", f"Chart files missing: {', '.join(missing)}")

            _emit("charts", charts)

        except ImportError as e:
            _emit("log", f"Charts skipped: {e}")

        _emit("done", None)

    except Exception as e:
        _emit("log", f"ERROR: {e}")
        import traceback
        _emit("log", traceback.format_exc())
        _emit("done", None)

    finally:
        _analysis_running = False


def _serialize_stats(stats):
    """Convert stats to JSON-serializable plain dicts."""
    out = {}
    for ttl, s in stats.items():
        entry = {}
        for k, v in s.items():
            try:
                entry[k] = float(v)
            except (TypeError, ValueError):
                entry[k] = v
        out[str(ttl)] = entry
    return out


def _scan_chart_files():
    """Return list of chart filenames that exist in results/."""
    if not os.path.isdir("results"):
        return []
    standard = [
        "chart_coverage_vs_overhead.png",
        "chart_ttl_metrics.png",
        "chart_duplicate_ratio.png",
        "topology_graph.png",
    ]
    charts = [name for name in standard if os.path.exists(f"results/{name}")]
    for name in sorted(os.listdir("results")):
        if name.startswith("query_path_ttl_") and name.endswith(".png"):
            charts.append(name)
    if os.path.exists("results/chart_failure_case.png"):
        charts.append("chart_failure_case.png")
    return charts


@app.route("/api/analysis/run", methods=["POST"])
def start_analysis():
    global _analysis_running
    if _analysis_running:
        return flask.jsonify({"status": "already_running"}), 429
    _analysis_running = True
    thread = threading.Thread(target=_run_analysis_background, daemon=True)
    thread.start()
    return flask.jsonify({"status": "started"})


@app.route("/api/analysis/results")
def analysis_results():
    with _analysis_lock:
        if _analysis_stats is None:
            charts = _scan_chart_files()
            if charts:
                return flask.jsonify({"stats": None, "charts": charts})
            return flask.jsonify(None)
        return flask.jsonify({
            "stats": _serialize_stats(_analysis_stats),
            "charts": _scan_chart_files(),
        })


@app.route("/api/events")
def events():
    q = queue.Queue(maxsize=100)
    _event_queues.append(q)

    def generate():
        try:
            yield "event: ready\ndata: {}\n\n"
            while True:
                msg = q.get()
                yield f"event: {msg['event']}\n"
                yield f"data: {json.dumps(msg.get('data'))}\n"
                yield "\n"
                if msg["event"] == "done":
                    break
        finally:
            if q in _event_queues:
                _event_queues.remove(q)

    return flask.Response(
        generate(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@app.route("/api/failure/run", methods=["POST"])
def run_failure():
    topology = _get_topology()

    async def _run():
        from analysis import run_failure_experiment, plot_failure_case, save_failure_report
        results = await run_failure_experiment(topology)
        chart_error = None
        chart_generated = False
        chart_path = os.path.join("results", "chart_failure_case.png")
        if os.path.exists(chart_path):
            os.remove(chart_path)
        if not results:
            return {
                "ok": False,
                "error": "Failure experiment returned no results.",
                "results": [],
                "chart_generated": False,
                "chart_error": None,
            }
        try:
            plot_failure_case(results)
            chart_generated = os.path.exists(chart_path)
        except Exception as e:
            chart_error = f"Chart generation failed: {e}"
            print(chart_error)
        save_failure_report(results)
        return {
            "ok": True,
            "results": results,
            "chart_generated": chart_generated,
            "chart_error": chart_error,
        }

    loop = asyncio.new_event_loop()
    try:
        payload = loop.run_until_complete(_run())
    except Exception as e:
        payload = {
            "ok": False,
            "error": str(e),
            "traceback": traceback.format_exc(),
            "results": [],
            "chart_generated": False,
        }
        return flask.jsonify(payload), 500
    finally:
        loop.close()

    status = 200 if payload.get("ok") else 500
    return flask.jsonify(payload), status


@app.route("/api/charts/<name>")
def chart(name):
    return flask.send_from_directory("results", name)


if __name__ == "__main__":
    os.makedirs("results", exist_ok=True)
    print("Dashboard: http://localhost:5500")
    app.run(debug=True, host="0.0.0.0", port=5500, threaded=True)
