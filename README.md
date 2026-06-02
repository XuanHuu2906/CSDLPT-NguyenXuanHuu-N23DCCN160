# Mô Phỏng P2P Flooding — Tìm Kiếm Phi Cấu Trúc Kiểu Gnutella

Mô phỏng mạng peer-to-peer phân tán với 100 peers, triển khai giao thức QUERY/QUERYHIT kiểu Gnutella, khả năng chịu churn (churn resilience), và phân tích metrics toàn diện.

## Tổng Quan

Dự án mô phỏng mạng P2P phi cấu trúc (unstructured) với 100 peers độc lập, mỗi peer giữ 5 file ngẫu nhiên. Việc tìm kiếm được thực hiện qua cơ chế **flooding**: message query lan truyền qua mạng hop-by-hop đến giới hạn TTL. Kết quả tìm thấy quay về peer gốc qua **reverse-path routing**.

**Tính năng chính:**
- Mạng overlay P2P với 100 peers qua TCP
- Tìm kiếm dạng flooding với kiểm soát TTL
- Chống duplicate (QUERY + QUERYHIT)
- Reverse-path routing cho QUERYHIT
- PING heartbeat để phát hiện node chết (không gửi PONG riêng; kết nối thành công được xem là alive)
- Phát hiện và cách ly neighbor chết
- Metrics mở rộng (latency, hop count, coverage ratio)
- Đồ thị NetworkX (topology + query propagation)
- Demo failure (mô phỏng tấn công hub)
- Web dashboard (Flask + SSE)

## Kiến Trúc

```
┌─────────────────────────────────────────────────────┐
│                  100 Peers (node.py)                 │
│  ┌─────┐  ┌─────┐  ┌─────┐         ┌─────┐        │
│  │Peer0│──│Peer1│──│Peer2│──...──│Peer99│        │
│  │:6000│  │:6001│  │:6002│         │:6099│        │
│  └─────┘  └─────┘  └─────┘         └─────┘        │
│       │        │        │               │          │
│       └────────┴────────┴───────────────┘          │
│                Kết nối TCP                          │
├─────────────────────────────────────────────────────┤
│  query.py / analysis.py — Chạy thí nghiệm           │
│  failure_demo.py       — Demo khả năng chịu churn   │
│  app.py + dashboard    — Web UI (Flask, port 5500)  │
└─────────────────────────────────────────────────────┘
```

Mỗi peer là một TCP server độc lập. Toàn bộ giao tiếp dùng JSON qua TCP. Không dùng shared memory, không có index trung tâm, không có database.

## Các Loại Message

| Loại | Hướng | Mục đích |
|------|-------|----------|
| **QUERY** | Origin → Neighbors | Tìm kiếm keyword với TTL |
| **QUERYHIT** | Peer tìm thấy → Origin (reverse-path) | Phản hồi với file match |
| **PING** | Peer → Neighbor | Kiểm tra heartbeat/liveness qua kết nối TCP |
| **METRICS_REQUEST** | Analysis → Peer | Yêu cầu metrics |
| **METRICS_RESPONSE** | Peer → Analysis | Trả về metrics |
| **RESET** | Analysis → Tất cả peers | Xóa state cho thí nghiệm mới |
| **SHUTDOWN** | Analysis → Peer | Tắt peer (failure demo) |

## Cài Đặt

### Yêu cầu

- Python 3.10+
- Thư viện: Cài đặt nhanh bằng lệnh `pip install -r requirements.txt` (hoặc cài đặt thủ công: `pip install matplotlib flask networkx`)

### Sinh Topology

```bash
python bootstrap.py
```

Tạo `topology.json` với 100 peers, các cạnh (đồ thị liên thông), và phân phối file.

### Chạy Full Pipeline

```bash
bash run_all.sh
```

Script này sinh topology, khởi động 100 peers, chạy `analysis.py`, chạy `failure_demo.py`, rồi cleanup peer processes.

### Khởi Động Peers Thủ Công

```bash
for i in $(seq 0 99); do
    python -u node.py $i &
done
sleep 4
```

### Chạy Một Query Đơn

```bash
python query.py <source_id> <keyword> <ttl>
```

Ví dụ: `python query.py 5 music.mp3 5`

### Chạy Full Analysis

```bash
python analysis.py
```

Chạy thí nghiệm với TTL=3,5,7 (10 case, mỗi case chạy đủ 3 TTL), sinh CSV, biểu đồ, đồ thị topology, và failure experiment.

### Chạy Failure Demo

```bash
python failure_demo.py
```

Kill 5 high-degree non-holder nodes, so sánh search coverage trước và sau. File holders không bị kill nên coverage giảm phản ánh routing/connectivity, không phải mất dữ liệu.
Script standalone này ghi `failure_report.txt` dạng before/after và có thể ghi đè report batch từ `analysis.py`.

### Web Dashboard

```bash
bash run_dashboard.sh
# Mở http://localhost:5500
```

Dashboard gồm Network Monitor, Search Demo, TTL Analysis (Flask SSE progress log) và Failure Analysis. Failure Analysis kill high-degree non-holder hubs theo batch 5 node đến tối đa 20 node, đồng thời hiển thị `Reachable Holders`, số alive neighbors của source và source component size để kiểm chứng mạng bị phân mảnh.

## File Đầu Ra

Tất cả kết quả được lưu trong thư mục `results/`:

### Dữ Liệu CSV
| File | Mô tả |
|------|-------|
| `metrics_ttl.csv` | Metrics từng lần chạy cho tất cả thí nghiệm |
| `metrics_summary.csv` | Thống kê tóm tắt theo TTL |

### Biểu Đồ
| File | Mô tả |
|------|-------|
| `chart_coverage_vs_overhead.png` | Coverage ratio vs messages gửi (errorbar) |
| `chart_ttl_metrics.png` | 2×2 subplots: coverage, overhead, duplicate ratio, latency |
| `chart_duplicate_ratio.png` | Tỉ lệ duplicate query theo TTL |
| `chart_failure_case.png` | Suy giảm coverage theo batch hub attack (`analysis.py` hoặc dashboard Failure Analysis) |
| `failure_before_after.png` | Biểu đồ before/after của `failure_demo.py` standalone |

### Đồ Thị NetworkX
| File | Mô tả |
|------|-------|
| `topology_graph.png` | Trực quan hóa toàn bộ 100-node topology |
| `query_path_ttl_3.png` | Lan truyền query với TTL=3 |
| `query_path_ttl_5.png` | Lan truyền query với TTL=5 |
| `query_path_ttl_7.png` | Lan truyền query với TTL=7 |

### Báo Cáo
| File | Mô tả |
|------|-------|
| `topology_stats.txt` | Thống kê mạng (degree, density, diameter) |
| `failure_report.txt` | Kết quả failure. `analysis.py`/dashboard ghi batch debug; `failure_demo.py` ghi before/after standalone |

## Các Metrics

| Metric | Mô tả |
|--------|-------|
| `matched_peers_count` | Số peers duy nhất tìm thấy keyword |
| `total_peers_having_file` | Ground truth: tổng số peers thực sự có file (tính offline) |
| `coverage_ratio` | `matched / total` — tỉ lệ peers có file được tìm thấy |
| `messages_sent` | Tổng số QUERY messages được forward qua tất cả peers |
| `duplicate_queries_dropped` | Số QUERY messages bị drop bởi `seen_queries` |
| `duplicate_ratio` | `dropped / sent` — hiệu quả chống duplicate |
| `queryhit_count` | Tổng số QUERYHIT messages nhận được tại origin |
| `duplicate_queryhits_dropped` | Số QUERYHIT bị drop do dedup |
| `avg_hops` | Số hop trung bình từ origin đến peers tìm thấy file |
| `min_hops` / `max_hops` | Số hop nhỏ nhất/lớn nhất |
| `avg_latency_ms` | Trung bình latency của các QUERYHIT về origin (ms), không phải wall-clock hoàn tất toàn query |
| `failed_forward_count` | Số lần forward thất bại (đến neighbor chết) |
| `dead_neighbors_detected` | Số neighbor chết được phát hiện |
| `overhead_per_match` | `messages_sent / matched` — chi phí cho mỗi kết quả tìm thấy |
| `fallback_used` | Số QUERYHIT dùng alternate reverse route thay vì primary route |
| `holders_left` | Failure metric: số peers còn giữ keyword sau khi kill hub (phải giữ nguyên vì không kill holders) |
| `reachable_holders` | Failure metric: số keyword holders reachable từ source trong alive topology với TTL=5 |
| `source_alive_neighbors` | Failure metric: số neighbor còn sống trực tiếp của source |
| `source_component_size` | Failure metric: kích thước connected component chứa source sau khi kill hubs |

## Các Quyết Định Thiết Kế Quan Trọng

- **asyncio single-threaded**: counters không cần locks (chỉ yield tại `await`)
- **Tập `matched_peer_ports`**: ngăn đếm trùng khi nhiều file match tại một peer
- **`shutdown_event.wait()`**: tắt peer sạch sẽ hơn `serve_forever()`
- **Exact diameter**: BFS từ tất cả 100 nodes (nhanh vì đồ thị chỉ 100 nodes)
- **Thu thập metrics song song**: 100 peers × ~0.1s = ~10s mỗi thí nghiệm
- **Timeout-based termination**: origin chờ timeout cố định cho QUERYHITs, sau đó tổng hợp
- **Persistent TCP connections**: peer cache outbound connections để giảm TIME_WAIT/connection storm trên Windows
- **Failure debug bằng graph model**: `reachable_holders` được tính offline từ alive topology để đối chiếu với `matched`

## Cạm Bẫy Đã Biết

- Kill peers cũ trước khi chạy lại nếu chạy thủ công (`pkill -f "python node.py"`)
- `collect_all_metrics()` trả về 0 cho peers chết một cách im lặng — OK cho failure case
- `messages_sent` chỉ tăng khi forward QUERY thành công; failed forward được ghi vào `failed_forward_count`
- **TIME_WAIT trên Windows (~240s)**: Sau SHUTDOWN/pkill, port peer ở trạng thái TIME_WAIT không cho rebind ngay. `reuse_address=True` trong `asyncio.start_server()` giảm thiểu vấn đề này.
- **WinError 52** ("duplicate name exists on the network"): Xảy ra khi kết nối localhost TCP quá nhanh với `SO_REUSEADDR`. Đã xử lý bằng retry/backoff trong `send_message()`.
- **Stale QUERYHIT contamination**: Sau RESET, các QUERYHIT từ thí nghiệm trước còn đang trên đường TCP có thể đến origin và bị đếm sai. Đã fix bằng `current_query_id` guard: origin chỉ chấp nhận QUERYHIT thuộc query hiện tại.
- **Double-reset protocol**: `reset_all_peers()` gửi RESET hai lần với khoảng drain 0.5s để dọn sạch in-flight messages.
- **Peer-detection auto-skip**: `analysis.py` kiểm tra nếu peers đang chạy thì không restart, tránh mất công bind lại port.
- **Shutdown với persistent connections**: peer shutdown phải đóng cả outbound và inbound connections; nếu không, peer đã tắt listener vẫn có thể xử lý message qua socket cũ.
