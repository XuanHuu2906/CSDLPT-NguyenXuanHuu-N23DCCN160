import json
import random

FILE_POOL = [
    "music.mp3", "video.mp4", "photo.jpg", "doc.pdf",   "game.exe",
    "data.csv",  "book.epub", "code.zip",  "audio.wav",  "image.png",
    "report.pdf","notes.txt", "backup.zip","movie.mp4",  "song.mp3",
    "chart.xlsx","slides.pptx","script.py","readme.md",  "config.json"
]

def generate_peers(n=100):
    return [{"id": i, "port": 6000 + i} for i in range(n)]

def generate_topology(n=100, extra_edges=150):
    edges = []
    nodes = list(range(n))
    random.shuffle(nodes)
    for i in range(1, n):
        a = nodes[i]
        b = nodes[random.randint(0, i - 1)]
        edges.append([a, b])

    added = 0
    attempts = 0
    while added < extra_edges and attempts < extra_edges * 10:
        a = random.randint(0, n - 1)
        b = random.randint(0, n - 1)
        attempts += 1
        if a != b and [a, b] not in edges and [b, a] not in edges:
            edges.append([a, b])
            added += 1
    return edges

def assign_files(n=100):
    return {str(i): random.sample(FILE_POOL, 5) for i in range(n)}

def build_adjacency(n, edges):
    adj = {i: [] for i in range(n)}
    for a, b in edges:
        adj[a].append(b)
        adj[b].append(a)
    return adj

def is_connected(adj, n):
    visited = set()
    stack = [0]
    while stack:
        node = stack.pop()
        if node not in visited:
            visited.add(node)
            stack.extend(adj[node])
    return len(visited) == n

def bfs_distances(adj, src):
    dist = {src: 0}
    queue = [src]
    for node in queue:
        for neighbor in adj[node]:
            if neighbor not in dist:
                dist[neighbor] = dist[node] + 1
                queue.append(neighbor)
    return dist

def compute_metrics(adj, n, edges):
    avg_degree = sum(len(adj[i]) for i in range(n)) / n
    density = len(edges) / (n * (n - 1) / 2) * 100
    diameter = 0
    for src in range(n):
        dist = bfs_distances(adj, src)
        diameter = max(diameter, max(dist.values()))
    return {
        "avg_degree": avg_degree,
        "density": density,
        "diameter": diameter
    }

def save_topology(peers, edges, files):
    with open("topology.json", "w") as f:
        json.dump({"peers": peers, "edges": edges, "files": files}, f, indent=2)

def main():
    n = 100
    print("Generating peers...")
    peers = generate_peers(n)
    print("Generating topology...")
    edges = generate_topology(n)
    print("Assigning files...")
    files = assign_files(n)

    print("Verifying connectivity...")
    adj = build_adjacency(n, edges)
    assert is_connected(adj, n), "ERROR: Graph is disconnected!"

    metrics = compute_metrics(adj, n, edges)
    print(f"Connected:  Yes")
    print(f"Avg degree: {metrics['avg_degree']:.2f}")
    print(f"Density:    {metrics['density']:.4f}%")
    print(f"Diameter:   {metrics['diameter']} hops")

    print("Saving topology.json...")
    save_topology(peers, edges, files)
    print("Done.")

if __name__ == "__main__":
    main()
