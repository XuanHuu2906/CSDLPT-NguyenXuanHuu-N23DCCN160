#!/bin/bash
set -e

echo "=== Generating topology ==="
python bootstrap.py

echo "=== Starting 100 peers ==="
mkdir -p results
for i in $(seq 0 99); do
    python -u node.py $i &
done
echo "Waiting for peers to initialize..."
sleep 4

echo "=== Starting dashboard ==="
echo "Open http://localhost:5500 in your browser"
echo "Press Ctrl+C to stop"

# Trap để dọn peer khi thoát
cleanup() {
    echo ""
    echo "=== Cleaning up peers ==="
    pkill -f "python node.py" 2>/dev/null || true
    wait 2>/dev/null || true
    echo "=== Stopped ==="
}
trap cleanup EXIT

python -u app.py
