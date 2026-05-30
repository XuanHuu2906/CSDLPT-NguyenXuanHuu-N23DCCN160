#!/bin/bash
set -e

echo "=== P2P Flooding — Full Pipeline ==="
echo ""

echo "=== 1. Generating topology ==="
python bootstrap.py

echo ""
echo "=== 2. Starting 100 peers ==="
mkdir -p results
for i in $(seq 0 99); do
    python -u node.py $i &
done
echo "Waiting for peers to initialize..."
sleep 4

echo ""
echo "=== 3. Running experiments (TTL=3,5,7) ==="
python -u analysis.py

echo ""
echo "=== 4. Running failure demo ==="
python -u failure_demo.py

echo ""
echo "=== 5. Cleanup ==="
pkill -f "python node.py" 2>/dev/null || true
wait 2>/dev/null || true

echo ""
echo "=== Done. Results: ==="
ls -la results/
