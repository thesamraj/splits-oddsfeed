#!/bin/bash
# Find large blobs in git history
set -euo pipefail

echo "Scanning git history for large blobs..."
echo "========================================="
echo ""

# Find top 30 largest blobs
echo "Top 30 largest objects in history:"
git rev-list --objects --all |
git cat-file --batch-check='%(objectname) %(objecttype) %(objectsize) %(rest)' |
awk '$2=="blob"{print}' | sort -k3 -n | tail -30 |
awk '{ 
    sz=$3; unit="B"; 
    if(sz>1024){sz/=1024;unit="KB"} 
    if(sz>1024){sz/=1024;unit="MB"} 
    if(sz>1024){sz/=1024;unit="GB"} 
    printf "%-12s %8.2f %s  %s\n",$1,sz,unit,$4 
}'

echo ""
echo "========================================="
echo "Checking for blobs >100MB..."
echo ""

# Flag any blobs over 100MB
LARGE_BLOBS=$(git rev-list --objects --all |
git cat-file --batch-check='%(objectname) %(objecttype) %(objectsize) %(rest)' |
awk '$2=="blob" && $3>104857600{print}' |
awk '{ 
    sz=$3/1048576; 
    printf "%-12s %8.2f MB  %s\n",$1,sz,$4 
}')

if [ -n "$LARGE_BLOBS" ]; then
    echo "❌ FOUND BLOBS >100MB:"
    echo "$LARGE_BLOBS"
    exit 1
else
    echo "✅ No blobs >100MB found"
    exit 0
fi