#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
(cd frontend && npm install && npm run build)
mkdir -p backend/cmd/raftlab/webdist
find backend/cmd/raftlab/webdist -mindepth 1 -delete
cp -r frontend/dist/. backend/cmd/raftlab/webdist/
(cd backend && go test ./... && go build -o raftlab ./cmd/raftlab)
echo "built backend/raftlab — run: ./backend/raftlab -addr :8080"
