#!/bin/bash
docker build -f Dockerfile -t kv:$1 .

#docker run --name kv-3 -v /Users/huangda/project/talent-challenge/matrixbase/distributed/cfg/node3.toml:/etc/cfg.toml --network distributed_mynet1 --ip 172.19.1.3 -p 8083:8080 kv:x