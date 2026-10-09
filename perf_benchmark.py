import time
import math
from concurrent.futures import ThreadPoolExecutor
import grpc

import counter_pb2
import counter_pb2_grpc
from server import CounterServicer
from client import ReplicatedCounterClient

def run_benchmark(addrs, num_clients, requests_per_client):
    latencies = []

    def worker():
        c = ReplicatedCounterClient(addrs)
        local_lats = []
        for _ in range(requests_per_client):
            t0 = time.perf_counter()
            c.incr("bench_counter", 1)
            t1 = time.perf_counter()
            local_lats.append((t1 - t0) * 1000.0)  # ms
        return local_lats

    with ThreadPoolExecutor(max_workers=num_clients) as ex:
        futures = [ex.submit(worker) for _ in range(num_clients)]
        for f in futures:
            latencies.extend(f.result())

    latencies.sort()
    n = len(latencies)
    median = latencies[n // 2]
    p95 = latencies[math.ceil(0.95 * n) - 1]
    return median, p95, n

def main():
    # Запуск 3 реплик
    servers = []
    addrs = []
    for name in ["A", "B", "C"]:
        srv = grpc.server(ThreadPoolExecutor(max_workers=16))
        svc = CounterServicer(node_id=f"perf-{name}")
        counter_pb2_grpc.add_CounterServicer_to_server(svc, srv)
        port = srv.add_insecure_port("127.0.0.1:0")
        srv.start()
        servers.append(srv)
        addrs.append(f"127.0.0.1:{port}")

    single_addr = [addrs[0]]

    print("Running performance benchmark...")
    configs = [
        ("Single replica, 1 client", single_addr, 1, 2000),
        ("Single replica, 16 clients", single_addr, 16, 125),
        ("Quorum (3 replicas), 1 client", addrs, 1, 2000),
        ("Quorum (3 replicas), 16 clients", addrs, 16, 125),
    ]

    print(f"{'Configuration':<32} | {'Median latency (ms)':<20} | {'p95 latency (ms)':<18} | {'Requests':<8}")
    print("-" * 83)

    for name, target_addrs, clients, reqs_per_client in configs:
        med, p95, reqs = run_benchmark(target_addrs, clients, reqs_per_client)
        print(f"{name:<32} | {med:<20.2f} | {p95:<18.2f} | {reqs:<8}")

    for srv in servers:
        srv.stop(0)

if __name__ == "__main__":
    main()