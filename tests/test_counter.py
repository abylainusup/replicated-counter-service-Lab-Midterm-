import time
import uuid
import pytest
import grpc
from concurrent.futures import ThreadPoolExecutor

import counter_pb2
import counter_pb2_grpc
from server import CounterServicer
from client import ReplicatedCounterClient

@pytest.fixture
def single_server():
    server = grpc.server(ThreadPoolExecutor(max_workers=8))
    servicer = CounterServicer(node_id="test-replica")
    counter_pb2_grpc.add_CounterServicer_to_server(servicer, server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    yield f"127.0.0.1:{port}", servicer
    server.stop(0)

@pytest.fixture
def three_replicas():
    servers = []
    addrs = []
    servicers = []
    for name in ["A", "B", "C"]:
        srv = grpc.server(ThreadPoolExecutor(max_workers=8))
        svc = CounterServicer(node_id=f"replica-{name}")
        counter_pb2_grpc.add_CounterServicer_to_server(svc, srv)
        port = srv.add_insecure_port("127.0.0.1:0")
        srv.start()
        servers.append(srv)
        addrs.append(f"127.0.0.1:{port}")
        servicers.append(svc)
    yield addrs, servers, servicers
    for srv in servers:
        srv.stop(0)

def test_increment_applies_delta(single_server):
    addr, _ = single_server
    client = ReplicatedCounterClient([addr])
    ok, val, dup = client.incr("test_cnt", 5)
    assert ok and val == 5 and not dup

def test_duplicate_key_not_reapplied(single_server):
    addr, _ = single_server
    client = ReplicatedCounterClient([addr])
    k = str(uuid.uuid4())
    ok1, val1, dup1 = client.incr("cnt_dup", 10, idempotency_key=k)
    assert ok1 and val1 == 10 and not dup1

    ok2, val2, dup2 = client.incr("cnt_dup", 10, idempotency_key=k)
    assert ok2 and val2 == 10 and dup2

def test_concurrent_increments_exact(single_server):
    addr, _ = single_server
    
    def worker():
        c = ReplicatedCounterClient([addr])
        for _ in range(1000):
            c.incr("cnt_conc", 1)

    with ThreadPoolExecutor(max_workers=2) as ex:
        f1 = ex.submit(worker)
        f2 = ex.submit(worker)
        f1.result()
        f2.result()

    client = ReplicatedCounterClient([addr])
    found, val = client.get("cnt_conc")
    assert found and val == 2000

def test_get_missing_counter(single_server):
    addr, _ = single_server
    client = ReplicatedCounterClient([addr])
    found, val = client.get("non_existent")
    assert not found and val == 0

def test_retry_after_timeout_is_safe():
    # Запускаем сервер с искусственной задержкой, чтобы клиент сначала превысил deadline
    srv = grpc.server(ThreadPoolExecutor(max_workers=8))
    svc = CounterServicer(node_id="delay-replica", delay_ms=1200)
    counter_pb2_grpc.add_CounterServicer_to_server(svc, srv)
    port = srv.add_insecure_port("127.0.0.1:0")
    srv.start()

    addr = f"127.0.0.1:{port}"
    client = ReplicatedCounterClient([addr], timeout=0.5)  # таймаут меньше задержки
    k = "retry-key-1"
    
    # 1-я попытка завершится таймаутом
    ok, val, _ = client.incr("delay_cnt", 5, idempotency_key=k)
    assert not ok

    # Убираем задержку
    svc.delay_ms = 0
    # Повторная попытка с тем же ключом
    ok2, val2, dup2 = client.incr("delay_cnt", 5, idempotency_key=k)
    assert ok2 and val2 == 5

    srv.stop(0)

def test_majority_commit_two_acks(three_replicas):
    addrs, servers, _ = three_replicas
    servers[2].stop(0)  # Останавливаем 1 реплику из 3

    client = ReplicatedCounterClient(addrs)
    ok, val, _ = client.incr("maj_cnt", 7)
    assert ok and val == 7

def test_no_commit_below_majority(three_replicas):
    addrs, servers, _ = three_replicas
    servers[1].stop(0)  # Останавливаем 2 реплики из 3
    servers[2].stop(0)

    client = ReplicatedCounterClient(addrs)
    ok, val, _ = client.incr("no_maj_cnt", 7)
    assert not ok

def test_replicas_converge(three_replicas):
    addrs, _, _ = three_replicas
    client = ReplicatedCounterClient(addrs)

    for i in range(10):
        client.incr("conv_cnt", 1)

    c1 = ReplicatedCounterClient([addrs[0]])
    c2 = ReplicatedCounterClient([addrs[1]])
    c3 = ReplicatedCounterClient([addrs[2]])

    _, v1 = c1.get("conv_cnt")
    _, v2 = c2.get("conv_cnt")
    _, v3 = c3.get("conv_cnt")

    assert v1 == 10 and v2 == 10 and v3 == 10