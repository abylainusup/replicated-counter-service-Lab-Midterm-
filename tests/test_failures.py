import uuid
import pytest
import grpc
from concurrent.futures import ThreadPoolExecutor

import counter_pb2
import counter_pb2_grpc
from server import CounterServicer
from client import ReplicatedCounterClient

def test_failure_replica_crash_mid_request():
    servers, addrs = [], []
    for name in ["A", "B", "C"]:
        srv = grpc.server(ThreadPoolExecutor(max_workers=8))
        svc = CounterServicer(node_id=f"crash-replica-{name}")
        counter_pb2_grpc.add_CounterServicer_to_server(svc, srv)
        port = srv.add_insecure_port("127.0.0.1:0")
        srv.start()
        servers.append(srv)
        addrs.append(f"127.0.0.1:{port}")

    # Аварийно останавливаем реплику C до отправки
    servers[2].stop(0)

    client = ReplicatedCounterClient(addrs)
    ok, val, _ = client.incr("crash_test", 10)
    
    assert ok and val == 10  # 2 из 3 ответили -> успех

    for s in servers:
        s.stop(0)

def test_failure_request_duplication():
    srv = grpc.server(ThreadPoolExecutor(max_workers=8))
    svc = CounterServicer(node_id="dup-replica")
    counter_pb2_grpc.add_CounterServicer_to_server(svc, srv)
    port = srv.add_insecure_port("127.0.0.1:0")
    srv.start()

    addr = f"127.0.0.1:{port}"
    client = ReplicatedCounterClient([addr])
    k = str(uuid.uuid4())

    ok1, v1, dup1 = client.incr("dup_cnt", 3, idempotency_key=k)
    ok2, v2, dup2 = client.incr("dup_cnt", 3, idempotency_key=k)

    assert ok1 and v1 == 3 and not dup1
    assert ok2 and v2 == 3 and dup2

    srv.stop(0)

def test_failure_induced_timeout_with_retry():
    srv = grpc.server(ThreadPoolExecutor(max_workers=8))
    svc = CounterServicer(node_id="timeout-replica", delay_ms=2500)
    counter_pb2_grpc.add_CounterServicer_to_server(svc, srv)
    port = srv.add_insecure_port("127.0.0.1:0")
    srv.start()

    addr = f"127.0.0.1:{port}"
    client = ReplicatedCounterClient([addr], timeout=0.3)
    k = str(uuid.uuid4())

    # Вызов задерживается и не подтверждается
    ok1, _, _ = client.incr("timeout_cnt", 5, idempotency_key=k)
    assert not ok1

    # Реплика восстанавливает нормальную скорость
    svc.delay_ms = 0
    ok2, v2, _ = client.incr("timeout_cnt", 5, idempotency_key=k)
    assert ok2 and v2 == 5

    srv.stop(0)
    