import argparse
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
import grpc

import counter_pb2
import counter_pb2_grpc
from clocks import LamportClock

class ReplicatedCounterClient:
    def __init__(self, replica_addresses, client_id="client-1", timeout=2.0):
        self.replica_addresses = replica_addresses
        self.client_id = client_id
        self.timeout = timeout
        self.clock = LamportClock()

    def _log(self, event_type, details, l_time):
        print(f"[{self.client_id}] {event_type} {details} L={l_time}", flush=True)

    def _send_increment_to_replica(self, addr, request):
        channel = grpc.insecure_channel(addr)
        stub = counter_pb2_grpc.CounterStub(channel)
        backoffs = [0.2, 0.4, 0.8]
        
        for attempt in range(4):
            try:
                reply = stub.Increment(request, timeout=self.timeout)
                channel.close()
                return (True, reply)
            except grpc.RpcError as e:
                if e.code() in (grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.UNAVAILABLE):
                    if attempt < len(backoffs):
                        time.sleep(backoffs[attempt])
                        continue
                channel.close()
                return (False, None)
        channel.close()
        return (False, None)

    def incr(self, counter_id: str, delta: int, idempotency_key: str = None):
        if not idempotency_key:
            idempotency_key = str(uuid.uuid4())

        send_l = self.clock.increment()
        self._log("SEND", f"Increment(counter={counter_id}, delta={delta})", send_l)

        req = counter_pb2.IncrementRequest(
            counter_id=counter_id,
            delta=delta,
            idempotency_key=idempotency_key,
            lamport_time=send_l
        )

        results = []
        with ThreadPoolExecutor(max_workers=len(self.replica_addresses)) as executor:
            futures_map = {
                executor.submit(self._send_increment_to_replica, addr, req): addr 
                for addr in self.replica_addresses
            }
            for fut in as_completed(futures_map):
                success, reply = fut.result()
                if success:
                    results.append(reply)

        # Порог большинства зависит от числа переданных реплик:
        # Для 1 реплики -> majority = 1 (1/2 + 1 = 1)
        # Для 3 реплик -> majority = 2 (3/2 + 1 = 2)
        majority_threshold = len(self.replica_addresses) // 2 + 1

        if len(results) >= majority_threshold:
            max_reply_l = max(r.lamport_time for r in results)
            recv_l = self.clock.update_on_receive(max_reply_l)
            val = results[0].new_value
            was_dup = any(r.was_duplicate for r in results)
            self._log("RECV", f"IncrementReply(new_value={val})", recv_l)
            return True, val, was_dup
        else:
            return False, -1, False

    def get(self, counter_id: str):
        send_l = self.clock.increment()
        self._log("SEND", f"Get(counter={counter_id})", send_l)

        req = counter_pb2.GetRequest(counter_id=counter_id, lamport_time=send_l)
        
        for addr in self.replica_addresses:
            try:
                channel = grpc.insecure_channel(addr)
                stub = counter_pb2_grpc.CounterStub(channel)
                reply = stub.Get(req, timeout=self.timeout)
                recv_l = self.clock.update_on_receive(reply.lamport_time)
                self._log("RECV", f"GetReply(value={reply.value}, found={reply.found})", recv_l)
                channel.close()
                return reply.found, reply.value
            except grpc.RpcError:
                continue
        return False, 0

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("cmd", choices=["incr", "get"])
    parser.add_argument("counter_id")
    parser.add_argument("--by", type=int, default=1)
    parser.add_argument("--key", type=str, default=None)
    parser.add_argument("--ports", type=str, default="50051,50052,50053")
    args = parser.parse_args()

    ports = [p.strip() for p in args.ports.split(",")]
    addrs = [f"localhost:{p}" for p in ports]
    client = ReplicatedCounterClient(addrs)

    if args.cmd == "incr":
        ok, val, dup = client.incr(args.counter_id, args.by, idempotency_key=args.key)
        if ok:
            print(f"OK committed value={val} (duplicate: {dup})")
        else:
            print("FAILED to reach quorum commit.")
    elif args.cmd == "get":
        found, val = client.get(args.counter_id)
        if found:
            print(f"value={val}")
        else:
            print("Counter not found.")