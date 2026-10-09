import argparse
import time
import threading
from concurrent import futures
import grpc

import counter_pb2
import counter_pb2_grpc
from clocks import LamportClock

class CounterServicer(counter_pb2_grpc.CounterServicer):
    def __init__(self, node_id: str, fault: str = None, delay_ms: int = 0):
        self.node_id = node_id
        self.fault = fault
        self.delay_ms = delay_ms
        self._lock = threading.Lock()
        self._values = {}  # counter_id -> int
        self._seen = {}    # idempotency_key -> (counter_id, new_value)
        self.clock = LamportClock()

    def _log_event(self, event_type: str, details: str, l_time: int):
        print(f"[{self.node_id}] {event_type} {details} L={l_time}", flush=True)

    def Increment(self, request, context):
        if self.fault == "drop-after-recv":
            # Имитация сброса соединения после получения
            context.abort(grpc.StatusCode.UNAVAILABLE, "Fault injected: drop-after-recv")

        if self.delay_ms > 0:
            time.sleep(self.delay_ms / 1000.0)

        recv_l = self.clock.update_on_receive(request.lamport_time)
        self._log_event("RECV", f"Increment(counter={request.counter_id}, delta={request.delta})", recv_l)

        with self._lock:
            if request.idempotency_key in self._seen:
                counter_id, val = self._seen[request.idempotency_key]
                reply_l = self.clock.increment()
                self._log_event("SEND", f"IncrementReply(new_value={val}, duplicate=True)", reply_l)
                return counter_pb2.IncrementReply(
                    new_value=val,
                    was_duplicate=True,
                    lamport_time=reply_l
                )

            current_val = self._values.get(request.counter_id, 0)
            new_val = current_val + request.delta
            self._values[request.counter_id] = new_val
            self._seen[request.idempotency_key] = (request.counter_id, new_val)

            apply_l = self.clock.increment()
            self._log_event("APPLY", f"counter={request.counter_id} -> {new_val}", apply_l)

            reply_l = self.clock.increment()
            self._log_event("SEND", f"IncrementReply(new_value={new_val})", reply_l)

            return counter_pb2.IncrementReply(
                new_value=new_val,
                was_duplicate=False,
                lamport_time=reply_l
            )

    def Get(self, request, context):
        recv_l = self.clock.update_on_receive(request.lamport_time)
        self._log_event("RECV", f"Get(counter={request.counter_id})", recv_l)

        with self._lock:
            found = request.counter_id in self._values
            val = self._values.get(request.counter_id, 0)

            reply_l = self.clock.increment()
            self._log_event("SEND", f"GetReply(value={val}, found={found})", reply_l)

            return counter_pb2.GetReply(
                value=val,
                found=found,
                lamport_time=reply_l
            )

def serve():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--node-id", type=str, default="replica-A")
    parser.add_argument("--fault", type=str, default=None)
    parser.add_argument("--delay-ms", type=int, default=0)
    args = parser.parse_args()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    counter_pb2_grpc.add_CounterServicer_to_server(
        CounterServicer(node_id=args.node_id, fault=args.fault, delay_ms=args.delay_ms),
        server
    )
    server.add_insecure_port(f"[::]:{args.port}")
    server.start()
    print(f"Server {args.node_id} running on port {args.port}...")
    server.wait_for_termination()

if __name__ == "__main__":
    serve()