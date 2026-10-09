import threading

class LamportClock:
    def __init__(self):
        self._value = 0
        self._lock = threading.Lock()

    def value(self) -> int:
        with self._lock:
            return self._value

    def increment(self) -> int:
        with self._lock:
            self._value += 1
            return self._value

    def update_on_receive(self, received_time: int) -> int:
        with self._lock:
            self._value = max(self._value, received_time) + 1
            return self._value