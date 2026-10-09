# Replicated Counter Service

Реплицируемый сервис счетчиков на Python с использованием gRPC, логических часов Лампорта и кворумной записи.

## Настройка окружения

```bash
# 1. Проверка версии Python
python3 --version

# 2. Создание и активация виртуального окружения
python3 -m venv .venv
source .venv/bin/activate

# 3. Установка зависимостей
pip install grpcio grpcio-tools pytest

# 4. Компиляция Protocol Buffers
python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. counter.proto