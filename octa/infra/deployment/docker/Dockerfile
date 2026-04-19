FROM python:3.11-slim

WORKDIR /app

COPY config/requirements-runtime.txt .

RUN pip install --upgrade pip && pip install -r requirements-runtime.txt

COPY . .

ENTRYPOINT ["python3", "-m", "scripts.demo_train_and_save"]
