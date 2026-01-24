FROM python:3.11-slim
WORKDIR /app
COPY . /app
# install minimal system build deps required for some ML libraries
RUN apt-get update && apt-get install -y --no-install-recommends \
	build-essential \
	cmake \
	git \
	libgomp1 \
	&& rm -rf /var/lib/apt/lists/*
RUN pip install --upgrade pip && pip install -r requirements-runtime.txt
ENTRYPOINT ["python3", "-m", "scripts.demo_train_and_save"]