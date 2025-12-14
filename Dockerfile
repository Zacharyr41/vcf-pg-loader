FROM python:3.12-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libcurl4-openssl-dev \
    zlib1g-dev \
    libbz2-dev \
    liblzma-dev \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src/ src/

RUN uv venv /opt/venv
ENV VIRTUAL_ENV=/opt/venv
ENV PATH="/opt/venv/bin:$PATH"

RUN uv pip install .

FROM python:3.12-slim

LABEL org.opencontainers.image.title="vcf-pg-loader"
LABEL org.opencontainers.image.description="High-performance VCF to PostgreSQL loader with clinical-grade compliance"
LABEL org.opencontainers.image.url="https://github.com/Zacharyr41/vcf-pg-loader"
LABEL org.opencontainers.image.source="https://github.com/Zacharyr41/vcf-pg-loader"
LABEL org.opencontainers.image.version="0.4.0"
LABEL org.opencontainers.image.licenses="MIT"
LABEL org.opencontainers.image.authors="Zachary Rothstein"

RUN apt-get update && apt-get install -y --no-install-recommends \
    libcurl4 \
    zlib1g \
    libbz2-1.0 \
    liblzma5 \
    procps \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /opt/venv /opt/venv

ENV VIRTUAL_ENV=/opt/venv
ENV PATH="/opt/venv/bin:$PATH"

WORKDIR /work

CMD ["vcf-pg-loader", "--help"]
