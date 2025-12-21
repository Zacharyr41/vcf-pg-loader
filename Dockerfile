ARG VERSION=dev

FROM python:3.12-slim-bookworm AS builder

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

FROM python:3.12-slim-bookworm

ARG VERSION=dev

RUN groupadd -r vcfloader && useradd -r -g vcfloader -d /home/vcfloader -m vcfloader

RUN apt-get update && apt-get install -y --no-install-recommends \
    libcurl4 \
    zlib1g \
    libbz2-1.0 \
    liblzma5 \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

COPY --from=builder /opt/venv /opt/venv

ENV VIRTUAL_ENV=/opt/venv
ENV PATH="/opt/venv/bin:$PATH"

VOLUME ["/tmp"]

WORKDIR /work
RUN chown vcfloader:vcfloader /work

USER vcfloader

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import vcf_pg_loader; print('healthy')" || exit 1

LABEL org.opencontainers.image.title="vcf-pg-loader" \
    org.opencontainers.image.description="High-performance VCF-to-PostgreSQL loader with HIPAA compliance" \
    org.opencontainers.image.version="${VERSION}" \
    org.opencontainers.image.vendor="vcf-pg-loader" \
    security.hipaa-compliant="true" \
    security.non-root="true"

CMD ["vcf-pg-loader", "--help"]
