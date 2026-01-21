# Yi-Rang MQ Docker Image
# Multi-stage build for minimal final image

# ============================================================
# Stage 1: Build
# ============================================================
FROM ubuntu:24.04 AS builder

# Prevent interactive prompts
ENV DEBIAN_FRONTEND=noninteractive

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    ninja-build \
    git \
    curl \
    zip \
    unzip \
    tar \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Install vcpkg
WORKDIR /opt
RUN git clone https://github.com/microsoft/vcpkg.git && \
    ./vcpkg/bootstrap-vcpkg.sh -disableMetrics

ENV VCPKG_ROOT=/opt/vcpkg

# Copy source code
WORKDIR /app
COPY . .

# Install dependencies and build
RUN cmake -B build -S . \
    -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_TOOLCHAIN_FILE=/opt/vcpkg/scripts/buildsystems/vcpkg.cmake \
    && cmake --build build -j $(nproc)

# ============================================================
# Stage 2: Runtime
# ============================================================
FROM ubuntu:24.04 AS runtime

# Install runtime dependencies only
RUN apt-get update && apt-get install -y \
    libsqlite3-0 \
    liblz4-1 \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -s /bin/bash yirangmq

# Create directories
RUN mkdir -p /app/data /app/logs /app/ipc/requests /app/ipc/responses && \
    chown -R yirangmq:yirangmq /app

WORKDIR /app

# Copy built executables and config files
COPY --from=builder /app/build/out/MainMQ /app/
COPY --from=builder /app/build/out/yirangmq-cli /app/
COPY --from=builder /app/build/out/*.json /app/
COPY --from=builder /app/MainMQ/sqlite_schema.sql /app/

# Set ownership
RUN chown -R yirangmq:yirangmq /app

USER yirangmq

# Expose IPC directories as volumes
VOLUME ["/app/data", "/app/logs", "/app/ipc"]

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD /app/yirangmq-cli health --timeout 5 || exit 1

# Default command
CMD ["/app/MainMQ"]
