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

# Install dependencies and build (with tests)
RUN cmake -B build -S . \
    -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_TOOLCHAIN_FILE=/opt/vcpkg/scripts/buildsystems/vcpkg.cmake \
    -DBUILD_TESTS=ON \
    && cmake --build build -j $(nproc)

# ============================================================
# Stage 2: Test Runner
# ============================================================
FROM builder AS test-runner

WORKDIR /app

# ctest runs when the container runs, not while the image builds: a build-time `RUN ctest` layer is
# a cache hit on an unchanged tree, so the gate reported PASS without executing a single test.
# --no-tests=error because ctest otherwise exits 0 when nothing matched. (D-56)
ENTRYPOINT ["ctest", "--test-dir", "build", "--output-on-failure", "--no-tests=error"]

# ============================================================
# Stage 3: Integration Test Environment
# ============================================================
FROM ubuntu:24.04 AS integration-test

RUN apt-get update && apt-get install -y \
    libsqlite3-0 \
    liblz4-1 \
    jq \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -m -s /bin/bash yirangmq

RUN mkdir -p /app/data /app/logs /app/ipc/requests /app/ipc/responses && \
    chown -R yirangmq:yirangmq /app

WORKDIR /app

# Copy daemon, CLI tools, AND test executables
COPY --from=builder /app/build/out/ /app/
COPY --from=builder /app/MainMQ/sqlite_schema.sql /app/

# Disable FolderWatcher for Docker
RUN sed -i 's/"useFolderWatcher": true/"useFolderWatcher": false/' /app/main_mq_configuration.json

# Copy integration test script
COPY docker/integration-test.sh /app/integration-test.sh
RUN chmod +x /app/integration-test.sh

RUN chown -R yirangmq:yirangmq /app

USER yirangmq

ENTRYPOINT ["/app/integration-test.sh"]

# ============================================================
# Stage 4: Runtime (Production)
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
COPY --from=builder /app/build/out/yirangmq-cli-publisher /app/
COPY --from=builder /app/build/out/yirangmq-cli-consumer /app/
# 시연용 데모 프로세스. integration-test 스테이지는 :75 에서 out 디렉터리를 통째로 복사하므로
# 이 줄이 없으면 테스트 이미지에는 있고 runtime 이미지에는 없는 비대칭이 조용히 남는다.
COPY --from=builder /app/build/out/yirangmq-demo-* /app/
COPY --from=builder /app/build/out/run-demo.sh /app/
COPY --from=builder /app/build/out/*.json /app/
COPY --from=builder /app/MainMQ/sqlite_schema.sql /app/

# Disable FolderWatcher for Docker (volume mounts don't propagate fs events)
RUN sed -i 's/"useFolderWatcher": true/"useFolderWatcher": false/' /app/main_mq_configuration.json

# Set ownership
RUN chown -R yirangmq:yirangmq /app

USER yirangmq

# Expose IPC directories as volumes
VOLUME ["/app/data", "/app/logs", "/app/ipc"]

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD /app/yirangmq-cli-publisher health --timeout 5000 || exit 1

# Default command
CMD ["/app/MainMQ"]
