# ─────────────────────────────────────────────────────────────────
#  Stage 1 — Build
#
#  Mac M4 + Jetson are both linux/arm64, so this single Dockerfile
#  works natively on both with no cross-compilation or emulation.
#
#  Build on Mac M4 (Minikube also runs arm64 on Apple Silicon):
#    eval $(minikube docker-env)
#    docker build -t cep-engine-rust:1.0.0 .
#
#  Copy to Jetson (k3s):
#    docker save cep-engine-rust:1.0.0 | ssh jetson docker load
#  or push to a registry and pull on the Jetson.
# ─────────────────────────────────────────────────────────────────
FROM rust:1.85-slim AS builder

# Install musl toolchain for a fully-static binary (no glibc dependency)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        musl-tools \
        pkg-config \
        libssl-dev \
    && rustup target add aarch64-unknown-linux-musl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# ── Dependency cache layer ────────────────────────────────────────
# Copy only the manifest first so Docker can cache the crate
# downloads separately from source changes.
COPY Cargo.toml Cargo.lock ./

# Create a dummy main so cargo can resolve + download dependencies
RUN mkdir src && echo "fn main() {}" > src/main.rs && \
    cargo build --release --target aarch64-unknown-linux-musl && \
    rm -rf src

# ── Full build ────────────────────────────────────────────────────
COPY src ./src
COPY rules.yaml ./rules.yaml

# Touch main.rs so cargo knows the source changed
RUN touch src/main.rs && \
    cargo build --release --target aarch64-unknown-linux-musl

# ─────────────────────────────────────────────────────────────────
#  Stage 2 — Runtime
#  scratch = zero OS overhead; only the static binary.
#  Final image size: ~8–15 MB  (vs ~800 MB for Python)
#  RAM at runtime:  ~2–4 MB    (vs ~120 MB for Python)
# ─────────────────────────────────────────────────────────────────
FROM scratch

COPY --from=builder \
    /app/target/aarch64-unknown-linux-musl/release/cep-engine \
    /usr/local/bin/cep-engine

# Bundle the default rules file — can be overridden by mounting a ConfigMap
# at /etc/cep-engine/rules.yaml and setting RULES_FILE accordingly.
COPY --from=builder /app/rules.yaml /etc/cep-engine/rules.yaml

# ── Environment defaults (overridable via k8s env / ConfigMap) ────
ENV MQTT_HOST=mqtt-broker \
    MQTT_PORT=1883 \
    SITE_ID=SUBSTATION-ABU-01 \
    RULES_FILE=/etc/cep-engine/rules.yaml \
    RUST_LOG=info

ENTRYPOINT ["/usr/local/bin/cep-engine"]
