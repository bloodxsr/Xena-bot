# 1. Use Node base
FROM node:22-bookworm

# 2. Install Rust and System dependencies
RUN apt-get update && apt-get install -y \
    curl \
    build-essential \
    pkg-config \
    libavcodec-dev \
    libavutil-dev \
    libswscale-dev \
    libswresample-dev \
    ffmpeg \
    && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

# 3. Add Rust to the path
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /usr/src/app

# 4. Install Node dependencies (Caches this layer)
COPY package*.json ./
RUN npm install

# 5. Copy all project files
COPY . .

# 6. CRITICAL STEP: Build the Rust binary exactly where the script expects it
# This creates the file at: ./raid_ml_sidecar/target/release/raid-ml-sidecar
RUN cargo build --manifest-path raid_ml_sidecar/Cargo.toml --release

# 7. Environment Setup
ENV RAID_ML_HOST=0.0.0.0
ENV RAID_ML_PORT=8787
ENV PORT=10000

# 8. Start the parent process manager
# This script will now find the binary because Step 6 created it.
CMD ["node", "scripts/run-rust-stack.js"]