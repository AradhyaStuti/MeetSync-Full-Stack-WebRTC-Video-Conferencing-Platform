# Build stage — needs Python + build-essential because mediasoup compiles
# a C++ worker during `npm install`.
FROM node:20-slim AS deps

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY package*.json ./
RUN npm ci --omit=dev

# Runtime stage — the compiled mediasoup worker binary is already in
# node_modules, so we don't need Python or compilers here.
FROM node:20-slim AS runtime

WORKDIR /app

COPY --from=deps /app/node_modules ./node_modules
COPY . .

# HTTP API + Socket.IO
EXPOSE 8000
# mediasoup RTC ports (range configured in src/sfu/config.js)
EXPOSE 10000-10100/udp
EXPOSE 10000-10100/tcp

ENV NODE_ENV=production

CMD ["node", "src/app.js"]
