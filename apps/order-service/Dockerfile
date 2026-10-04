# Multi-stage Dockerfile for Order Service
# Build stage
FROM node:20-alpine AS builder

WORKDIR /app

RUN npm install -g pnpm@12

# Copy manifests and source code
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml tsconfig.json ./
COPY proto ./proto
COPY apps/api-gateway ./apps/api-gateway
COPY apps/order-service ./apps/order-service
COPY apps/inventory-service ./apps/inventory-service
COPY apps/stock-service ./apps/stock-service

# Install dependencies across workspace
RUN pnpm install --frozen-lockfile

# Build the TypeScript application
RUN pnpm --filter order-service run build

# Runtime stage
FROM node:20-alpine AS runner

WORKDIR /app

ENV NODE_ENV=production

RUN npm install -g pnpm@12

# Copy workspace manifests
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
COPY apps/api-gateway/package.json ./apps/api-gateway/
COPY apps/order-service/package.json ./apps/order-service/
COPY apps/inventory-service/package.json ./apps/inventory-service/
COPY apps/stock-service/package.json ./apps/stock-service/

# Install production dependencies only
RUN pnpm --filter order-service install --prod --frozen-lockfile

# Copy compiled JavaScript output and SQL migrations
COPY --from=builder /app/apps/order-service/dist ./apps/order-service/dist
COPY apps/order-service/src/db/migrations ./apps/order-service/src/db/migrations
COPY apps/order-service/src/db/migrations ./apps/order-service/dist/db/migrations

WORKDIR /app/apps/order-service

EXPOSE 4001

CMD ["node", "-e", "import('./dist/index.js').then(m => m.startHttpServer())"]
