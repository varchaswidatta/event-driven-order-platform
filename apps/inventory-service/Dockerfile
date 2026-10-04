# Multi-stage Dockerfile for Inventory Service
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
RUN pnpm --filter inventory-service run build

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
RUN pnpm --filter inventory-service install --prod --frozen-lockfile

# Copy compiled JavaScript output, SQL migrations, and proto contracts
COPY --from=builder /app/apps/inventory-service/dist ./apps/inventory-service/dist
COPY apps/inventory-service/src/db/migrations ./apps/inventory-service/src/db/migrations
COPY apps/inventory-service/src/db/migrations ./apps/inventory-service/dist/db/migrations
COPY proto ./proto
COPY proto ./apps/inventory-service/proto

WORKDIR /app/apps/inventory-service

CMD ["node", "dist/index.js"]
