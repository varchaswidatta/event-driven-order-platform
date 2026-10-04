# Multi-stage Dockerfile for Stock Service
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
RUN pnpm --filter stock-service run build

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
RUN pnpm --filter stock-service install --prod --frozen-lockfile

# Copy compiled JavaScript output, SQL migrations, and proto contracts
COPY --from=builder /app/apps/stock-service/dist ./apps/stock-service/dist
COPY apps/stock-service/src/db/migrations ./apps/stock-service/src/db/migrations
COPY apps/stock-service/src/db/migrations ./apps/stock-service/dist/db/migrations
COPY proto ./proto
COPY proto ./apps/stock-service/proto

WORKDIR /app/apps/stock-service

EXPOSE 50051

CMD ["node", "dist/index.js"]
