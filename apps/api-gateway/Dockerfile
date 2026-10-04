# Multi-stage Dockerfile for API Gateway
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
RUN pnpm --filter api-gateway run build

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
RUN pnpm --filter api-gateway install --prod --frozen-lockfile

# Copy compiled JavaScript output
COPY --from=builder /app/apps/api-gateway/dist ./apps/api-gateway/dist

WORKDIR /app/apps/api-gateway

EXPOSE 4000

CMD ["node", "dist/index.js"]
