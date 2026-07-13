FROM node:22-bookworm-slim AS deps
WORKDIR /repo
RUN corepack enable
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml turbo.json ./
COPY apps/tether/package.json ./apps/tether/package.json
COPY packages/client-bridge/package.json ./packages/client-bridge/package.json
COPY packages/client/package.json ./packages/client/package.json
COPY packages/protocol/package.json ./packages/protocol/package.json
RUN pnpm install --frozen-lockfile --ignore-scripts

FROM node:22-bookworm-slim AS build
WORKDIR /repo
RUN corepack enable
COPY --from=deps /repo/node_modules ./node_modules
COPY --from=deps /repo/apps/tether/node_modules ./apps/tether/node_modules
COPY --from=deps /repo/packages/client-bridge/node_modules ./packages/client-bridge/node_modules
COPY --from=deps /repo/packages/client/node_modules ./packages/client/node_modules
COPY --from=deps /repo/packages/protocol/node_modules ./packages/protocol/node_modules
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml turbo.json ./
COPY tooling ./tooling
COPY packages/client-bridge ./packages/client-bridge
COPY packages/client ./packages/client
COPY packages/protocol ./packages/protocol
COPY apps/tether ./apps/tether
RUN pnpm --filter @dungle-scrubs/tether-protocol build && pnpm --filter @dungle-scrubs/tether-client build && pnpm --filter @dungle-scrubs/tether-client-bridge build && pnpm --filter tether build

FROM node:22-bookworm-slim AS runtime
WORKDIR /repo
ENV NODE_ENV=production
RUN corepack enable
COPY --from=build /repo ./
EXPOSE 3025
CMD ["pnpm", "--filter", "tether", "start"]
