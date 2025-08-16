FROM node:22.12.0-alpine

# Install global dependencies first (rarely changes)
RUN npm install -g pnpm@latest typescript

WORKDIR /app

# Copy only the files needed for install first, to cache dependencies better
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./

RUN pnpm install --frozen-lockfile

# Then copy the rest of your source code
COPY . .


RUN echo "PUBLIC_APP_NAME=Docker App Default" > .env
RUN pnpm build

EXPOSE 3000
CMD ["pnpm", "start"]