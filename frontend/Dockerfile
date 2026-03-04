# ── Stage 1: build ────────────────────────────────────────────────────────────
FROM node:alpine AS build
WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm install
COPY . .
# Point the API client at the nginx proxy path (no Vite dev-server proxy in prod)
ENV VITE_API_BASE=/api
RUN npm run build

# ── Stage 2: serve ────────────────────────────────────────────────────────────
FROM nginx:alpine
COPY --from=build /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
