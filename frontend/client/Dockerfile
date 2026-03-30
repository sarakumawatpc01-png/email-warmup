FROM node:20-alpine AS builder
WORKDIR /app
COPY package.json ./
RUN npm install --omit=dev
COPY src ./src
RUN npm run build

FROM nginx:alpine
COPY --from=builder /app/src/dist /usr/share/nginx/html
EXPOSE 80
