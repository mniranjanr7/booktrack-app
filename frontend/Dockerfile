# =============Build Stage ==============
FROM node:20-alpine AS builder
WORKDIR /app
COPY package*.json ./
RUN npm ci

COPY . .
RUN npm run build

# --------RUN STAGE-----
FROM nginx:1.25-alpine

RUN rm /etc/nginx/conf.d/default.conf
COPY nginx.conf /etc/nginx/nginx.conf
COPY default.conf /etc/nginx/conf.d/default.conf

# COPY build frontend
COPY --from=builder /app/build /usr/share/nginx/html

# use non root user nginx
USER nginx
EXPOSE 8080
CMD ["nginx", "-g", "daemon off;"]