# Build stage - not shipped in the final image.
FROM node:20-alpine AS build
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
RUN npm run build

# Runtime stage - small, static-file-only image. Backend hostnames are never baked in here;
# they are substituted into nginx.conf.template from environment variables at container start
# (see docker-entrypoint.d/20-envsubst-on-templates.sh, built into the base nginx image).
FROM nginx:1.27-alpine
RUN rm -f /etc/nginx/conf.d/default.conf
COPY nginx.conf.template /etc/nginx/templates/default.conf.template
COPY --from=build /app/dist /usr/share/nginx/html

ENV DEVICE_REST_UPSTREAM=device-rest:80 \
    ALERTING_REST_UPSTREAM=alerting-rest:80 \
    PORT=8080

EXPOSE 8080

RUN addgroup -g 1000 pathlens && adduser -D -u 1000 -G pathlens pathlens \
    && chown -R pathlens:pathlens /var/cache/nginx /etc/nginx/conf.d /usr/share/nginx/html \
    && touch /var/run/nginx.pid && chown pathlens:pathlens /var/run/nginx.pid
USER pathlens

HEALTHCHECK --interval=30s --timeout=3s CMD wget -q -O- http://127.0.0.1:8080/ || exit 1
