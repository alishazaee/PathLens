# PathLens Helm charts

```bash
helm dependency build charts/pathlens-infra

helm upgrade --install pathlens-infra ./charts/pathlens-infra -n pathlens-dev --create-namespace \
  -f charts/pathlens-infra/values.yaml -f charts/pathlens-infra/values-dev.yaml \
  --set postgresql.auth.password="$DB_PASSWORD" \
  --set postgresql.auth.postgresPassword="$DB_ADMIN_PASSWORD"
```

That release creates:

| Endpoint                        | Contents                                            |
|---------------------------------|-----------------------------------------------------|
| `pathlens-infra-postgresql:5432`| databases `device_db` and `alert_db`, role `pathlens`|
| `pathlens-infra-kafka:9092`     | topics `raw-log-source`, `destination`, `trash`, `alerting-target-log` |

Every service chart defaults to exactly those endpoints, so the remaining installs need no
connection configuration - only the password:

```bash
for c in device-rest alerting-rest alerting-evaluator processor simulator frontend; do
  helm upgrade --install "$c" "./charts/$c" -n pathlens-dev \
    -f "charts/$c/values.yaml" -f "charts/$c/values-dev.yaml" \
    --set database.password="$DB_PASSWORD" --set config.postgresConfig.password="$DB_PASSWORD"
done
```

### Using a managed Kafka or PostgreSQL instead

Turn the relevant subchart off and point the services at the managed endpoint:

```bash
helm upgrade --install infrastructure ./charts/infrastructure --set postgresql.enabled=false ...

helm upgrade --install alerting-rest ./charts/alerting-rest \
  --set database.host=my-postgres.rds.example.com ...
helm upgrade --install alerting-evaluator ./charts/alerting-evaluator \
  --set config.postgresConfig.url="jdbc:postgresql://my-postgres.rds.example.com:5432/alert_db" ...
```

Nothing else changes: the charts never assume the infrastructure is local, only that these
values point somewhere reachable.

### Bitnami image repositories

Bitnami moved every pinned image tag out of `docker.io/bitnami` into `docker.io/bitnamilegacy`
in August 2025, leaving only `latest` behind. `charts/pathlens-infra/values.yaml` therefore
pins the tags the subcharts reference and re-points them at `bitnamilegacy`, and sets
`global.security.allowInsecureImages: true` (the Bitnami charts otherwise refuse a
non-`bitnami` repository). If you mirror images into a private registry, override the
`registry`/`repository` fields in that file instead - the pinned tags are the ones the chart
versions in `Chart.yaml` were built against.

## Ports are declared by the chart, not by the image

The container images intentionally carry no `EXPOSE`/port metadata (see the note in
`buildSrc/src/main/groovy/docker-build.gradle`). Each chart owns its port in exactly one place:

- `device-rest`, `alerting-rest`, `frontend`: `service.targetPort` sets the container port, the
  Service target and - via the ConfigMap's `SERVER_PORT` - the port the application binds.
- `processor`, `alerting-evaluator`: `metrics.port` sets the container port, the metrics Service
  and the `prometheusPortNumber` injected into `/config/application.yml`.

Changing a port means changing one value; nothing else has to be kept in sync.

## Routing

HTTP routing is Ingress-only (`templates/ingress.yaml`, `ingress.*` values). There is
deliberately no Gateway API `HTTPRoute` alongside it - two overlapping routing paths in one
chart is a way to end up with a route that silently wins. If you migrate the platform to
Gateway API, replace `ingress.yaml` rather than adding to it.

## Environment values

`values.yaml` holds everything that is the same everywhere - image repository, endpoints,
probes, config structure. `values-<env>.yaml` holds only what genuinely differs: replica count,
image tag, resources, consumer group ids, ingress hosts, autoscaling. Passwords appear in
neither; they are supplied at install time with `--set` (see `../ansible`).
