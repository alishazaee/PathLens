# PathLens Ansible deployment

Deploys the Helm charts in `../charts` to Kubernetes. Ansible only orchestrates *which*
environment and *which* secrets are used - all non-secret configuration lives in the Helm
charts themselves (`values.yaml` + `values-<env>.yaml`), so nothing is duplicated here.

## Layout

```
inventories/{dev,test,prod}/
  hosts.yml            # a single local control node that talks to the cluster's API
  group_vars/
    all.yml             # environment + which charts to manage, in install order
    vault.yml            # ansible-vault encrypted secrets (DB passwords, ...)
roles/pathlens_helm/     # idempotent helm upgrade --install / uninstall logic
playbooks/
  deploy.yml            # first rollout (safe to re-run)
  upgrade.yml           # upgrade an existing install, optionally one service / one image tag
  undeploy.yml          # uninstall the releases (namespace is left in place)
```

## Install order

`pathlens_services` is an ordered list and `pathlens-infra` is the first entry. That release
brings up the Kafka and PostgreSQL (as Bitnami subcharts) that every other release expects at
`pathlens-infra-kafka:9092` and `pathlens-infra-postgresql:5432`; `undeploy.yml` walks the list
in reverse, so the infrastructure is removed last. Uninstalling does **not** delete the
PersistentVolumeClaims behind PostgreSQL and Kafka - remove those by hand if you really mean to
throw the data away.

Because `pathlens-infra` has chart dependencies, the helm task runs with
`dependency_update: true`, which needs registry access to `registry-1.docker.io` from wherever
the playbook runs. To deploy from a host without that access, run
`helm dependency build charts/pathlens-infra` beforehand and commit/ship the resulting
`charts/pathlens-infra/charts/*.tgz`.

If the environment already has a managed Kafka or PostgreSQL, keep `pathlens-infra` in the list
but disable the subchart(s) in `charts/pathlens-infra/values-<env>.yaml` and override the
endpoints in the service charts - see `../charts/README.md`.

## One-time setup

```bash
ansible-galaxy collection install -r requirements.yml
```

Requires `kubectl`/`helm` config pointing at the target cluster's context (see
`pathlens_kube_context` per environment) and a `kubeconfig` reachable from wherever you run
these playbooks (defaults to `~/.kube/config`, override with `-e pathlens_kubeconfig=...`).

## Secrets

Two variables must be defined in every inventory's `vault.yml`:

| Variable                                  | Used by                                          |
|-------------------------------------------|--------------------------------------------------|
| `vault_pathlens_db_password`              | pathlens-infra (creates the role), device-rest, alerting-rest, alerting-evaluator |
| `vault_pathlens_postgres_admin_password`  | pathlens-infra only - the PostgreSQL superuser    |

All three services share the single `pathlens` PostgreSQL role created by `pathlens-infra`, so
they authenticate with the same password; `all.yml` exposes it once as `pathlens_db_password`.
This replaces the earlier per-service `vault_device_rest_db_password` /
`vault_alerting_rest_db_password` / `vault_alerting_evaluator_db_password` variables - the
encrypted `test` and `prod` vaults still contain the old names and must be updated:

```bash
ansible-vault edit inventories/test/group_vars/vault.yml
ansible-vault edit inventories/prod/group_vars/vault.yml
```

The `vault.yml` files shipped here hold **placeholder** values so the repository never
contains a real secret. Before deploying anywhere real, replace them with your own:

```bash
ansible-vault create inventories/test/group_vars/vault.yml
ansible-vault create inventories/prod/group_vars/vault.yml
```

## Usage

```bash
# First rollout / re-run
ansible-playbook -i inventories/dev playbooks/deploy.yml --ask-vault-pass

# Upgrade everything, or just one service with a new image tag
ansible-playbook -i inventories/test playbooks/upgrade.yml --ask-vault-pass
ansible-playbook -i inventories/test playbooks/upgrade.yml --ask-vault-pass \
  -e pathlens_target_service=alerting-rest -e image_tag=1.2.3

# Tear down
ansible-playbook -i inventories/prod playbooks/undeploy.yml --ask-vault-pass
```

All three playbooks call the same `kubernetes.core.helm` task (`state=present`/`absent`),
which is idempotent by construction: running `deploy.yml` twice, or running `upgrade.yml`
against a brand-new environment, produces the same result.
