# Stitch Helm chart

This chart deploys Stitch as a multi-replica `Deployment` behind one
multi-port `ClusterIP` Service. It is internal-only by default. Optional
Gateway API `HTTPRoute` and `GRPCRoute` resources can expose selected
listeners through an existing Gateway.

The chart never creates an Ingress, public Service, Gateway, certificate, or
DNS record.

## Requirements

- Kubernetes 1.25 or newer
- Helm 3
- Gateway API Standard CRDs and a compatible controller when
  `gateway.enabled=true`

## Install

The chart requires a real Stitch configuration. Put the application values
directly at the top level of a values file. The included example runs two
replicas and remains internal to the cluster:

```bash
helm upgrade --install stitch ./helm/stitch \
  --namespace injective \
  --create-namespace \
  --values ./helm/stitch/examples/values.internal.yaml
```

For example, custom application ports can be supplied without a second Service
port configuration:

```bash
helm upgrade --install stitch ./helm/stitch \
  --namespace injective \
  --create-namespace \
  --values ./helm/stitch/examples/values.custom-ports.yaml
```

To use a raw existing Stitch configuration file instead:

```bash
helm upgrade --install stitch ./helm/stitch \
  --namespace injective \
  --create-namespace \
  --set-file config.content=./config.yaml
```

The admin listener must bind to `0.0.0.0:<port>` while chart-managed probes are
enabled. A loopback-only listener cannot receive kubelet probes.

## Internal access

The Service is named `stitch-service` by default. With
`examples/values.internal.yaml`, clients in the same namespace can use:

| Listener | Address |
| --- | --- |
| CometBFT RPC | `stitch-service:5001` |
| Cosmos gRPC | `stitch-service:5002` |
| Cosmos REST | `stitch-service:5003` |
| EVM JSON-RPC | `stitch-service:5005` |
| EVM WebSocket | `stitch-service:5006` |
| ChainStream gRPC | `stitch-service:5007` |
| Injective WebSocket | `stitch-service:5008/injstream-ws` |
| Admin and metrics | `stitch-service:9091` |

Container and Service ports are derived from the final `listen` map. An empty
or omitted listener is disabled, and names such as `eth_rpc` are converted to
Kubernetes port names such as `eth-rpc`.

## Gateway API access

Gateway exposure is explicit and route-by-route. The example exposes EVM HTTP,
EVM WebSocket, Cosmos gRPC, and ChainStream as HTTPRoutes. This works with
HTTPS listeners that admit HTTPRoute but not GRPCRoute:

```bash
helm upgrade --install stitch ./helm/stitch \
  --namespace injective \
  --create-namespace \
  --values ./helm/stitch/examples/values.internal.yaml \
  --values ./helm/stitch/examples/values.gateway.yaml
```

`gateway.parentRefs` points to an existing Gateway and acts as the default for
every Route. A Route can set its own non-empty `parentRefs` to override it.
The Gateway must allow Routes from the release namespace.

Supported targets:

- `HTTPRoute`: `rpc`, `grpc`, `api`, `eth-rpc`, `eth-ws`, `chainstream`,
  `inj-ws`
- `GRPCRoute`: `grpc`, `chainstream`

Use HTTPRoute for native gRPC when the selected HTTPS listener does not admit
GRPCRoute. The Service marks these backend ports as `kubernetes.io/h2c`.
For gRPC and ChainStream routes, disable both Gateway timeouts:

```yaml
timeouts:
  request: 0s
  backendRequest: 0s
```

GRPCRoute remains available for Gateway listeners that explicitly admit it.
The admin listener cannot be selected. Verify the Gateway controller's
WebSocket support and any controller-level timeout policy before production
use.

## Configuration and secrets

The normal configuration mode uses the same top-level keys as Stitch itself:

```yaml
listen:
  rpc:   { addr: "0.0.0.0:30000" }
  api:   { addr: "0.0.0.0:30001" }
  grpc:  { addr: "0.0.0.0:30002" }
  admin: { addr: "0.0.0.0:30099" }

log:
  level: info
  format: json

policies:
  failover:
    max_attempts: 3
    per_attempt_timeout: 8s

backends:
  - name: archive
    coverage: { kind: archive }
    endpoints:
      rpc: http://archive.example.internal:26657
      api: http://archive.example.internal:10337
      grpc: archive.example.internal:9900
```

The chart renders `listen`, `log`, `policies`, `backends`, and non-empty
`auth` values into `stitch-config`. The generated ConfigMap is checksummed onto
the Pod template, so any application configuration change triggers a rolling
restart.

Raw and externally managed configurations are escape hatches:

```yaml
config:
  content: ""             # raw full Stitch YAML
  existingConfigMap: ""
  existingSecret: ""
  key: config.yaml
```

Leave all three source fields empty to use structured values. Otherwise, set
exactly one source. Raw `config.content` is parsed to discover listener ports.
For an existing ConfigMap or Secret, Helm cannot inspect the file, so repeat
its listener addresses in the top-level `listen` map or provide the advanced
`listeners` override. External ConfigMap or Secret changes require a rollout
or an explicit Stitch reload.

Stitch expands `${VARIABLE}` references in its YAML. Use `extraEnv` or
`extraEnvFrom` to source upstream URLs and credentials from Secrets:

```yaml
extraEnvFrom:
  - secretRef:
      name: stitch-upstreams
```

## Replicas and rollouts

The default is two replicas with preferred node anti-affinity, a
`PodDisruptionBudget` of `minAvailable: 1`, and a rolling update that keeps all
existing replicas available before replacing them.

Stitch keeps caches, circuit-breaker state, and subscriptions in each process.
Replicas do not need persistent storage or session affinity for correctness.
Existing WebSocket and streaming gRPC clients must reconnect when their Pod is
replaced.

If response caching is enabled, size the Pod's memory limit above
`policies.cache.l1_size_mb` plus normal process overhead.

## Naming

The base name is always `stitch`; the Helm release name is not prepended.
Resources are named `stitch`, `stitch-service`, `stitch-config`, and
`stitch-<route>`.

For a second installation in the same namespace, override the whole base:

```yaml
fullnameOverride: stitch-testnet
```

## Validation

Render the internal chart:

```bash
helm lint ./helm/stitch \
  --values ./helm/stitch/examples/values.internal.yaml

helm template stitch ./helm/stitch \
  --namespace injective \
  --values ./helm/stitch/examples/values.internal.yaml
```

Offline Gateway rendering needs the API versions supplied to Helm:

```bash
helm template stitch ./helm/stitch \
  --namespace injective \
  --api-versions gateway.networking.k8s.io/v1/HTTPRoute \
  --values ./helm/stitch/examples/values.internal.yaml \
  --values ./helm/stitch/examples/values.gateway.yaml
```
