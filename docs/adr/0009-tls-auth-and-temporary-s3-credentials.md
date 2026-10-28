# ADR 0009: TLS, Authentication, and Temporary S3 Credentials

## Status

Current state: `Draft`

Date: 2026-10-06

Discussion thread: GitHub issues listed below.

Issues:

- [#69: TLS and mutual TLS across KalDB services](https://github.com/kaldb/kaldb/issues/69)
- [#70: Platform-independent authentication and authorization](https://github.com/kaldb/kaldb/issues/70)
- [#71: Short-lived S3 session credentials with automatic refresh](https://github.com/kaldb/kaldb/issues/71)

PR: `n/a`

Supersedes: `n/a`

Superseded by: `n/a`

This is a proposal for review, not a description of shipped security features.

## Motivation

KalDB needs encrypted transport, authenticated callers, explicit permissions,
and renewable object-storage credentials. The design must work on VMs, bare
metal, Kubernetes, and managed platforms, without requiring AWS, Aiven, a
particular ingress controller, or a service mesh.

The current code has the following gaps:

- `ArmeriaService.Builder` starts a plaintext HTTP listener and registers health,
  metrics, documentation, and management endpoints on the same server.
- `ServerConfig` has address, port, and timeout fields but no TLS/auth settings.
- `SearchContext` hardcodes `gproto+http://`; distributed query clients consume
  discovered URLs without explicit workload TLS configuration.
- The shared server setup does not enforce caller authentication or permissions.
  Manager RPCs include destructive operations such as partition-data reset.
- `CuratorBuilder` explicitly leaves ZooKeeper authentication as a TODO.
- `S3AsyncUtil` uses static basic credentials when explicit keys are configured,
  otherwise the AWS SDK default provider chain. The static path lacks session
  tokens and refresh. The sample configuration defaults to placeholder keys and
  a local HTTP S3 endpoint.
- `Astra` logs its configuration, so adding secret values to that configuration
  would risk exposing them.

## Questions

- **Why can't we simply use Armeria's features?** We can and should use them.
  Armeria provides HTTPS/TLS and authentication extension points, including
  `AuthService` and `Authorizer` in the pinned 1.32.5 dependency. The missing
  feature is KalDB integration, not an Armeria limitation. Configuring a TLS
  listener does not update outbound gRPC clients or discovery, distribute and
  rotate certificates, establish trusted identities, or define dataset and
  manager permissions. Likewise, extracting a bearer token does not establish
  its validity or grant the caller appropriate permissions. We will reuse the
  framework and maintained verification libraries rather than implement TLS
  or cryptographic token verification ourselves.
- **Who issues identities?** An operator-selected CA and OIDC/OAuth-compatible
  identity provider. KalDB validates identities; it does not become a CA,
  password database, or token issuer.
- **What isolation is promised initially?** Proposed first release: cluster-wide
  reader, writer, operator, service, and monitoring permissions within one trust
  domain. Dataset-scoped permissions need a separate explicit decision before
  offering a shared cluster to mutually untrusted tenants.
- **Must deployments run a proxy or mesh?** No. Native TLS/auth is the portable
  baseline. A gateway or mesh may supply transport security if bypass is blocked
  and its identity boundary is explicitly configured and tested.
- **How are certificates rotated?** Initially, atomically replace mounted files
  and perform a controlled rolling restart with sufficient capacity. Live reload
  is a future enhancement, not an assumed property of mounted files.
- **What remains for review?** Confirm cluster-wide permission scope, token
  claim-to-role mapping, certificate SAN identity conventions, and whether a
  temporary dual-listener migration is required. No backward-compatible insecure
  fallback is proposed by default.

## Public Interfaces

Proposed configuration concepts (field names and protobuf numbers are not yet
final):

| Area | Configuration contract |
| --- | --- |
| Server TLS | Explicit transport mode, certificate/key file paths, trust bundle, required client-certificate mode |
| Client TLS | Trust bundle, optional client certificate/key, verified server identity |
| Listener addresses | Bind address separate from advertised address; restricted management listener |
| API authentication | Issuer, audience, configured/discovered JWKS location, allowed algorithms, bounded clock skew |
| Authorization | Explicit mapping from verified claims or workload identities to allowed operations |
| S3 credentials | Explicit default-chain or static mode; documented refreshing sources and optional finite-lived static session credentials |

All new protobuf fields need comments, configuration reference entries, and
sample configurations. Store private keys and credential material in mounted
files or external providers; never expose them in logs or diagnostics.

Clients will use HTTPS and bearer tokens or permitted workload certificates.
HTTP failures use 401/403; gRPC uses UNAUTHENTICATED/PERMISSION_DENIED. Discovery
must preserve the advertised transport scheme. Existing plaintext URLs need a
planned migration; this is operationally incompatible with plaintext-only clients.

## Proposed Changes

### TLS and workload transport

Use Armeria TLS on externally reachable API listeners and internal RPC servers.
Require TLS 1.2 or 1.3 and HTTP/2 negotiation for gRPC. Validate certificate chains
and server SANs; do not disable hostname verification. Invalid security material
must fail startup rather than select plaintext.

Use mTLS for internal query-to-index/cache traffic, with certificates issued by
the deployment's CA. TLS authenticates certificate possession; the auth policy
must separately authorize the asserted workload identity. Do not grant every
certificate issued by the CA access to every RPC.

Update `SearchContext`, discovery publication/consumption, and
`AstraDistributedQueryService` client construction together. Advertised names must
match certificate SANs. Define certificate/trust overlap and connection draining
for rotation, including long-lived pooled gRPC connections.

Gateways can terminate external TLS, but backend traffic must also be protected
when it crosses a network boundary. Trusted gateways must validate backend
identity. Plaintext is acceptable only in an explicit development mode or an
isolated local proxy-to-process hop whose network assumptions are documented.
Mesh deployments must enforce identity and prevent direct bypass; merely
installing a sidecar is insufficient.

### Authentication and authorization

Use short-lived bearer access tokens for external API clients, verified through
a maintained JWT library integrated with Armeria authentication/decorators.
Validate signature, issuer, audience, expiration, not-before, and an algorithm
allowlist. Do not accept an arbitrary token-supplied key URL or unsigned token.
Fetch keys only from trusted configured/discovered locations over verified TLS.

Bound JWKS caching, refresh, retry, and request timeouts. Rate-limit refresh on
unknown key IDs. Never block Armeria event loops on remote identity calls. During
an identity-provider outage, accept only tokens verifiable under the defined
cache/expiry policy; never fail open. Token expiry bounds revocation latency;
immediate revocation/introspection is not part of the initial proposal.

Propagate a verified principal within the request context. Enforce policy on
annotated HTTP services, framed and unframed gRPC, and administrative routes.
Return protocol-appropriate errors. A client-provided identity header must never
be trusted without a separately authenticated proxy boundary.

| Principal | Initial permission |
| --- | --- |
| Reader | Query, schema, mappings, trace and graph reads |
| Writer | Bulk ingestion |
| Operator | Explicit manager/admin operations |
| Query workload | Internal search/schema RPCs on index/cache services |
| Monitoring | Metrics and limited health information |

Default-deny new routes and methods. Do not infer permissions from HTTP verbs:
search uses POST, and bulk payloads carry resource names. If dataset isolation is
required, authorize every parsed bulk/multi-search item and all alternate query
APIs before executing or disclosing metadata.

Browser SSO may be handled by an optional gateway, with secure sessions and CSRF
protection for cookie-authenticated mutations. Machine clients use access tokens
or approved workload identities. Do not forward user bearer credentials to every
internal service by default; use service identity for fan-out and retain caller
context for audit without turning it into trusted client-supplied headers.

### Administrative surfaces and dependencies

Separate management exposure from public query/ingest access. Limit health
responses to probe requirements; restrict metrics to monitoring. Disable docs
and management services unless required, otherwise require operator permission.
Keep manager RPCs and the admin UI private and authenticated.

Production dashboard integration must replace demo anonymous-admin and disabled
security settings. The dashboard gateway's synthetic admin-account response is
not authentication and must not establish production permissions.

Secure Kafka producers and consumers through TLS/SASL using the existing
additional-properties support. Configure ZooKeeper secure transport, identity,
and ACLs, including migration of existing znodes. Protect tracing endpoints.
TLS work owns dependency transport validation; auth work owns dependency
identity/ACL validation. S3 credential renewal is tracked separately below.

Audit identity, operation, outcome, and correlation IDs without logging tokens,
private keys, raw credentials, or sensitive request bodies. Expose bounded-cardinality
authentication failure and certificate-expiry signals.

### Short-lived S3 session credentials

Keep credential acquisition separate from TLS/API authentication. The existing
AWS SDK default chain already supports renewable sources. Explicit access/secret
settings select static basic credentials and override that path today.

Make provider selection explicit and remove production placeholder credentials.
Use a refresh-capable SDK provider for the lifetime of the CRT S3 client. Do not
resolve it once and wrap the result in `StaticCredentialsProvider`.

Portable sources include a configured credential process or a provider adapter
to an operator-managed credential broker. Workload roles and STS are additional
deployment options, not mandatory infrastructure. A process source must return
access key, secret, session token, and expiration; the SDK can then obtain fresh
credentials before expiry. A custom broker needs an agreed authentication and
renewal contract. An existing session token cannot renew itself.

If direct static session credentials are supported, use session credentials and
clearly document that they expire and do not refresh. Environment-injected
one-time tokens are not a renewable source. Validate incomplete credential
configuration rather than silently falling through to a different identity.

Keep S3-compatible endpoint/region configuration independent from provider
selection. Verify TLS and signing/session-token compatibility with the target
object store; not every S3-compatible service implements AWS STS. Do not claim
universal broker or STS compatibility. Refresh failure must be bounded and
observable, and expired credentials must never be treated as valid indefinitely.

### High-level implementation ownership

1. **TLS:** server/client transport, discovery, certificate lifecycle, management
   network separation, dependency transport, deployment guidance and tests.
2. **Auth:** token/workload identity verification, operation permissions,
   management protection, dependency ACLs, audit and client integration/tests.
3. **S3 sessions:** provider selection, token-aware credentials, refreshing-source
   integration, failure behavior and CRT rotation tests.

TLS and auth have a shared workload-identity interface: TLS supplies a verified
peer identity; auth decides its permissions. Neither issue alone establishes the
complete production security boundary.

## Compatibility, Deprecation, and Migration Plan

Keep local insecure examples explicitly labeled development-only. Production
configuration must opt into a defined secure deployment profile, with no silent
fallback when TLS/auth initialization fails.

Provision identities, certificates and trust first. Deploy secure listeners on a
private canary, migrate clients and discovery, verify access policies, then close
old listeners and remove bypass paths. Only introduce temporary dual listeners
if reviewers approve that migration requirement; restrict them and set a removal
milestone. Test old/new node interactions before a rolling upgrade.

Rollback must retain the authenticated ingress boundary and compatible trust
material. Do not make disabling authentication or exposing plaintext a routine
rollback step. ZooKeeper ACL and discovery migrations need their own validated
rollback procedures within the two high-level tasks.

## Test Plan

- Trusted TLS and authorized HTTP/gRPC requests succeed; wrong hostname,
  expired/untrusted certificates, and unauthorized workload identities fail.
- Missing, malformed, unsigned, expired, wrong-issuer/audience tokens fail;
  readers cannot ingest or invoke manager mutations.
- Framed/unframed RPCs, alternate routes, bulk and multi-search APIs enforce the
  same policy. Direct ingress bypass is blocked.
- Certificate and JWKS rotation, provider outages, pooled connections, and
  restart/rolling migration behave according to the documented lifecycle.
- Health probes and metrics remain usable only by intended callers; secrets do
  not appear in config logs, request logs, traces, or error messages.
- S3 reads, writes, and multipart operations continue across multiple credential
  rotations using the real CRT client. Exercise session-token signing, temporary
  source failures, expiry, recovery, and concurrent requests. A mock returning
  credentials without verifying their use is insufficient.
- Kafka, ZooKeeper and tracing use intended transport/identity settings; an
  unauthorized ZooKeeper identity cannot mutate metadata.
- Measure authentication overhead, fan-out latency and ingestion throughput;
  ensure identity lookups do not create per-request remote calls or unbounded work.

## Documentation Plan

Update server/client configuration references, sample deployment configurations,
query/ingest/manager API authentication guidance, and dashboard integration docs.
Provide generic VM/container/Kubernetes examples, certificate/key rotation and
incident runbooks, trust migration instructions, and refreshing S3 credential
source examples. Cloud-specific guidance may supplement this portable baseline.

## Rejected Alternatives

- **Claim Armeria cannot do TLS/auth:** incorrect; its building blocks are the
  recommended implementation base. They do not replace KalDB policy/integration.
- **Ingress TLS alone:** leaves internal transport and direct-backend access
  outside the security boundary unless explicitly secured.
- **Mandatory cloud IAM or a mandatory mesh:** excludes supported deployment
  models and introduces avoidable platform coupling.
- **Shared static API secret as the default identity:** weak caller attribution,
  coarse permissions and manual rotation; not the proposed identity model.
- **Trust every valid certificate equally:** confuses identity with permission.
- **Add an S3 token field and call it refresh:** only represents one credential
  generation; renewal needs an authorized source and expiration-aware provider.

## Consequences

The proposal reuses existing frameworks and works across deployment platforms.
It introduces certificate/identity lifecycle operations, client configuration,
policy enforcement, and migration costs. Cluster-wide permissions do not promise
multi-tenant dataset isolation. Stronger isolation and live certificate reload
remain explicit follow-on decisions.

## References

- [Armeria TLS/server configuration](https://armeria.dev/docs/server/basics/)
- [Armeria service decorators](https://armeria.dev/docs/server/decorator/)
- [Armeria gRPC integration](https://armeria.dev/docs/server/grpc/)
- [AWS SDK credential caching and refresh](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credential-caching.html)
- [External credential-process contract](https://docs.aws.amazon.com/sdkref/latest/guide/feature-process-credentials.html)

Code anchors: `astra/src/main/java/com/slack/astra/server/ArmeriaService.java`,
`server/Astra.java`, `server/ValidateAstraConfig.java`, `chunk/SearchContext.java`,
`logstore/search/AstraDistributedQueryService.java`, `metadata/core/CuratorBuilder.java`,
and `blobfs/S3AsyncUtil.java` (paths after the first are relative to the same
`com/slack/astra` source directory), plus `astra/src/main/proto/astra_configs.proto`.
