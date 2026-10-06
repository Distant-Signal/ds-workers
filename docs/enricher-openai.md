# Using the OpenAI API for the enricher

How to point the enricher (`crates/enricher`) at OpenAI's own platform with
`gpt-6-luna` at reasoning effort `none`, what that costs, and what to check
before switching production.

**Status (2026-10-06): not deployed.** The production default is still the
self-hosted model. Nothing in the chart or the service defaults to OpenAI,
and [keyless auth](#keyless-auth-workload-identity-federation) is off by
default too.
Switch only after the [evaluation checklist](#before-switching-production)
passes.

OpenAI documentation this page relies on (read 2026-10):

- Structured outputs: <https://developers.openai.com/api/docs/guides/structured-outputs>
- Latest model guide (reasoning effort and `temperature`): <https://developers.openai.com/api/docs/guides/latest-model>
- Model page: <https://developers.openai.com/api/docs/models/gpt-6-luna>
- Rate limits: <https://developers.openai.com/api/docs/guides/rate-limits>
- Error codes: <https://developers.openai.com/api/docs/guides/error-codes>

## Configuration

Service env vars (the chart value that sets each one is in brackets):

| Env var | Value | Why |
| --- | --- | --- |
| `LLM_BASE_URL` | `https://api.openai.com/v1` | [`enricher.llm.baseUrl`] |
| `LLM_MODEL` | `gpt-6-luna` | [`enricher.llm.model`]. Also the extraction's `model_version`, so switching re-extracts every uncleared incident on the next sweep. |
| `LLM_API_KEY` | an OpenAI project key | [`enricher.llm.existingSecret` + `existingSecretApiKeyKey`]. Use a dedicated project with its own key and a monthly budget. Or no key at all: see [keyless auth](#keyless-auth-workload-identity-federation). |
| `LLM_REASONING_EFFORT` | `none` | [`enricher.llm.reasoningEffort`]. Required; see below. |
| `LLM_MAX_TOKENS` | **unset** | This model reportedly rejects `max_tokens`. Unset means the field is never sent. |
| `LLM_REQUEST_TIMEOUT_SECS` | `120` | [`enricher.llmRequestTimeoutSecs`]. Effort `none` answers in seconds; confirm with the perf benchmark. |
| `LLM_MAX_IN_FLIGHT` | `3` | [`enricher.extraEnv`] |
| `LLM_RATE_LIMIT_RETRIES` | `3` | [`enricher.extraEnv`]. Real rate limits only; a quota 429 is never retried (below). |
| `LLM_GATEWAY_RETRIES` | `1` | [`enricher.extraEnv`]. One retry on 502/503/504 or a client timeout. |

These are the same settings as the `openai-gpt-6-luna-none` target in
`crates/enricher/eval/targets.example.toml`, so the eval measures the
configuration you would deploy.

Chart values (the key lives in a Secret you create; never in values):

```yaml
enricher:
  llm:
    baseUrl: https://api.openai.com/v1
    model: gpt-6-luna
    reasoningEffort: "none"
    existingSecret: enricher-openai        # kubectl create secret generic enricher-openai --from-literal=llm-api-key=...
    existingSecretApiKeyKey: llm-api-key
  llmRequestTimeoutSecs: 120
  extraEnv:
    - { name: LLM_MAX_IN_FLIGHT, value: "3" }
    - { name: LLM_RATE_LIMIT_RETRIES, value: "3" }
    - { name: LLM_GATEWAY_RETRIES, value: "1" }
```

Quote `"none"` in YAML. Unquoted, it is still the string `none`, but
quoting makes it obvious it is not a null. If the chart's opt-in egress
NetworkPolicies are on, the enricher's public-internet egress already
covers `api.openai.com`. Any `extraEgress` rule kept for the old tailnet
endpoint can go once the switch is done.

### Why the effort must be `none`

Every request the enricher sends has `temperature: 0`, so the same text
gives the same extraction as far as the model allows. For gpt-6-luna,
`temperature` is accepted only at reasoning effort `none`. At every other
effort (`low`, `medium`, `high`, `xhigh`, `max`) it must be left out, and
leaving `LLM_REASONING_EFFORT` unset means the model's default, `medium`,
so an unset effort breaks every request (latest-model guide; model page).
The enricher has no switch to drop `temperature`, on purpose: effort `none`
is the configuration chosen. Effort `none` is also the cheapest and
fastest option, and it leaves no reasoning tokens to bill (check
`reasoning_tokens` in the eval records).

`LLM_REASONING_EFFORT=none` passes config validation (the knob is free
text) and is sent as `"reasoning_effort": "none"`. Both are unit-tested
(`config::tests::reasoning_effort_none_is_accepted_and_passed_through`,
`llm::tests::reasoning_effort_none_is_sent_with_temperature_and_without_max_tokens`).

### Schema strictness

The enricher always sends `response_format: {"type": "json_schema",
"json_schema": {"strict": true, ...}}`. In strict mode OpenAI requires
(structured-outputs guide):

- `additionalProperties: false` on every object;
- every property listed in `required`;
- a root that is an object;
- nullable written as a type union with `"null"`.

OpenAI rejects a schema outside the supported subset with an error rather
than ignoring it. The three schemas in `llm.rs` (primary,
resolution-adversarial and severity-adversarial) meet these rules. The
nullable objects `date_range` and `schedule_window` are written as
`anyOf: [{object}, {"type": "null"}]`, the form the guide documents for
optional objects. `llm::strict_schema_tests` walks every schema and fails
on any object without `additionalProperties: false`, any property missing
from `required`, a non-object root, a nullable object written as a type
union, or an unsupported keyword (`allOf`, `oneOf`, `not`,
`if`/`then`/`else`, `dependent*`, `patternProperties`,
`minLength`/`maxLength`, `$ref`). It also checks that the shipped eval
dataset's gold answers validate against the schemas and parse through the
service's own parsers.

The tightened schemas are also what Ollama and NVIDIA receive. The JSON the
model returns is the same shape, and the parsers did not change.

### Errors and what the enricher does with them

| Response | Outcome label (`enricher_llm_call_total`) | Retried in-call? | Feeds the per-text backoff? |
| --- | --- | --- | --- |
| 429 rate limit (`rate_limit_exceeded`, or no OpenAI body) | `rate_limited` | Yes, up to `LLM_RATE_LIMIT_RETRIES`, waiting `Retry-After` (at least `LLM_RATE_LIMIT_RETRY_SECS`; over 600 s fails at once) | No |
| 429 with `error.code`/`error.type` `insufficient_quota`, `billing_hard_limit_reached` or `billing_not_active` | `quota_exhausted` | No: no wait fixes an empty account | No: it isn't the text's fault. Reclaim keeps retrying at its normal cadence, and each attempt fails fast and costs nothing. |
| 503 `server_is_overloaded` (and any 502/504) | `gateway_error` | Yes, up to `LLM_GATEWAY_RETRIES`, after `Retry-After` when sent (same 600 s cap), otherwise 2 s, 4 s, ... up to 30 s | 502/503 no; 504 only when gateway retries are off |
| 200 with `message.refusal` set and null content (a safety refusal) | `refused` | No | Yes: it is the model's answer to this text |
| 200 with null/empty content | `empty_content` | No | Yes |
| Keyless auth only: no access token could be minted (token file, Authentik or OpenAI token endpoint) | `auth_error` | No (nothing was sent) | No |
| Keyless auth only: 401 to a freshly exchanged token (the first 401 is resent once with a new token, spending no retry budget) | `unauthorized` | No | No |

Every failed call is logged with the response's `x-request-id`
(`request_id` field), plus `error.code`/`error.type` from the body. Give
OpenAI support the `request_id` when you report a problem. In the model
eval, `refused` and `empty_content` count as invalid output, not transport
failures, and each record keeps the response's token `usage`
(`prompt_tokens`, `completion_tokens`, `reasoning_tokens`,
`cached_tokens`) when the provider sends it.

`DistantSignalEnricherErrors` fires on any non-`success` outcome. A
`quota_exhausted` outcome means "top up the account or raise the budget".

## Keyless auth (workload identity federation)

**Off by default** (`enricher.llm.auth: apiKey`). With it on, the enricher
holds no OpenAI API key and no other static secret. It exchanges a
short-lived Kubernetes service-account token for an OpenAI access token that
lasts at most an hour, and renews it by exchanging again.

OpenAI documentation this section relies on (read 2026-10):

- Guide: <https://developers.openai.com/api/docs/guides/workload-identity-federation>
- Kubernetes (self-managed clusters, uploaded JWKS): <https://developers.openai.com/api/docs/guides/workload-identity-federation/kubernetes>
- Token exchange reference: <https://developers.openai.com/api/reference/workload-identity-federation>
- Permissions (RBAC): <https://developers.openai.com/api/docs/guides/rbac>
- Spend limits: <https://developers.openai.com/api/docs/guides/spend-limits>

Authentik's side: <https://docs.goauthentik.io/add-secure-apps/providers/oauth2/machine_to_machine/>
("JWT authentication", "Kubernetes service account tokens").

### The two flows

**Primary, `openaiWifAuthentik`** (`LLM_AUTH=openai-wif-authentik`):

1. The kubelet projects a service-account token for the enricher's own
   ServiceAccount into `/var/run/secrets/openai/token`, with audience
   `workloadIdentity.tokenAudience` (default: the Authentik client ID). It
   is valid for an hour and rotated by the kubelet.
2. The enricher POSTs it, form-encoded, to Authentik's token endpoint
   (`LLM_AUTHENTIK_TOKEN_URL`, e.g. `https://sso.cursed.solutions/application/o/token/`):
   `grant_type=client_credentials`, `client_id=<client ID>`,
   `client_assertion_type=urn:ietf:params:oauth:client-assertion-type:jwt-bearer`,
   `client_assertion=<k8s token>` (and `scope`, if set). This is Authentik's
   JWT-federation machine-to-machine flow: Authentik checks the token's
   signature against the k3s JWKS it holds, runs the application's
   policies, and issues its own RS256 access token.
3. That Authentik token is the subject token of the OpenAI exchange.

**Fallback, `openaiWifKubernetes`** (`LLM_AUTH=openai-wif-kubernetes`): the
projected token (audience = the OpenAI provider's audience) is the subject
token itself. OpenAI verifies it against a k3s JWKS uploaded to the provider.

**The OpenAI exchange** (both modes): `POST https://auth.openai.com/oauth/token`
(`LLM_TOKEN_EXCHANGE_URL`), JSON:

```json
{
  "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
  "subject_token": "<subject token>",
  "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
  "identity_provider_id": "<OPENAI_IDENTITY_PROVIDER_ID>",
  "service_account_id": "<OPENAI_SERVICE_ACCOUNT_ID>"
}
```

The response's `access_token` (`token_type: Bearer`, `expires_in`,
`expires_at`) goes out as `Authorization: Bearer` on `/v1/chat/completions`.
There is no refresh token. OpenAI's token never outlives its subject token:
with 15-minute Authentik tokens, each OpenAI token lasts at most 15 minutes;
in the fallback, at most what is left of the projected token.

### How the enricher handles tokens

- It caches the OpenAI token, and in the primary mode the Authentik token
  (by its own `expires_in`). It refreshes one at
  `t0 + min(expires_in, expires_at - now) - max(LLM_TOKEN_REFRESH_SKEW_SECS, 10% of the lifetime)`,
  where `t0` is when the request started (skew default 60 s).
- Refreshes are single-flight: concurrent calls wait for one exchange, and
  share its failure too. Each token request has its own 10 s timeout.
- It re-reads the token file on every exchange, since the kubelet rotates it.
- If a refresh fails while the cached token is still valid, it keeps using
  that token until it expires (and logs a warning).
- A 401 from `/v1/chat/completions` drops the token (only if it is still the
  cached one, so concurrent 401s cause one refresh) and resends once with a
  new one. A second 401 fails the call as `unauthorized`. This happens
  before the 429/5xx handling, so no retry budget is spent. A 403 is never
  refreshed. No token at all fails the call as `auth_error` without sending
  it. Both are provider-side: the incident's text does not enter the
  per-text backoff, and reclaim retries it at its normal cadence.
- It never logs a token. The exchange logs carry `stage`, `status`, the
  OAuth `error_code` and `error_description` (with any echoed credential
  replaced by `[redacted]`).
- Metrics: `enricher_llm_token_exchange_total{stage=authentik|openai, outcome}`
  (`success`, `token_file_error`, `invalid_grant`, `invalid_client`,
  `invalid_subject_token`, `http_error`, `timeout`, `error`) and
  `enricher_llm_token_remaining_seconds`. The alert is
  [DistantSignalEnricherTokenExchangeFailing](alerts.md#distantsignalenrichertokenexchangefailing).
- At startup, a WIF mode with a missing ID or URL, with `LLM_API_KEY` set, or
  with an unreadable or empty token file is an error, and the pod does not
  start.

### Why Authentik is primary, and why the fallback stays

OpenAI's token exchange reference lists, under Limitations
(<https://developers.openai.com/api/reference/workload-identity-federation>):

> Arbitrary OIDC issuer endpoints other than the providers documented in the
> [setup guides](https://developers.openai.com/api/docs/guides/workload-identity-federation)
> aren't supported yet.

and the guide says "For the OpenAI API, contact OpenAI support if your OIDC
provider isn't listed." Authentik is not listed. Self-managed Kubernetes is,
with an uploaded JWKS. But the dashboard offers a generic OIDC provider
type, so the Authentik route is tried first:

- OpenAI fetches Authentik's keys through standard discovery. Rotating the
  k3s service-account signing key then touches only Authentik, not OpenAI.
- Access can be revoked centrally in Authentik (disable the application,
  the provider, or the group), without touching OpenAI or the cluster.
- The group restriction lives in our IdP.

If OpenAI rejects the Authentik provider or the exchange, or stops
accepting it later, [switch to the fallback](#switching-to-the-fallback),
which is OpenAI's documented path. Configure the fallback provider and
mapping ahead of time, so switching is a values change.

### Setup checklist

Placeholders: `<ns>` is the release namespace; `<enricher-sa>` the
enricher's ServiceAccount (`<release>-enricher` with
`enricher.serviceAccount.create: true`); `<k3s-issuer>` the cluster's
service-account issuer; `<app-slug>`, `<client-id>` and `<ds-openai-enricher>`
the Authentik application slug, provider client ID and group;
`<authentik-sub>` the `sub` Authentik puts in its token;
`<identity-provider-id>` and `<openai-service-account-id>` the OpenAI IDs.
None of these is a secret, but keep real values out of this repo.

**Cluster (k3s):**

1. Read the issuer and the public keys (no secret in either):

   ```sh
   kubectl get --raw /.well-known/openid-configuration | jq -r .issuer   # <k3s-issuer>
   kubectl get --raw /openid/v1/jwks > k3s-jwks.json
   ```

   The issuer need not be reachable from the internet: neither Authentik
   nor OpenAI fetches it; both get the uploaded JWKS.

**Authentik (`https://sso.cursed.solutions`):**

1. A **Generic OAuth Source** (Directory > Federation and Social login >
   Create > OpenID OAuth Source) named e.g. `k3s-service-accounts`, holding
   the raw k3s JWKS (`k3s-jwks.json`) in its **OIDC JWKS** field, since the
   k3s issuer is not public (no JWKS URL). It is never used for an
   interactive login. Any field the form requires but this flow never uses
   (URLs, consumer key) gets an inert value.
2. A dedicated **OAuth2/OpenID provider**, e.g. `ds-openai-enricher`:
   - issuer mode: per-provider (`https://sso.cursed.solutions/application/o/<app-slug>/`);
   - signing key: an RSA certificate-keypair, so its tokens are RS256;
   - access token validity: short, e.g. `minutes=15`;
   - **Federated OIDC Sources**: the source from step 1;
   - subject mode: **based on the user's username**, so `<authentik-sub>`
     stays the same if Authentik regenerates the account;
   - scope mappings: the default `profile` mapping emits `groups`, and the
     enricher requests it with `authentik.scope: profile`. Add a custom
     scope mapping only if the token lacks a claim you need.
3. A dedicated **application**, slug `<app-slug>`, using that provider,
   policy engine mode **all**, bound to:
   - an **expression policy** that checks the incoming Kubernetes token.
     Authentik checks its signature and expiry, but not its audience, so
     this policy is what pins the issuer, the exact service account and the
     audience:

     ```python
     jwt = request.context.get("oauth_jwt") or {}
     aud = jwt.get("aud")
     auds = aud if isinstance(aud, list) else [aud]
     return (
         jwt.get("iss") == "<k3s-issuer>"
         and jwt.get("sub") == "system:serviceaccount:<ns>:<enricher-sa>"
         and "<client-id>" in auds
     )
     ```

   - the group `<ds-openai-enricher>` (below).
4. **Group restriction.** For JWT federation Authentik creates a service
   account itself, derived from the provider name and the Kubernetes `sub`.
   Create a group `<ds-openai-enricher>` with no other members and bind the
   application to it (step 3). The first exchange creates the account and is
   then refused by the group binding; add that account (and only it) to the
   group, then exchange again. The `groups` claim is emitted through the
   `profile` scope and checked again by OpenAI (below). See the risks: the
   generated account's lifecycle needs checking before relying on this.

**OpenAI (Platform):**

1. A dedicated **project**, e.g. `distant-signal-enricher`:
   - model allowlist: `gpt-6-luna` only;
   - a monthly **hard spend limit** plus a spend alert below it;
   - **no API keys**. If creating the service account below also created a
     key, delete it.
2. A **Workload Identity Provider** (Organization settings > Security >
   Workload Identity Provider), type **OIDC**:
   - OIDC Issuer URL: `https://sso.cursed.solutions/application/o/<app-slug>/`
     (the Authentik provider's issuer; a trailing slash is ignored);
   - Audience: `<client-id>` (what Authentik puts in `aud`);
   - standard OIDC discovery: no custom discovery URL, no uploaded JWKS;
   - attribute transformation (if the dashboard accepts it): suffix
     `in_group`, expression `"<ds-openai-enricher>" in assertion.groups`.
     Mapping values must be scalars, so the `groups` array becomes the
     boolean `openai.in_group`. A token without `groups` fails the mapping
     (fails closed).
3. A **service account mapping** on that provider:
   - `sub` = `<authentik-sub>`, exact (no wildcard);
   - `openai.in_group` = `true` (if step 2's transformation exists);
   - project: the one from step 1; service account: a new one, e.g.
     `ds-enricher-wif` (`<openai-service-account-id>`);
   - permissions: **`api.model.request` only**. OpenAI's RBAC guide lists it
     as covering `/v1/chat/completions` together with the other model
     endpoints (`/v1/audio`, `/v1/embeddings`, `/v1/images`,
     `/v1/moderations`, `/v1/realtime`, `/v1/responses`). There is no
     narrower permission; the project's model allowlist and spend limit
     confine what it can do.

**Fallback mapping (`openaiWifKubernetes`), set up ahead of time:**

1. A second **Workload Identity Provider**, type OIDC:
   - OIDC Issuer URL: `<k3s-issuer>`;
   - Audience: a dedicated opaque string, `<openai-wif-audience>` (e.g.
     `https://api.openai.com/v1`, or something unique to this workload);
   - **Use uploaded JWKS**: the full `k3s-jwks.json`, `keys` array included.
2. A mapping: `sub` = `system:serviceaccount:<ns>:<enricher-sa>`, exact; the
   same project, service account and `api.model.request` permission. (OpenAI
   allows one mapping per provider and service account, so both providers
   can map to the same service account.)

### HelmRelease values

Primary:

```yaml
enricher:
  serviceAccount:
    create: true                      # <release>-enricher; the only account any rule trusts
  llm:
    baseUrl: https://api.openai.com/v1
    model: gpt-6-luna
    reasoningEffort: "none"
    auth: openaiWifAuthentik
    workloadIdentity:
      identityProviderId: <identity-provider-id>        # the Authentik-issuer provider
      serviceAccountId: <openai-service-account-id>
      # tokenAudience: ""                               # default: authentik.clientId
      # tokenExchangeUrl: https://auth.openai.com/oauth/token
      authentik:
        tokenUrl: https://sso.cursed.solutions/application/o/token/
        clientId: <client-id>
        scope: profile
  llmRequestTimeoutSecs: 120
  extraEnv:
    - { name: LLM_MAX_IN_FLIGHT, value: "3" }
    - { name: LLM_RATE_LIMIT_RETRIES, value: "3" }
    - { name: LLM_GATEWAY_RETRIES, value: "1" }
```

Fallback (differences only):

```yaml
enricher:
  llm:
    auth: openaiWifKubernetes
    workloadIdentity:
      identityProviderId: <fallback-identity-provider-id>  # the k3s-issuer provider
      serviceAccountId: <openai-service-account-id>
      tokenAudience: <openai-wif-audience>
```

Leave `enricher.llm.apiKey` and `existingSecret` empty: the chart refuses to
render a WIF mode with either set, or without a dedicated ServiceAccount.
With the chart's egress NetworkPolicies on, the token URLs' ports are added
to the enricher's internet rule (an IP rule: it opens ports, not hosts).

### Test exchange

Do this once by hand before the first deploy, and again after any change on
either side. Keep tokens in shell variables only (not files, not
third-party JWT decoders), and `unset` them at the end.

```sh
# 1. A token for the enricher's ServiceAccount, as the kubelet would project it.
#    Primary: audience <client-id>; fallback: <openai-wif-audience>.
K8S=$(kubectl -n <ns> create token <enricher-sa> --audience <client-id> --duration 10m)

# Decode a JWT payload locally (no signature check).
claims() { python3 -c 'import base64,json,sys; p=sys.argv[1].split(".")[1]; print(json.dumps(json.loads(base64.urlsafe_b64decode(p+"="*(-len(p)%4))),indent=2))' "$1"; }
claims "$K8S"      # iss = <k3s-issuer>, sub = system:serviceaccount:<ns>:<enricher-sa>, aud = [<client-id>]

# 2. Primary only: Authentik.
AK=$(curl -sS https://sso.cursed.solutions/application/o/token/ \
  -d grant_type=client_credentials -d client_id=<client-id> \
  -d client_assertion_type=urn:ietf:params:oauth:client-assertion-type:jwt-bearer \
  --data-urlencode "client_assertion=$K8S" -d scope=profile | jq -r .access_token)
claims "$AK"       # iss = .../application/o/<app-slug>/, aud = <client-id>, sub = <authentik-sub>, groups contains <ds-openai-enricher>; header alg RS256

# 3. OpenAI (subject: $AK in the primary, $K8S in the fallback).
OAI=$(jq -n --arg t "$AK" --arg idp <identity-provider-id> --arg sa <openai-service-account-id> \
  '{grant_type:"urn:ietf:params:oauth:grant-type:token-exchange", subject_token:$t,
    subject_token_type:"urn:ietf:params:oauth:token-type:jwt",
    identity_provider_id:$idp, service_account_id:$sa}' \
  | curl -sS https://auth.openai.com/oauth/token -H 'Content-Type: application/json' -d @- | jq -r .access_token)

# 4. One tiny request with it.
curl -sS https://api.openai.com/v1/chat/completions -H "Authorization: Bearer $OAI" \
  -H 'Content-Type: application/json' \
  -d '{"model":"gpt-6-luna","reasoning_effort":"none","messages":[{"role":"user","content":"Say OK."}]}' | jq .choices[0].message.content

# 5. The allowlist is in force: any other model is refused (403
#    model_not_found). Send no reasoning_effort here: a model that doesn't
#    take it answers 400 before the allowlist is consulted, which proves nothing.
curl -sS https://api.openai.com/v1/chat/completions -H "Authorization: Bearer $OAI" \
  -H 'Content-Type: application/json' \
  -d '{"model":"gpt-4o-mini","messages":[{"role":"user","content":"Say OK."}]}' | jq .error

unset K8S AK OAI
```

On failure, print the error bodies instead of piping into `jq -r
.access_token` (an OAuth `error` and `error_description`, never a token).

After deploying, check the enricher's logs for `LLM workload identity
federation on` and `LLM token issued` (once per stage), and its `/metrics`
for `enricher_llm_token_exchange_total{outcome="success"}` and a non-zero
`enricher_llm_token_remaining_seconds`.

### Switching to the fallback

1. Make sure the fallback provider and mapping exist (checklist above) and
   that the uploaded JWKS matches `kubectl get --raw /openid/v1/jwks`.
2. Run the test exchange with `--audience <openai-wif-audience>`, skipping
   step 2 and using `$K8S` as the subject.
3. Apply the fallback values (`auth: openaiWifKubernetes`, the fallback
   provider's ID, `tokenAudience`). The pod restarts with a projected token
   for the new audience. Switching back is the reverse.

### k3s signing-key rotation

Kubernetes tokens are signed with the k3s service-account issuer key
(`/var/lib/rancher/k3s/server/tls/service.key`). Whoever verifies them holds
a copy of the public keys: the Authentik source (primary) or the OpenAI
provider (fallback). A token signed by a key missing from that copy is
refused, so **publish the new public key to the verifier first**:

1. Generate the new key and derive its JWK the way Kubernetes does (`kid` is
   the unpadded base64url SHA-256 of the DER public key):

   ```sh
   openssl genrsa -out service-new.key 2048
   kid=$(openssl rsa -in service-new.key -pubout -outform DER | openssl dgst -sha256 -binary | basenc --base64url | tr -d '=')
   n=$(openssl rsa -in service-new.key -noout -modulus | cut -d= -f2 | xxd -r -p | basenc --base64url | tr -d '=')
   jq --arg kid "$kid" --arg n "$n" '.keys += [{use:"sig", kty:"RSA", kid:$kid, alg:"RS256", n:$n, e:"AQAB"}]' k3s-jwks.json > k3s-jwks-next.json
   ```

2. Upload `k3s-jwks-next.json` (old and new keys): in `openaiWifKubernetes`
   mode to the OpenAI provider (**before anything else**); in the primary
   mode to the Authentik source's OIDC JWKS. Uploading it to both does no
   harm and keeps the fallback ready.
3. Rotate in k3s, keeping the old key in the file so existing tokens stay
   valid (k3s docs, "Service-Account Issuer Key Rotation": install a
   `service.key` holding the new key first and the old one after it, with
   `k3s certificate rotate-ca`), and restart the servers.
4. Check that `kubectl get --raw /openid/v1/jwks` now equals
   `k3s-jwks-next.json` (same `kid`s), and that the enricher still mints
   tokens (`enricher_llm_token_exchange_total`).
5. After every old-key token has expired (an hour after the restart; restart
   the enricher to be sure), remove the old key from k3s, then from the
   uploaded JWKS.

An Authentik signing-key rotation (primary mode) needs nothing on OpenAI's
side: OpenAI fetches Authentik's keys by discovery, caches them 600 s and
refetches on an unknown `kid`. A token still signed with the old key is
refused once that key leaves Authentik's JWKS; the enricher then drops it
and gets a fresh one at the next call.

### Risks

- **The primary path is outside OpenAI's documented support.** OpenAI may
  refuse the Authentik provider, or stop accepting it without notice. The
  fallback is the documented path; keep it configured and tested.
- **Authentik does not check `aud`.** The expression policy is what pins
  the issuer, service account and audience; without it, any service-account
  token signed by the cluster could mint OpenAI tokens. Any pod can get a
  token for its own ServiceAccount with any audience, so the exact `sub`
  (one dedicated ServiceAccount) is the real boundary, in Authentik's policy
  and in OpenAI's mapping alike.
- **The generated Authentik account's lifecycle.** Authentik says these
  accounts "expire and are deleted based on the expiry claim" of the
  incoming token (an hour here). If Authentik deletes and recreates it, the
  group membership is lost and exchanges fail closed (`stage="authentik"`,
  `invalid_grant`/`invalid_client`). Before relying on the group binding,
  leave the enricher idle for over an hour and check the account and its
  membership survive. If they do not, drop the group binding and the
  `openai.in_group` mapping key, and rely on the expression policy plus the
  exact `sub`.
- **Who can run as `<enricher-sa>`.** Anyone who can create pods in `<ns>`
  can mount its token, so namespace RBAC is part of the trust boundary, as
  is the k3s signing key itself.
- **Authentik is on the hot path.** OpenAI tokens last at most as long as
  the Authentik token (15 minutes), so the enricher exchanges about every
  13.5 minutes. During an Authentik outage it keeps the cached token until
  it expires, then fails calls as `auth_error` (incidents are retried by
  reclaim, not dropped).
- **`api.model.request` is broader than chat completions** (embeddings,
  images, audio, realtime, responses). The project's model allowlist and
  hard spend limit are what confine a leaked token, for at most its
  lifetime.
- **NetworkPolicy** egress is IP-based: the enricher's internet rule opens
  ports (443), not hostnames.
- **`enricher_llm_token_remaining_seconds`** is set at exchanges and LLM
  calls only; between calls it holds its last value.
- **Untested against the real endpoints.** The client is tested against
  mock token endpoints only. The first real test is the test exchange above.

## Cost

Prices: about **$0.10 per million input tokens**, **$0.01 per million
cached input tokens** and **$0.50 per million output tokens** (model page,
2026-10; check before relying on it).

Assumptions:

- About 200 incident extractions a day, each making 3 calls (primary plus
  two adversarial passes): 600 calls a day.
- Input per incident: about 5,000 tokens. The primary system prompt and
  schema are about 2,700 tokens (measured from `llm.rs` at 4 characters per
  token); incident text and wrappers add about 500. Each adversarial call is
  about 900 tokens.
- Output per incident: about 450 tokens (a few hundred for the primary
  pass, under 100 for each verdict list). Effort `none` adds no reasoning
  tokens.
- No credit taken for prompt caching. The long primary prompt is a stable
  prefix, so `cached_tokens` may bring the cost down. The eval records show
  how much.

That gives about 1.0M input tokens ($0.10) and 0.09M output tokens
($0.05) a day: **about $4.50 a month**. Allowing for retries, text-edit
re-extractions and longer incidents, budget **$4–8 a month**. Set an
OpenAI project budget around $15 a month: the hard limit then surfaces as
`quota_exhausted` instead of an unbounded bill. Switching the model also
re-extracts every uncleared incident once (a one-off of a few cents per
hundred incidents).

### Estimating spend from the metrics

The enricher counts the tokens each response reports, so spend can be
estimated from Prometheus without an OpenAI admin key:

- `distant_signal_enricher_llm_tokens_total{call, kind}`: `call` is
  `primary`, `resolution_adversarial` or `severity_adversarial` (the same
  labels as `enricher_llm_call_total`); `kind` is `prompt`, `completion`,
  `reasoning` or `cached`. Every 2xx response that carries `usage` counts,
  refusals and empty or unparseable content included, because OpenAI bills
  them. All 12 series start at 0.
- `distant_signal_enricher_llm_model_info{model, base_url_host}` is always
  1 and names the model and endpoint host the counts belong to.

How the kinds add up (OpenAI's billing, checked 2026-10-06 against the
[reasoning guide](https://developers.openai.com/api/docs/guides/reasoning)
and the [prompt caching guide](https://developers.openai.com/api/docs/guides/prompt-caching)):

- `cached` is part of `prompt` (`prompt_tokens_details.cached_tokens` is a
  breakdown of `prompt_tokens`). Cached tokens are billed at the cached
  price **instead of** the input price.
- `reasoning` is part of `completion`: reasoning tokens "are billed as
  output tokens" and `completion_tokens_details.reasoning_tokens` is a
  breakdown of `completion_tokens`. **Never add `reasoning` on top.** It is
  there to confirm that effort `none` adds none.

So: cost = (prompt − cached) × input price + cached × cached price +
completion × output price. With gpt-6-luna's prices, in USD over the last
day (use `[30d]` for a rolling month):

```promql
(
    (
        sum(increase(distant_signal_enricher_llm_tokens_total{kind="prompt"}[1d]))
      - sum(increase(distant_signal_enricher_llm_tokens_total{kind="cached"}[1d]))
    ) * 0.10
  + sum(increase(distant_signal_enricher_llm_tokens_total{kind="cached"}[1d])) * 0.01
  + sum(increase(distant_signal_enricher_llm_tokens_total{kind="completion"}[1d])) * 0.50
) / 1e6
and on() count(distant_signal_enricher_llm_model_info{model="gpt-6-luna"})
```

The last line returns nothing unless the enricher is actually running
gpt-6-luna, so a self-hosted model's tokens are never priced as OpenAI's.
Per call site, replace each `sum(...)` with `sum by (call) (...)`. It is an
estimate: tokens of a response that never arrived (a client timeout, a
dropped connection) are billed but not counted, and a model switch inside
the window prices the old model's tokens too. OpenAI's usage dashboard
stays the source of truth.

There is no spend alert in this chart: the OpenAI project's hard budget
($15 a month, owned by Ranma-Config) is the limit, and it surfaces as
`quota_exhausted`.

**Tier 1 is enough.** The load is under one request a minute on average
and at most 3 in flight (`LLM_MAX_IN_FLIGHT`). That is far below Tier 1's
request and token limits (rate-limits guide), and 429s are still handled
if a burst after an outage hits them.

## Model snapshot

`gpt-6-luna` is a single, unpinned alias: OpenAI can update the model
behind it without a name change, and there is no dated snapshot to pin.
Extraction quality can therefore change with no deploy on our side.

- Re-run the quality eval (`openai-gpt-6-luna-none` target) every month,
  and whenever OpenAI announces a model update. Keep the records so the
  runs can be compared.
- `model_version` stays `gpt-6-luna`, so an upstream change does **not**
  trigger re-extraction. Only text changes do.

## Data handling

What leaves the cluster: the incident summary and description (public
National Rail Knowledgebase text, not personal data), the reference date,
and the prompts. No user data is sent.

Per OpenAI's API data commitments (<https://openai.com/enterprise-privacy/>,
read 2026-10; re-check before switching):

- API inputs and outputs are not used to train OpenAI's models by default.
- They are kept for up to 30 days for abuse monitoring, then deleted.
- Zero Data Retention is available only by arrangement with OpenAI sales,
  for eligible endpoints.
- The UK is not one of OpenAI's data-residency regions, so the data is
  processed outside the UK (by default in the US).

### Legal follow-up before switching

The UK legal review (`ds-review/uk-legal-compliance-2026-09-27.md`) and the
privacy notice (`frontend/app/privacy/page.tsx`: "we use a self-hosted AI
model") both assume the LLM is self-hosted. Switching makes OpenAI a US
processor of the incident text. Before the switch:

- Update the privacy notice: incident messages are sent to OpenAI (US) to
  extract timing and severity; no personal data is involved.
- Add OpenAI to the review's processors and international-transfers list.
  Accept OpenAI's DPA for the organisation, and check how it covers UK
  transfers (IDTA or the UK Addendum).
- Re-check that no incident free text carries personal data. The review
  rates this "very rarely". If one does, it now leaves the UK.
- The AI-transparency item (LEG-16) is unchanged in substance. Its wording
  should say "OpenAI" instead of "self-hosted".

## Before switching production

1. Create a dedicated OpenAI project with a monthly budget (about $15), on
   Tier 1 or above, and either a key or, preferably, no key and the
   [keyless auth](#keyless-auth-workload-identity-federation) setup.
2. Run the quality eval with the `openai-gpt-6-luna-none` target next to
   the current production target (see
   [enricher-model-eval.md](enricher-model-eval.md)). Look for:
   - a valid-output rate of 100%, with no `refused` and no schema rejection
     (an HTTP 400 on the first call would mean a schema problem);
   - no false `resolved`;
   - segmentation (`multi_period`) at least as good as the self-hosted
     model's;
   - consistent results across repeats.
3. Confirm that `temperature: 0` at effort `none` is accepted. The first
   eval call proves it: OpenAI rejects the request otherwise.
4. Run the perf benchmark from the cluster's network at concurrency 1 and
   3, with `request_timeout_secs = 120`. Expect a `fits` verdict.
5. Check token usage and cost in the records (`usage` on each call). Is
   `reasoning_tokens` 0? Is the cost in line with the estimate above?
6. Complete the legal follow-up above.
7. Switch with the chart values above. Watch `enricher_llm_call_total` by
   outcome and `enricher_llm_call_duration_seconds` for a day, and expect
   one re-extraction pass over uncleared incidents. Compare the
   [spend estimate](#estimating-spend-from-the-metrics) with OpenAI's usage
   dashboard after that day.
8. Rollback: restore the previous `enricher.llm.*` values. The model
   version changes back, so incidents re-extract with the self-hosted
   model.
