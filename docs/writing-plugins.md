# Writing a plugin

A plugin packages one integration: how its client is shaped and authenticated,
plus the request functions that use it.

```ts
export const requests = handler.use(acmePlugin);
await requests.acme.cancelShipment("acme:_:production", "SHIP-1234");
```

## The two halves

A plugin is deliberately split in two, and understanding why makes the rest
obvious.

**`registerTemplate`** describes the _shape_ of a client — its rate limit, auth
flow, base URL, sub-clients — and carries **no credentials**.

**`createRequests`** returns the functions that call the API.

Credentials arrive separately at runtime, via `handler.addTemplateClient()`.
That separation is why a plugin never has to know whether its credentials came
from a vault, a database, or an environment variable — and why a template can
be published to npm without anyone's secrets in it.

## Two ways to reach the handler

A request function needs a `tryHandleRequest` bound to the handler that owns it.
There are two ways to get one, and they differ only in how it reaches your
request functions.

**From the context**, using nothing but `@dianemo/core`. `createRequests`
receives the bound function and your requests close over it:

```ts
import { definePlugin } from "@dianemo/core";

export default definePlugin({
  name: "acme",
  registerTemplate: registerAcmeTemplate,
  createRequests: ({ tryHandleRequest }) => ({
    cancelShipment: (clientName: string, id: string) =>
      tryHandleRequest(
        { clientName, requestName: "acme.shipping.cancel", method: "PUT", url: `/ship/v1/shipments/cancel/${id}` },
        "ACM_0001",
        "Failed to cancel shipment"
      ),
  }),
});
```

This is self-contained and puts no constraint on the host. It suits a plugin
that lives in your own codebase, and a handful of request functions defined in
one file.

**From a process-wide binding**, using
[`@dianemo/plugin-kit`](https://github.com/vivo-us/dianemo-plugins/tree/master/packages/plugin-kit).
Request functions import `tryHandleRequest` directly rather than receiving it,
which is what lets them live in separate files without threading a context
through every one:

```bash
npm install @dianemo/plugin-kit
```

`plugin-kit` ships from the plugins repo rather than this one, so its releases
trail core's. `@dianemo/plugin-kit@1.0.0` declares a peer range of
`@dianemo/core ^1.0.0`, so it will not install beside core 2.0 until a
plugin-kit release widens that range, and the vendor plugins built on it are
1.x as well, none of them working with core 2.0 yet. The `definePlugin` form
above is in `@dianemo/core` and always available, so reach for it if the
install fails — the two differ only in how `tryHandleRequest` arrives.

This is what the published
[dianemo-plugins](https://github.com/vivo-us/dianemo-plugins) catalogue uses,
because a vendor integration is routinely a few hundred request functions across
many files. It costs one handler per process — see
[the binding](#the-binding-and-its-one-rule) below.

The rest of this page uses the plugin-kit form. Everything about templates,
naming, rate limits and error codes applies equally to both.

## Minimal plugin

```ts
// src/index.ts
import { bindTryHandleRequest } from "@dianemo/plugin-kit";
import { definePlugin } from "@dianemo/core";
import { registerAcmeTemplate } from "./client.js";
import * as requests from "./requests/index.js";

export default definePlugin({
  name: "acme",
  registerTemplate: registerAcmeTemplate,
  createRequests: (ctx) => {
    bindTryHandleRequest(ctx);
    return requests;
  },
});

export { registerAcmeTemplate } from "./client.js";
export * from "./requests/index.js";
```

`name` is not decoration, and it must equal the template name the plugin passes
to `registerClientTemplate`. That template name is the first segment of every
client name and the key credentials are filed under. `use()` refuses two plugins
with one `name`, but it never sees the template name, so a plugin registering a
template under some other name escapes that check — and two templates of one
name route one's credentials to the other's builder.

Call `use()` before the first request where you can. Called on a handler that
has already started, it registers the template there and then and builds any
tenants already stored for it; since `use()` is synchronous, a registration
that fails at that point is logged as an error rather than thrown.

## The template

```ts
// src/client.ts
import RequestHandler, { buildClientName } from "@dianemo/core";
import type {
  CreateClientData,
  OAuth2Credentials,
} from "@dianemo/core/client/types";

declare module "@dianemo/core" {
  interface ClientTemplates {
    acme: OAuth2Credentials;
  }
}

export async function registerAcmeTemplate(handler: RequestHandler) {
  await handler.registerClientTemplate("acme", (creds): CreateClientData[] => [
    {
      name: buildClientName("acme", creds),
      rateLimit: [
        {
          type: "requestLimit",
          interval: 1000,
          tokensToAdd: 100,
          maxTokens: 100,
        },
      ],
      requestOptions: { defaults: { baseURL: creds.baseUrl } },
      authentication: {
        type: "oauth2",
        clientId: creds.clientId,
        clientSecret: creds.clientSecret,
        refreshConfig: {
          url: `${creds.baseUrl}/oauth/token`,
          dataLocation: "urlEncodedForm",
          data: {
            grant_type: "client_credentials",
            client_id: "{{clientId}}",
            client_secret: "{{clientSecret}}",
          },
        },
      },
    },
  ]);
}
```

Four things worth noting:

- **The `declare module` block** is what gives `addTemplateClient("acme", …)`
  a typed credential shape at every call site. It is not optional polish:
  `ClientTemplates` is empty by design, so without it `keyof ClientTemplates` is
  `never` and neither `registerClientTemplate` nor `addTemplateClient` accepts
  the name at all.
- **`{{clientId}}` placeholders** are substituted at refresh time from
  `authentication`, which holds the secret itself in `clientSecret`. Write them
  as placeholders rather than interpolating the values, so the secret appears
  once there instead of being copied into the token request's `data`.
- **The builder returns an array.** One credential set may produce several
  clients — a vendor with separate rate limits per region, or a
  [`sharedLimit`](rate-limits/shared-limit.md) child alongside its parent.
- **What it returns is copied before core writes to it.** Plans and overrides
  are placed on the copy, so a builder may return a module-level sub-client list
  or a config it keeps per tenant. Under `addTemplateClient` its credentials
  arrive as JSON reads them back from the backend: a `Date` is an ISO string by
  then. `addLocalTemplateClient` stores nothing, so it hands the builder the
  caller's own object unchanged.

Pick the rate limit from what the vendor publishes — see
[choosing a rate limit](rate-limits/README.md), and note that granularity
matters more than most people expect.

## Letting callers pick a plan

Some vendors set the quota by subscription tier, so the template cannot know the
right limit until it knows which plan a tenant is on. Declare the plans and let
the caller name one:

```ts
await handler.registerClientTemplate(
  "acme",
  (creds): CreateClientData[] => [
    {
      name: buildClientName("acme", creds),
      // …auth, baseURL, and everything else as before
    },
  ],
  {
    rateLimitOptions: {
      free: FREE_TIER,
      pro: [
        { name: "per_second", type: "requestLimit", interval: 1000, tokensToAdd: 20, maxTokens: 20 },
        { name: "per_day", type: "requestLimit", interval: 86_400_000, tokensToAdd: 50_000, maxTokens: 50_000 },
      ],
    },
    defaultRateLimitOption: "free",
  }
);
```

The caller then names a key, and nothing else:

```ts
await handler.addTemplateClient("acme", credentials, {
  rateLimitOption: "pro",
});
```

What this does *not* let a caller do is supply a limit of their own. The options
are the plugin author's constants, named by keys; a key the template does not
declare is refused at `addTemplateClient`, and so is any key at all for a
template that declares no options. That refusal is at the boundary rather than at
build time on purpose — the key is stored beside the credentials, so one accepted
now would be read back by every replica and every restart, leaving a client on
the template default with its record saying otherwise.

Operator [overrides](concepts.md#rate-limit-overrides) go beside the key, under
`rateLimitOverrides`, and are checked at the same boundary: an override that is
not a list or cannot be built, or an object mixing `rateLimitOption` with bare
sub-client paths, is refused with a `ConfigurationError` before anything is
stored.

Six more things worth knowing:

- **A plan may hold several limits.**
  [A per-second and a per-day cap](rate-limits/multiple-limits.md) is exactly the
  shape a paid tier tends to take.
- **Plans are validated at registration.** A plan whose budget can never hand out
  a token, a plan written as a single limit rather than a list, two unnamed
  limits in one plan, or a `defaultRateLimitOption` naming a plan that is not
  declared, fails the deploy that introduced it rather than the first tenant who
  picks it.
- **Renaming a plan does not strand its tenants, but it does change their
  limits.** A stored key the template no longer declares is dropped, logged as an
  error, and the client is built from the limits its builder declares — not from
  `defaultRateLimitOption`. A builder that leaves limits to the plan declares
  none, so its tenants on the old key run `noLimit` until they are re-added with
  a key that exists. Keep the old key as an alias until every tenant has moved.
- **A plan never changes what kind of client is built.** `""` reaches every root
  client the builder returns except a
  [`sharedLimit`](rate-limits/shared-limit.md) one, which keeps drawing on its
  owner — and the owner is one of the roots `""` just retuned. A path whose only
  match is a client of the other kind is skipped and logged, as an
  [override](concepts.md#rate-limit-overrides) of that shape would be.
- **One tenant cannot take the others down.** A tenant whose stored settings no
  longer build is logged and skipped — at startup, and when the template is
  registered after `start()` — and the tenants after it are built as usual.
  Re-adding it with `addTemplateClient` replaces the stored settings.
- **A re-add that cannot be built changes nothing.** `addTemplateClient` builds
  the new credentials and settings before storing them, so one that throws is
  refused with the tenant's running clients, their queued requests and the stored
  record all left as they were.

`handler.getRateLimitOptions("acme")` lists the keys, for a host rendering a
picker.

### A different limit per endpoint

Vendors often publish a limit per endpoint rather than one for the account —
Amazon's SP-API being the usual example — and a subscription tier then moves all
of them at once. A plan can be keyed by sub-client path instead of being a single
limit, using the same paths [overrides](concepts.md#rate-limit-overrides) use:
`""` is the root client, `"orders"` its sub-client of that name, `"a:b"` a nested
one.

```ts
await handler.registerClientTemplate(
  "sp",
  (creds): CreateClientData[] => [
    {
      name: buildClientName("sp", creds),
      // The builder says which clients exist. It does not have to place a single
      // limit — the chosen plan is applied to this tree by path.
      subClients: [{ name: "orders" }, { name: "reports" }],
    },
  ],
  {
    rateLimitOptions: {
      standard: {
        "": [{ type: "requestLimit", interval: 1000, tokensToAdd: 1, maxTokens: 1 }],
        orders: [{ type: "requestLimit", interval: 1000, tokensToAdd: 6, maxTokens: 6 }],
        reports: [
          { name: "per_second", type: "requestLimit", interval: 1000, tokensToAdd: 2, maxTokens: 2 },
          { name: "per_day", type: "requestLimit", interval: 86_400_000, tokensToAdd: 500, maxTokens: 500 },
        ],
      },
      premium: { /* the same paths, larger budgets */ },
    },
    defaultRateLimitOption: "standard",
  }
);
```

- **Every path of every plan is validated at registration**, which is the reason
  to declare the tree here rather than keep it in a private constant the plugin
  reads itself. A broken budget four endpoints deep fails the deploy.
- **The plan applies before any operator override**, so an override still wins
  for the one path it names and the plan holds everywhere else.
- **A path matching no client is reported.** A renamed sub-client would otherwise
  leave that endpoint running unlimited while the plan still claimed to cover it.
- **The builder can still read the plan.** `ctx.rateLimits` is the whole resolved
  map and `ctx.rateLimit` its `""` entry, for a builder that wants to decide
  whether a sub-client should exist at all. Each build gets its own copy, so a
  builder adjusting it for one tenant changes nothing for the others.
  `ctx.rateLimitOption` is there for everything a tier changes that is *not* a
  rate limit — a different base URL for enterprise, an endpoint an entry tier
  does not get.

Operator-side [rate-limit overrides](concepts.md#rate-limit-overrides) are a
separate mechanism and need no permission from the template. Plans are the
sanctioned choice offered to a caller; overrides are the escape hatch for whoever
runs the deployment.

## Request functions

```ts
// src/requests/index.ts
import { tryHandleRequest } from "@dianemo/plugin-kit";

export const cancelShipment = async (
  clientName: string,
  id: string
): Promise<void> => {
  await tryHandleRequest(
    {
      clientName,
      requestName: "acme.shipping.cancel",
      method: "PUT",
      url: `/ship/v1/shipments/cancel/${id}`,
    },
    "ACM_0001",
    "Failed to cancel shipment"
  );
};
```

A request function's signature is **its own domain arguments** — a client name
and the data. No handler parameter. That is the whole point of the process-wide
binding: `tryHandleRequest` is already bound to the owning handler, so a request
function stays callable without threading a handler through every layer.

`requestName` is dotted and namespaced by the plugin. It appears in spans, logs
and queue metadata, so make it describe the operation rather than the URL.

The last two arguments are an error code and a human message, used when the call
fails. A stable code per operation is worth the small effort — it is what makes
a failure greppable across services.

## The binding, and its one rule

`bindTryHandleRequest(ctx)` establishes a **process-wide** binding shared by
every plugin in the process. It is kept on `globalThis` under a `Symbol.for`
key rather than in a module variable, so two copies of plugin-kit installed side
by side still see one binding.

The cost is one handler per process. A second handler is refused loudly rather
than silently rerouting the first one's traffic — which would be a genuinely
hard bug to see, since requests would keep succeeding, against the wrong
rate-limit budget.

If you need two handlers, run them in separate processes.

## Plugin-owned state

`backend()` gives you the handler's backend for state your plugin needs to
cache — a session cookie one replica logs in for and the rest reuse, a payment
token with a server-side TTL:

```ts
import { acquireLock, backend, pluginKey, releaseLock } from "@dianemo/plugin-kit";

const key = pluginKey("acme", "token", clientName);
await backend().set(key, token, ttlSeconds);
const cached = await backend().get(key);
```

Build keys with `pluginKey(plugin, ...parts)` rather than by hand. The backend
is shared with the handler's own bookkeeping, with every other plugin, and — if
the host set a `keyPrefix` — with every other handler on the same Redis.
`pluginKey` folds that prefix in; a key written as a bare string skips it.

**How far that sharing reaches is the host's choice, not yours.** Under
[`@dianemo/backend-redis`](backends/redis.md) the cache is fleet-wide; under the
[memory backend](backends/memory.md) it is process-local. A plugin that assumes
sharing repeats work per process rather than breaking, which is the right
failure mode — but do not build correctness on top of it.

`acquireLock` / `releaseLock` cover work that should happen once rather than
once per replica, with the same caveat.

## Types come from inference

There is no module augmentation to write for the request namespace.
`handler.use(a, b)` infers the merged shape from the plugins you pass, so
`requests.acme.cancelShipment` is typed at the call site by virtue of existing.

The type-level test in `test/typeInference.test-d.ts` guards this: if `use()`
ever degraded to returning `any`, every runtime test would still pass while
every consumer silently lost type safety.

## Checklist

- [ ] `name` is unique and equals the template name it registers
- [ ] `declare module` block types the credentials
- [ ] Secrets sit in `authentication` and appear only as `{{placeholders}}` in
      refresh config
- [ ] Rate limit reflects what the vendor actually publishes
- [ ] `requestName` is dotted, namespaced, and describes the operation
- [ ] Every request function takes domain arguments only, no handler
- [ ] Backend keys are built with `pluginKey`
- [ ] A test composes the plugin onto a handler and checks the namespace
