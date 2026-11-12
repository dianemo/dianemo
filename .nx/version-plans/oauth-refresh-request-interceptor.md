---
"@dianemo/core": patch
---

A `requestInterceptor` on an OAuth2 `refreshConfig` or `grantRefreshConfig` now
runs. Every refresh on a client that set one failed with a `DataCloneError`
before any token request was made, because the config handed to the interceptor
was copied with `structuredClone`, which refuses the interceptor functions the
config itself carries. The interceptor still receives a copy, so writing into
its argument does not reach the next refresh.
