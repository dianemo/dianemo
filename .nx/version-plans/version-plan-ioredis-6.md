---
'@dianemo/backend-redis': major
---

`@dianemo/backend-redis` accepts ioredis 5.4 or later, or ioredis 6, so a project already on ioredis 6 installs it without a peer-dependency conflict. Both majors run the full suite, including under ioredis 6's default RESP3 protocol, and a freeze clamped to `Number.MAX_SAFE_INTEGER` now reads back exactly on either.
