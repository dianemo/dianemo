---
"@dianemo/core": minor
---

`RequestHandler.purgeClientCredentials(clientName)` deletes a client's stored OAuth2 tokens, every grant's included, so the next request on any replica fetches new ones — for an upstream that refuses a token the handler still holds as valid.
