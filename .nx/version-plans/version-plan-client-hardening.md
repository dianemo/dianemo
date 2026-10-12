---
'@dianemo/core': major
---

A request waiting out a retry's back-off now answers to its `signal`, ending with `RequestAbortedError`, and to `stop()`, ending with `ClientUnavailableError` `handler_stopped`; either way the queue entry it kept is released, and a retry is never sent upstream after `stop()` has returned. An OAuth2 token refresh is bounded by the client's axios `timeout`, or 30 seconds when it sets none, and a request whose `signal` aborts while it waits on one stops waiting without cancelling the refresh for the requests coalesced onto it.

A streamed response is now complete once every chunk has passed through to the caller's stream rather than when the upstream connection ends, so a `settleStream` observer settles at the stream's last chunk even when the caller reads slowly. A request that fails at its headers hands the caller the vendor's error body as the unread stream in `error.response.data`, and a retried attempt's body is closed. A keep-alive that finds a streaming request's slot gone logs a warning, and claim marks are refreshed within `settleWindow` as well as `concurrencySlotTtl`.

A queued request whose cost cannot be spent is failed and stepped over rather than blocking its client's queue. A peer registration that cannot be parsed is skipped by role election rather than failing it. `redactUrl` hides a password containing an unencoded `@`, and stored credentials are decrypted only against a full-length authentication tag.

BREAKING: once `stop()` has finished, `handleRequest` and the template mutations refuse with `ClientUnavailableError` `handler_stopped` rather than starting the handler again; call `start()` to bring it back. A template name that is empty or contains whitespace is refused with `invalid_template_name`, and a sub-client name that is empty or contains `:` or whitespace with `invalid_client_name_segment` — rename any that collided once backend keys normalized whitespace to `_`. An empty or blank `requestName` is refused with the new `ConfigurationError` code `invalid_request_name`.
