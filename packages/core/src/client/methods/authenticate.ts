import { credentialTtlSeconds } from "../../utils/credentialTtl.js";
import { encrypt, decrypt } from "../../utils/encryption.js";
import { sanitizeError } from "../../utils/redact.js";
import type Request from "../../request/index.js";
import type { AxiosRequestConfig } from "axios";
import type Client from "../index.js";
import {
  ConfigurationError,
  GrantRefreshTokenMissingError,
} from "../../errors.js";
import type {
  AuthDataBasic,
  AuthDataOAuth2,
  AuthDataToken,
  OAuthGrantTypeResponse,
  OAuthRefreshConfig,
  OAuthResponse,
} from "../types.js";

/** Builds the auth header for a request, or nothing if the client has no auth. */
async function authenticate(this: Client, request: Request) {
  if (!this.authData) return;
  const { type, customPrefix, excludePrefix } = this.authData;
  const headerName = this.authData.customHeaderName || "Authorization";
  const prefix = customPrefix || (type === "basic" ? "Basic" : "Bearer");
  let value;
  if (type === "token") value = handleToken(this.authData);
  else if (type === "basic") value = handleBasic(this.authData);
  else
    value = await handleOAuth2.bind(this)(
      this.authData,
      request.config.grantId
    );
  return { [headerName]: `${excludePrefix ? "" : `${prefix} `}${value}` };
}

function handleToken(authData: AuthDataToken) {
  return authData.encodeBase64
    ? Buffer.from(authData.token).toString("base64")
    : authData.token;
}

function handleBasic(authData: AuthDataBasic) {
  return Buffer.from(`${authData.username}:${authData.password}`).toString(
    "base64"
  );
}

/**
 * Coalesces refreshes running in this handler, keyed by credential cache key.
 *
 * Without it, every in-flight request misses the cache the moment a token enters its
 * renewal window and refreshes independently — unmetered, and on a provider that
 * rotates refresh tokens each surplus call replays a redeemed token, which replay
 * detection is specified to answer by revoking the whole family.
 *
 * Module scope, so two client objects sharing one cache key still coalesce; keyed
 * by handler too, since a second handler is another replica and meets this one at
 * the backend lock.
 */
const inFlightRefreshes = new Map<string, Promise<string>>();

/** Clients removed rather than rebuilt, which must not write credentials again. */
const retiredClients = new WeakSet<Client>();

/**
 * Stops `client` writing OAuth credentials from now on. For a client being
 * removed: a refresh still in flight would otherwise write its tokens back after
 * the removal purged them, under a key nothing will look up again.
 */
export function retireCredentialWrites(client: Client): void {
  retiredClients.add(client);
}

/** How long one replica may hold the refresh lock before others assume it died. */
const REFRESH_LOCK_TTL_MS = 30_000;

/** Assumed token lifetime when the provider does not state one. */
const DEFAULT_TOKEN_LIFETIME_SECONDS = 3600;

/**
 * Largest lifetime we will believe. A ten-year cap is far beyond any real access
 * token, and bounding it matters as much as bounding zero: an absurd value is
 * finite and positive, so it passed straight through and stored an expiry the
 * clock never reaches — the never-expiring cache entry this sanitising exists to
 * prevent, reached from the other end. It also produced a TTL Redis refuses
 * outright, which used to leave the credential hash with no expiry at all.
 */
const MAX_TOKEN_LIFETIME_SECONDS = 60 * 60 * 24 * 365 * 10;

/**
 * A renewal that fails while the cached token has not yet expired is logged and
 * the cached token used: the margin exists to renew early, not to fail requests a
 * valid token would serve. Only an expired token makes the failure the request's.
 */
async function handleOAuth2(
  this: Client,
  authData: AuthDataOAuth2,
  grantId?: string
) {
  const existingToken = await getExistingToken.bind(this)(grantId);
  if (existingToken) return existingToken;

  const cacheKey = `${this.authNamespace}:oauth2${
    grantId ? `:${grantId}` : ""
  }`;
  const coalesceKey = `${this.instanceId}|${cacheKey}`;

  let refresh = inFlightRefreshes.get(coalesceKey);
  if (!refresh) {
    refresh = refreshWithLock
      .bind(this)(authData, cacheKey, grantId)
      .finally(() => {
        inFlightRefreshes.delete(coalesceKey);
      });
    inFlightRefreshes.set(coalesceKey, refresh);
  }
  try {
    return await refresh;
  } catch (error) {
    const unexpired = await getUnexpiredToken.bind(this)(grantId);
    if (unexpired === undefined) throw error;
    this.logger.warn(
      { error },
      `Client ${this.name} | token renewal failed; using the cached token until it expires`
    );
    return unexpired;
  }
}

/**
 * Performs the refresh under a backend lock so replicas don't duplicate it.
 *
 * The lock is an optimisation, not a correctness requirement: if it cannot be
 * taken we wait for whoever holds it to publish a token, and failing that
 * refresh anyway. Blocking indefinitely on a peer that may have crashed would be
 * a worse failure than an extra token call.
 *
 * A holder whose refresh fails says so under `failedKey` before releasing, so a
 * waiter that takes the lock next uses the still-unexpired token rather than
 * calling an endpoint that has just failed.
 */
async function refreshWithLock(
  this: Client,
  authData: AuthDataOAuth2,
  cacheKey: string,
  grantId?: string
): Promise<string> {
  const lockKey = `${cacheKey}:refreshLock`;
  const failedKey = `${lockKey}:failed`;
  const lockToken = `${this.instanceId}:${Date.now()}`;
  const tryLock = () =>
    this.backend
      .acquireLock(lockKey, lockToken, REFRESH_LOCK_TTL_MS)
      .catch(() => false);

  let holdsLock = await tryLock();
  let waited = false;
  if (!holdsLock) {
    waited = true;
    const outcome = await waitForPeerRefresh.bind(this)(grantId, tryLock);
    if (outcome.token) return outcome.token;
    holdsLock = outcome.locked;
  }

  try {
    // Re-check under the lock. Whoever held it before us has written a token by
    // now, and refreshing again would rotate the one they just stored out from
    // under every request already using it.
    const fresh = await getExistingToken.bind(this)(grantId);
    if (fresh) return fresh;

    if (waited && (await this.backend.get(failedKey).catch(() => null))) {
      const unexpired = await getUnexpiredToken.bind(this)(grantId);
      if (unexpired !== undefined) {
        this.logger.warn(
          `Client ${this.name} | a peer's token renewal failed; using the cached token until it expires`
        );
        return unexpired;
      }
    }

    const newToken = await handleRefreshMethod.bind(this)(authData, grantId);
    await saveOAuthData.bind(this)(newToken, grantId);
    return newToken.access_token;
  } catch (error) {
    if (holdsLock) {
      await this.backend
        .set(failedKey, "1", REFRESH_LOCK_TTL_MS / 1000)
        .catch(() => undefined);
    }
    throw error;
  } finally {
    if (holdsLock) {
      await this.backend.releaseLock(lockKey, lockToken).catch(() => false);
    }
  }
}

/**
 * Polls for a token written by whichever replica holds the lock, and for the
 * lock coming free without one — which means the holder failed or died, and
 * this waiter now holds it. The deadline tracks the lock's own TTL, not an
 * unrelated constant: by then the lock has lapsed even if its holder never
 * released it.
 */
async function waitForPeerRefresh(
  this: Client,
  grantId: string | undefined,
  tryLock: () => Promise<boolean>
): Promise<{ token?: string; locked: boolean }> {
  const POLL_MS = 100;
  const deadline = Date.now() + REFRESH_LOCK_TTL_MS;
  while (Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, POLL_MS));
    const token = await getExistingToken.bind(this)(grantId);
    if (token) return { token, locked: false };
    if (await tryLock()) return { locked: true };
  }
  return { locked: false };
}

/**
 * Decrypts a cached credential, treating an undecryptable one as absent.
 *
 * Ciphertext written under a previous `key` fails GCM authentication. Reporting
 * a cache miss sends the caller down the refresh path, which rewrites the entry
 * under the current key; throwing would abort before anything could re-encrypt.
 */
function decryptCached(
  this: Client,
  value: string | undefined,
  what: string
): string | undefined {
  if (!value) return undefined;
  try {
    return decrypt(value, this.key);
  } catch {
    this.logger.warn(
      `Client ${this.name} | stored ${what} could not be decrypted with the current key; treating it as absent and re-authenticating. This is expected once after rotating the handler key.`
    );
    return undefined;
  }
}

/**
 * How early to renew, capped at a share of the token's own lifetime — a flat five
 * minutes is longer than some tokens live, and a token issued for 300s or less would
 * be inside its margin the moment it was written, so it could never be served from
 * cache and the coalescing lock could never work for it.
 */
const RENEWAL_MARGIN_MS = 300_000;
const MAX_RENEWAL_FRACTION = 0.5;
/** Never renew later than this, however short the token's declared lifetime. */
const MIN_RENEWAL_MARGIN_MS = 2_000;

function renewalMargin(lifetimeMs: number | undefined): number {
  if (!lifetimeMs || !Number.isFinite(lifetimeMs) || lifetimeMs <= 0) {
    return RENEWAL_MARGIN_MS;
  }
  // Floored, so scaling to the lifetime cannot produce a margin shorter than a
  // round trip. A 200ms declared lifetime gives a 100ms margin, which would hand
  // out a token with less life left than the request needs to arrive.
  return Math.max(
    MIN_RENEWAL_MARGIN_MS,
    Math.min(RENEWAL_MARGIN_MS, lifetimeMs * MAX_RENEWAL_FRACTION)
  );
}

/** The cached access token, if one is stored and not yet inside its renewal margin. */
async function getExistingToken(this: Client, grantId?: string) {
  const existing = await this.backend.hgetall(
    `${this.authNamespace}:oauth2${grantId ? `:${grantId}` : ""}`
  );
  if (!existing.expiresAt || !existing.accessToken) return;

  const expiresAt = Number(existing.expiresAt);
  // A provider that omits `expires_in` used to store the string "NaN", which is
  // truthy above and fails every comparison below — so the token read as valid
  // forever and the client never re-authenticated. Unusable expiry means treat
  // the entry as absent and mint a fresh token.
  if (!Number.isFinite(expiresAt)) {
    this.logger.warn(
      `Client ${this.name} | stored token has an unusable expiry (${JSON.stringify(
        existing.expiresAt
      )}); re-authenticating`
    );
    return;
  }

  const issuedAt = Number(existing.issuedAt);
  const lifetime = Number.isFinite(issuedAt) ? expiresAt - issuedAt : undefined;
  const expired = expiresAt - Date.now() <= renewalMargin(lifetime);
  if (!expired) {
    return decryptCached.bind(this)(existing.accessToken, "access token");
  }
}

/** The cached access token until it expires, renewal margin or not. */
async function getUnexpiredToken(this: Client, grantId?: string) {
  const existing = await this.backend.hgetall(
    `${this.authNamespace}:oauth2${grantId ? `:${grantId}` : ""}`
  );
  const expiresAt = Number(existing.expiresAt);
  if (!existing.accessToken || !Number.isFinite(expiresAt)) return;
  if (expiresAt <= Date.now()) return;
  return decryptCached.bind(this)(existing.accessToken, "access token");
}

async function handleRefreshMethod(
  this: Client,
  authData: AuthDataOAuth2,
  grantId?: string
) {
  // The interceptor's result stays local rather than being written back onto
  // `authData`, which is shared by the client and — for grantRefreshConfig — by
  // every grant on it. Persisting it would let a non-idempotent interceptor
  // accumulate across refreshes, and would let two grants refreshing at once
  // each build a request from the other's intercepted config.
  const baseConfig = getRefreshConfig(authData, grantId);
  // A deep copy, not the live object: `refreshConfig` is shared by every grant
  // on this client, so an interceptor writing into `config.data` would corrupt
  // the `{{...}}` placeholders for all of them.
  const refreshConfig = baseConfig.requestInterceptor
    ? await baseConfig.requestInterceptor(structuredClone(baseConfig))
    : baseConfig;

  const config = await generateConfig.bind(this)(
    authData,
    refreshConfig,
    grantId
  );

  // This config carries `client_secret` and the rotated refresh token by
  // construction, and the call sits outside the request path's error handling.
  let res;
  try {
    res = await this.http(config);
  } catch (error) {
    throw sanitizeError(error);
  }

  if (refreshConfig.responseInterceptor) {
    return await refreshConfig.responseInterceptor(res);
  }
  return res.data as OAuthResponse | OAuthGrantTypeResponse;
}

async function generateConfig(
  this: Client,
  authData: AuthDataOAuth2,
  refreshConfig: OAuthRefreshConfig,
  grantId?: string
) {
  const { clientId, clientSecret } = authData;
  const { url, dataLocation, useBasicAuth } = refreshConfig;
  // Substitute into a copy. `refreshConfig` is one object shared by every grant
  // on this client, so resolving placeholders in place would replace the
  // `{{refreshToken}}` literal with the first grant's token — and every later
  // grant would then send that grant's credentials instead of its own.
  const data: Record<string, string> = { ...refreshConfig.data };
  const config: AxiosRequestConfig = {
    method: "POST",
    url: url,
    headers: { ...refreshConfig?.customHeaders, Accept: "application/json" },
  };
  if (useBasicAuth) {
    if (!config.headers) config.headers = {};
    const buffer = Buffer.from(`${clientId}:${clientSecret}`);
    config.headers.Authorization = `Basic ${buffer.toString("base64")}`;
  }
  for (const key in data) {
    switch (data[key]) {
      case "{{clientId}}":
        data[key] = clientId;
        break;
      case "{{clientSecret}}":
        data[key] = clientSecret;
        break;
      case "{{refreshToken}}": {
        const token = grantId
          ? await fetchGrantRefreshToken.bind(this)(grantId)
          : await fetchClientRefreshToken.bind(this)();
        if (token !== undefined) data[key] = token;
        break;
      }
    }
  }
  switch (dataLocation) {
    case "urlEncodedForm": {
      const formParams = new URLSearchParams();
      if (!config.headers) config.headers = {};
      config.headers["Content-Type"] = "application/x-www-form-urlencoded";
      for (const key in data) {
        formParams.append(key, data[key]);
      }
      config.data = formParams.toString();
      break;
    }
    case "urlQuery": {
      const queryParams = new URLSearchParams();
      for (const key in data) {
        queryParams.append(key, data[key]);
      }
      config.url += `?${queryParams.toString()}`;
      break;
    }
    case "jsonBody":
      if (!config.headers) config.headers = {};
      config.headers["Content-Type"] = "application/json";
      config.data = data;
      break;
  }
  return config;
}

function getRefreshConfig(authData: AuthDataOAuth2, grantId?: string) {
  if (!grantId) return authData.refreshConfig;
  if (!authData.grantRefreshConfig) {
    throw new ConfigurationError(
      "missing_grant_refresh_config",
      "No grantRefreshConfig provided for an OAuth2 grant-type client"
    );
  }
  return authData.grantRefreshConfig;
}

async function fetchGrantRefreshToken(this: Client, grantId: string) {
  const grantData = await this.backend.hgetall(
    `${this.authNamespace}:oauth2:${grantId}`
  );
  if (!grantData.refreshToken) {
    throw new GrantRefreshTokenMissingError(grantId);
  }
  const token = decryptCached.bind(this)(
    grantData.refreshToken,
    `refresh token for grant "${grantId}"`
  );
  // Undecryptable and missing amount to the same thing here.
  if (token === undefined) throw new GrantRefreshTokenMissingError(grantId);
  return token;
}

/**
 * Reads the refresh token cached for a client with no grants, which needs a reader
 * that takes no `grantId` — otherwise it sends the literal `{{refreshToken}}` once
 * its access token lapses. Undefined when nothing is cached.
 */
async function fetchClientRefreshToken(this: Client) {
  const data = await this.backend.hgetall(`${this.authNamespace}:oauth2`);
  if (!data.refreshToken) return;
  return decryptCached.bind(this)(data.refreshToken, "refresh token");
}

async function saveOAuthData(
  this: Client,
  oAuthResponse: OAuthResponse | OAuthGrantTypeResponse,
  grantId?: string
) {
  const { access_token, token_type } = oAuthResponse;
  const key = `${this.authNamespace}:oauth2${grantId ? `:${grantId}` : ""}`;
  const now = new Date().getTime();

  if (typeof access_token !== "string" || access_token.length === 0) {
    throw new ConfigurationError(
      "invalid_token_response",
      `Client ${this.name} received a token response with no usable access_token. Received keys: ${JSON.stringify(
        Object.keys(oAuthResponse ?? {})
      )}.`
    );
  }

  // `expires_in` is optional in practice: providers omit it, and a
  // responseInterceptor normalising a non-standard provider can drop it. Without
  // the fallback, `now + undefined * 1000` stores the string "NaN", which reads as
  // never-expiring and stops re-authentication permanently once the real token
  // lapses. A short assumed lifetime keeps it refreshing.
  const rawExpiresIn = Number(oAuthResponse.expires_in);
  const expires_in =
    Number.isFinite(rawExpiresIn) &&
    rawExpiresIn > 0 &&
    rawExpiresIn <= MAX_TOKEN_LIFETIME_SECONDS
      ? rawExpiresIn
      : DEFAULT_TOKEN_LIFETIME_SECONDS;
  if (rawExpiresIn !== expires_in) {
    this.logger.warn(
      `Client ${this.name} | token response had an unusable expires_in (${JSON.stringify(
        oAuthResponse.expires_in
      )}); assuming ${DEFAULT_TOKEN_LIFETIME_SECONDS}s`
    );
  }

  // The TTL covers the whole hash and `hset` leaves fields it is not given, so
  // sizing must consider the stored refresh token as well as the response's.
  // `refresh_token` is optional in a refresh response (RFC 6749 §6) and several
  // major providers omit it.
  const existing = await this.backend.hgetall(key);
  const hasRefreshToken = Boolean(
    oAuthResponse.refresh_token || existing.refreshToken
  );

  // A refresh response may rotate `refresh_token` while omitting
  // `refresh_token_expires_in` (RFC 6749 §6), so an unknown expiry is recorded as
  // unknown rather than inherited from the token being replaced — inheriting makes
  // the hash TTL shrink on every rotation and eventually deletes a live refresh
  // token, the outcome the TTL sizing exists to prevent.
  //
  // Read once, because `0` reads differently through truthiness than through `??`,
  // and a provider sending `refresh_token_expires_in: 0` means "does not expire".
  // `Number()` also accepts the JSON-string form several providers send.
  const rawStated = Number(oAuthResponse.refresh_token_expires_in);
  const statedRefreshTtl =
    Number.isFinite(rawStated) && rawStated > 0 ? rawStated : undefined;

  const rotatedRefreshToken =
    Boolean(oAuthResponse.refresh_token) && statedRefreshTtl === undefined;

  const storedRefreshTtl =
    !rotatedRefreshToken && existing.refreshTokenExpiresAt
      ? (Number(existing.refreshTokenExpiresAt) - now) / 1000
      : undefined;
  const refreshTtlSeconds =
    statedRefreshTtl ??
    (storedRefreshTtl !== undefined && storedRefreshTtl > 0
      ? storedRefreshTtl
      : undefined);

  // Checked after the last await before the write: a removal retires the
  // client before purging, so nothing written past this point outlives it.
  if (retiredClients.has(this)) {
    this.logger.debug(
      `Client ${this.name} | removed while refreshing; discarding the new token`
    );
    return;
  }
  await this.backend.hset(
    key,
    {
      accessToken: encrypt(access_token, this.key),
      expiresAt: now + expires_in * 1000,
      // Lets the renewal margin scale to the token's own lifetime rather than
      // assuming every token lives longer than the margin.
      issuedAt: now,
      tokenType: token_type,
      ...(oAuthResponse.refresh_token
        ? { refreshToken: encrypt(oAuthResponse.refresh_token, this.key) }
        : {}),
      ...(statedRefreshTtl !== undefined
        ? { refreshTokenExpiresAt: now + statedRefreshTtl * 1000 }
        : rotatedRefreshToken
          ? { refreshTokenExpiresAt: "" }
          : {}),
    },
    credentialTtlSeconds(expires_in, hasRefreshToken, refreshTtlSeconds)
  );
  if (grantId && !retiredClients.has(this)) await this.trackGrantId(grantId);
}

export default authenticate;
