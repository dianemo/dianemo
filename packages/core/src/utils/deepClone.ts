/**
 * An array, or an object literal: the config shapes the merge copies. Anything
 * else — an `http.Agent`, a Buffer, a Map — keeps its identity, since copying
 * one field by field drops its prototype.
 */
export function isPlainData(value: object): boolean {
  if (Array.isArray(value)) return true;
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
}

/**
 * Copies plain objects and arrays, so writing into the copy cannot reach the
 * original: a child's merged config, or a template builder's output that a
 * plan is placed on. Anything else is passed by reference: an
 * `http.Agent`, a `Buffer` or a `Date` copied key by key loses its prototype,
 * and Node's http module refuses an agent that is not an `Agent`.
 */
export function deepCloneWithFunctions<T>(obj: T): T {
  if (obj === null || typeof obj !== "object") return obj;
  if (Array.isArray(obj)) {
    return obj.map((item) => deepCloneWithFunctions(item)) as T;
  }
  if (!isPlainData(obj)) return obj;

  const cloned = {} as T;
  for (const key in obj) {
    if (Object.prototype.hasOwnProperty.call(obj, key)) {
      cloned[key] = deepCloneWithFunctions(obj[key]);
    }
  }
  return cloned;
}
