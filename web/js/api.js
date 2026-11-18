// Cliente de la API REST: timeouts, reintentos con espera exponencial y validación de respuestas.
(function () {
  const { API_BASE, TIMEOUT_MS, RETRIES } = window.CONFIG;

  class ApiError extends Error {
    constructor(kind, message, status) {
      super(message);
      this.kind = kind;     // "timeout" | "network" | "validation" | "http" | "shape"
      this.status = status;
    }
  }

  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

  // Convierte el "detail" de FastAPI (texto o lista de errores de Pydantic) en un mensaje legible.
  function detailMessage(body) {
    const d = body && body.detail;
    if (!d) return null;
    if (typeof d === "string") return d;
    if (Array.isArray(d)) {
      return d.map((e) => {
        const field = Array.isArray(e.loc) ? e.loc.filter((x) => x !== "body").join(".") : "";
        const msg = String(e.msg || "").replace(/^Value error, /, "");
        return field ? `${field}: ${msg}` : msg;
      }).join(" · ");
    }
    return JSON.stringify(d);
  }

  async function once(path, options, timeout) {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), timeout);
    let res;
    try {
      res = await fetch(API_BASE + path, { ...options, signal: ctrl.signal });
    } catch (e) {
      throw e.name === "AbortError"
        ? new ApiError("timeout", "La API no respondió a tiempo.")
        : new ApiError("network", "No se pudo conectar con la API.");
    } finally {
      clearTimeout(timer);
    }
    let body = null;
    try { body = await res.json(); } catch { /* cuerpo no-JSON */ }
    if (!res.ok) {
      const msg = detailMessage(body) || `Error ${res.status} de la API.`;
      throw new ApiError(res.status === 422 || res.status === 404 ? "validation" : "http", msg, res.status);
    }
    if (body === null) throw new ApiError("shape", "La API devolvió una respuesta que no es JSON.");
    return body;
  }

  // Reintenta solo errores transitorios (timeout, red, 5xx). Los 4xx se devuelven al momento.
  async function request(path, options = {}, { onRetry, retries = RETRIES, timeout = TIMEOUT_MS } = {}) {
    let last;
    for (let i = 0; i <= retries; i++) {
      try {
        return await once(path, options, timeout);
      } catch (e) {
        last = e;
        const transient = e.kind === "timeout" || e.kind === "network" || (e.kind === "http" && e.status >= 500);
        if (!transient || i === retries) break;
        const wait = 1500 * 2 ** i;
        if (onRetry) onRetry(i + 1, wait, e);
        await sleep(wait);
      }
    }
    throw last;
  }

  // --- Validación de la forma de las respuestas ---
  const isNum = (x) => typeof x === "number" && Number.isFinite(x);
  const isStr = (x) => typeof x === "string";
  function check(cond, what) {
    if (!cond) throw new ApiError("shape", `Respuesta inesperada de la API (${what}).`);
  }

  const api = {
    ApiError,

    async health(opts) {
      const b = await request("/health", {}, opts);
      check(b.status === "ok" && isStr(b.data_as_of), "health");
      return b;
    },

    async routes(eva, opts) {
      const b = await request(`/stations/${encodeURIComponent(eva)}/routes`, {}, opts);
      check(Array.isArray(b), "routes");
      b.forEach((r) => check(isStr(r.route_key) && Array.isArray(r.hours), "route"));
      return b;
    },

    async predict(payload, opts) {
      const b = await request("/predict", {
        method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload),
      }, opts);
      check(isNum(b.probability) && b.probability >= 0 && b.probability <= 1, "probability");
      check(isNum(b.threshold) && ["bajo", "medio", "alto"].includes(b.risk), "risk");
      return b;
    },

    async hourly(payload, opts) {
      const b = await request("/predict/hourly", {
        method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload),
      }, opts);
      check(Array.isArray(b.hours), "hours");
      b.hours.forEach((h) => check(Number.isInteger(h.hour) && isNum(h.probability), "hour"));
      return b;
    },
  };

  window.api = api;
})();
