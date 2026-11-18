// http.server anuncia http://0.0.0.0:PORT, pero la API solo acepta (CORS) localhost, 127.0.0.1 y GitHub Pages
if (location.hostname === "0.0.0.0") location.replace(location.href.replace("//0.0.0.0", "//localhost"));

// Configuración de la web. Para usar una API local: ?api=http://localhost:8000
window.CONFIG = {
  API_BASE: new URLSearchParams(location.search).get("api") || "https://db-delay-api-rrbh.onrender.com",
  DATA_BASE: "data",
  TIMEOUT_MS: 15000,        // por intento
  COLD_START_MS: 90000,     // Render (plan gratuito) puede tardar ~1 min en despertar
  RETRIES: 3,
};
