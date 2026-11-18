// ¿Llegará tarde mi tren? — vistas y navegación
(function () {
  const { DATA_BASE, API_BASE, COLD_START_MS } = window.CONFIG;
  const $ = (id) => document.getElementById(id);
  const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const pct = (x, d = 1) => (x * 100).toFixed(d).replace(".", ",") + " %";
  const dec = (x, d = 1) => Number(x).toFixed(d).replace(".", ",");
  const int = (x) => Number(x).toLocaleString("es-ES");
  // Cantidades redondeadas para mostrar: 1.868.679 → "~1,9 M", 8315 → "~8.300", 131 → "131"
  const approx = (x) => {
    x = Number(x);
    if (x >= 1e7) return `~${int(Math.round(x / 1e6))} M`;
    if (x >= 1e6) return `~${dec(x / 1e6).replace(/,0$/, "")} M`;
    if (x >= 1e3) { const k = 10 ** (Math.floor(Math.log10(x)) - 1); return `~${int(Math.round(x / k) * k)}`; }
    return int(x);
  };
  const FONT = getComputedStyle(document.body).fontFamily;

  const COLORS = { red: "#ec0016", redDark: "#bd000f", green: "#10b981", amber: "#f59e0b", delayed: "#dc2626", text: "#18181b", muted: "#71717a", border: "#e4e4e7", soft: "#f4f4f5", tooltip: "#18181b" };
  const CAT_COLORS = { "Larga distancia": "#ec0016", "Regional": "#3b82f6", "S-Bahn": "#10b981" };
  const DAYS = ["Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"];
  const MONTHS = ["ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dic"];
  const monthLabel = (m) => { const [y, mm] = m.split("-"); return `${MONTHS[+mm - 1]} ${y.slice(2)}`; };
  const sparkDowToIdx = (d) => (d + 5) % 7; // Spark: 1 = domingo … 7 = sábado → 0 = lunes

  const baseChart = {
    textStyle: { fontFamily: FONT, color: COLORS.muted },
    tooltip: { backgroundColor: COLORS.tooltip, borderColor: COLORS.tooltip, textStyle: { color: "#fff", fontFamily: FONT, fontSize: 12 } },
    animationDuration: 500,
  };
  const charts = {};
  function chart(id, option) {
    if (!charts[id]) charts[id] = echarts.init($(id), null, { renderer: "canvas" });
    charts[id].setOption({ ...baseChart, ...option }, true);
    return charts[id];
  }
  window.addEventListener("resize", () => Object.values(charts).forEach((c) => c.resize()));

  // ---------------- Datos estáticos ----------------
  const cache = {};
  async function data(path) {
    if (!cache[path]) {
      cache[path] = fetch(`${DATA_BASE}/${path}`).then((r) => {
        if (!r.ok) throw new Error(`No se pudo cargar ${path} (${r.status})`);
        return r.json();
      });
      cache[path].catch(() => delete cache[path]);
    }
    return cache[path];
  }
  function showDataError(viewId, err) {
    const v = $(viewId);
    if (v.querySelector(".data-error")) return;
    v.insertAdjacentHTML("beforeend", `<div class="notice error data-error"><span class="material-symbols-outlined">error</span><span>${esc(err.message)}</span></div>`);
  }

  // ---------------- Estado de la API ----------------
  const apiState = { ready: false, promise: null };
  function setApiStatus(kind, text, detail) {
    const pill = $("api-status");
    pill.className = `status-pill ${kind}`;
    pill.lastElementChild.textContent = text;
    if (detail) $("api-detail").textContent = detail;
  }
  function checkHealth() {
    setApiStatus("wait", "Conectando", "FastAPI en Render");
    const slow = setTimeout(() => { if (!apiState.ready) setApiStatus("wait", "Despertando", "Render tarda ~1 min tras inactividad"); }, 4000);
    apiState.promise = api.health({ timeout: COLD_START_MS, retries: 1 })
      .then((h) => {
        apiState.ready = true;
        setApiStatus("ok", "En línea", `v${h.model_version} · LightGBM · ${approx(h.station_routes)} líneas`);
        return h;
      })
      .catch((e) => {
        setApiStatus("down", "Sin conexión", `${e.message} Reintentando en 30 s…`);
        setTimeout(() => { if (!apiState.ready) checkHealth(); }, 30000);
        throw e;
      })
      .finally(() => clearTimeout(slow));
    apiState.promise.catch(() => {});
    return apiState.promise;
  }
  $("api-status").parentElement.parentElement.addEventListener("click", () => { if (!apiState.ready) checkHealth(); });
  $("docs-link").href = `${API_BASE}/docs`;

  // ---------------- Navegación ----------------
  const views = { prediccion: initPrediction, mapa: initMap, tendencias: initTrends, modelo: initModel };
  const initialized = {};
  function route() {
    const [path, query] = location.hash.replace(/^#\/?/, "").split("?");
    const name = views[path] ? path : "prediccion";
    const params = new URLSearchParams(query || "");
    document.querySelectorAll(".view").forEach((v) => v.classList.toggle("hidden", v.id !== `view-${name}`));
    document.querySelectorAll(".nav a").forEach((a) => a.classList.toggle("active", a.dataset.route === name));
    document.body.classList.remove("menu-open");
    window.scrollTo(0, 0);
    const first = !initialized[name];
    initialized[name] = true;
    Promise.resolve(views[name](params, first)).then(() => {
      requestAnimationFrame(() => {
        Object.values(charts).forEach((c) => c.resize());
        if (name === "mapa" && mapState.map) mapState.map.invalidateSize();
      });
    });
  }
  window.addEventListener("hashchange", route);
  $("menu-btn").addEventListener("click", () => document.body.classList.toggle("menu-open"));

  // =====================================================================
  // Predicción
  // =====================================================================
  const pred = { stations: [], routes: {}, asOf: null, busy: false };

  const isoDate = (d) => `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
  const addDays = (iso, n) => { const d = new Date(iso + "T12:00:00"); d.setDate(d.getDate() + n); return isoDate(d); };
  const fmtDate = (iso) => new Date(iso + "T12:00:00").toLocaleDateString("es-ES", { weekday: "long", day: "numeric", month: "long", year: "numeric" });

  async function initPrediction(params, first) {
    try {
      if (first) {
        const [stations, meta] = await Promise.all([data("stations.json"), data("meta.json")]);
        pred.stations = [...stations].sort((a, b) => a.name.localeCompare(b.name, "de"));
        pred.asOf = meta.updated_at;
        $("f-station").innerHTML = pred.stations.map((s) => `<option value="${esc(s.eva)}">${esc(s.name)}</option>`).join("");
        const minD = addDays(pred.asOf, 1), maxD = addDays(pred.asOf, 180);
        const today = isoDate(new Date());
        const f = $("f-date");
        f.min = minD; f.max = maxD;
        f.value = today < minD ? minD : today > maxD ? minD : today;
        setHours([]);
        $("f-station").addEventListener("change", () => selectStation($("f-station").value));
        $("f-route").addEventListener("change", () => setHours(currentRoute()?.hours || []));
        $("predict-form").addEventListener("submit", (e) => { e.preventDefault(); submitPrediction(); });
      }
      const eva = params.get("eva");
      const target = pred.stations.find((s) => s.eva === eva) ? eva : (first ? "8000105" : null);
      if (target && (first || target !== $("f-station").value)) {
        $("f-station").value = target;
        selectStation(target);
      }
    } catch (e) { showDataError("view-prediccion", e); }
  }

  function currentRoute() {
    const list = pred.routes[$("f-station").value];
    return Array.isArray(list) ? list.find((r) => r.route_key === $("f-route").value) : null;
  }

  function setHours(usual) {
    const sel = $("f-hour");
    const prev = sel.value;
    const set = new Set(usual);
    sel.innerHTML = Array.from({ length: 24 }, (_, h) =>
      `<option value="${h}">${String(h).padStart(2, "0")}:00${set.has(h) ? "" : " · poco habitual"}</option>`).join("");
    const pick = set.has(+prev) ? +prev : usual.find((h) => h >= 7) ?? usual[0] ?? 8;
    sel.value = String(pick);
  }

  function formMessage(kind, html) {
    const box = $("form-msg");
    if (!kind) { box.className = "hidden"; box.innerHTML = ""; return; }
    const icon = { error: "error", warn: "warning", info: "info" }[kind];
    box.className = `notice ${kind}`;
    box.innerHTML = `<span class="material-symbols-outlined">${icon}</span><span>${html}</span>`;
  }

  async function selectStation(eva) {
    formMessage(null);
    stationPanel(eva);
    const sel = $("f-route");
    sel.disabled = true;
    sel.innerHTML = `<option>Cargando líneas…</option>`;
    try {
      if (!pred.routes[eva]) {
        if (!apiState.ready) sel.innerHTML = `<option>Despertando la API…</option>`;
        pred.routes[eva] = await api.routes(eva, {
          timeout: apiState.ready ? 15000 : COLD_START_MS, retries: 2,
          onRetry: (n) => { sel.innerHTML = `<option>Reintentando (${n})…</option>`; },
        });
        if (!apiState.ready) checkHealth();
      }
      if ($("f-station").value !== eva) return;
      const list = pred.routes[eva];
      if (!list.length) {
        sel.innerHTML = `<option>Sin líneas recientes</option>`;
        return;
      }
      const groups = {};
      list.forEach((r) => (groups[r.train_category] ||= []).push(r));
      sel.innerHTML = ["Larga distancia", "Regional", "S-Bahn"].filter((c) => groups[c]).map((c) =>
        `<optgroup label="${c}">${groups[c].map((r) =>
          `<option value="${esc(r.route_key)}">${esc(r.route_key)}${r.destination ? " → " + esc(r.destination) : ""}</option>`).join("")}</optgroup>`).join("");
      sel.disabled = false;
      setHours(list.find((r) => r.route_key === sel.value)?.hours || []);
    } catch (e) {
      if ($("f-station").value !== eva) return;
      sel.innerHTML = `<option>No disponible</option>`;
      formMessage("error", `No se pudieron cargar las líneas: ${esc(e.message)} <a href="#" id="retry-routes">Reintentar</a>`);
      $("retry-routes").addEventListener("click", (ev) => { ev.preventDefault(); selectStation(eva); });
    }
  }

  async function stationPanel(eva) {
    try {
      const [stations, series] = await Promise.all([data("stations.json"), data(`trends/${eva}.json`)]);
      if ($("f-station").value !== eva) return;
      const s = stations.find((x) => x.eva === eva);
      $("s-name").textContent = s ? s.name : "Estación";
      $("s-ontime").textContent = s ? pct(s.pct_on_time) : "—";
      $("s-ontime").className = `kpi-value num ${s ? pctClass(s.pct_on_time) : ""}`;
      $("s-delay").textContent = s ? `${dec(s.avg_delay)} min` : "—";
      const vals = series.map((d) => d.pct_on_time);
      const ma = vals.map((_, i) => { const w = vals.slice(Math.max(0, i - 6), i + 1); return w.reduce((a, b) => a + b, 0) / w.length; });
      chart("station-chart", {
        grid: { left: 36, right: 8, top: 8, bottom: 22 },
        tooltip: { ...baseChart.tooltip, trigger: "axis", valueFormatter: (v) => pct(v) },
        xAxis: { type: "category", data: series.map((d) => d.service_date), axisLine: { lineStyle: { color: COLORS.border } }, axisTick: { show: false },
          axisLabel: { fontSize: 10, interval: 44, formatter: (v) => { const d = new Date(v + "T12:00:00"); return `${d.getDate()} ${MONTHS[d.getMonth()]}`; } } },
        yAxis: { type: "value", min: (v) => Math.floor(v.min * 10) / 10, max: 1, splitNumber: 3, axisLabel: { fontSize: 10, formatter: (v) => Math.round(v * 100) + "%" }, splitLine: { lineStyle: { color: COLORS.soft } } },
        series: [
          { name: "Diaria", type: "line", data: vals, showSymbol: false, lineStyle: { width: 1, color: "#d4d4d8" } },
          { name: "Media 7 días", type: "line", data: ma, showSymbol: false, smooth: true, lineStyle: { width: 2, color: COLORS.red },
            areaStyle: { color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [{ offset: 0, color: "rgba(236,0,22,.15)" }, { offset: 1, color: "rgba(236,0,22,0)" }]) } },
        ],
      });
    } catch (e) { /* panel secundario: se queda vacío */ }
  }

  function setLoading(on, title, sub) {
    $("result-empty").classList.add("hidden");
    $("result-loading").classList.toggle("hidden", !on);
    if (on) { $("result").classList.add("hidden"); $("loading-title").textContent = title; $("loading-sub").textContent = sub || ""; }
  }

  async function submitPrediction() {
    if (pred.busy) return;
    formMessage(null);
    const eva = $("f-station").value, route_key = $("f-route").value, date = $("f-date").value, hour = Number($("f-hour").value);
    // Validación en el cliente (la API vuelve a validar todo)
    const problems = [];
    if (!pred.stations.some((s) => s.eva === eva)) problems.push("elige una estación");
    if (!currentRoute()) problems.push("elige una línea");
    if (!date) problems.push("indica la fecha");
    else if (date < $("f-date").min || date > $("f-date").max) problems.push(`la fecha debe estar entre ${$("f-date").min} y ${$("f-date").max}`);
    if (!(hour >= 0 && hour <= 23)) problems.push("la hora debe estar entre 0 y 23");
    if (problems.length) { formMessage("warn", `Revisa el formulario: ${problems.join(", ")}.`); return; }

    pred.busy = true;
    $("f-submit").disabled = true;
    const cold = !apiState.ready;
    setLoading(true, cold ? "Despertando la API…" : "Consultando el modelo…", cold ? "El servidor gratuito de Render se suspende tras 15 min sin uso; la primera consulta puede tardar hasta un minuto." : "");
    const opts = {
      timeout: cold ? COLD_START_MS : 15000,
      onRetry: (n, wait) => { $("loading-sub").textContent = `La API tarda en responder. Reintento ${n} en ${Math.round(wait / 1000)} s…`; },
    };
    try {
      const [p, h] = await Promise.all([
        api.predict({ eva, route_key, date, hour }, opts),
        api.hourly({ eva, route_key, date }, opts).catch(() => null), // la gráfica es opcional
      ]);
      if (!apiState.ready) checkHealth();
      renderResult(p, h);
    } catch (e) {
      setLoading(false);
      $("result-empty").classList.remove("hidden");
      const msg = e.kind === "validation" ? `La API rechazó la consulta: ${esc(e.message)}`
        : e.kind === "shape" ? esc(e.message)
        : `${esc(e.message)} Comprueba tu conexión o vuelve a intentarlo en unos segundos.`;
      formMessage("error", msg);
    } finally {
      pred.busy = false;
      $("f-submit").disabled = false;
    }
  }

  function renderResult(p, h) {
    setLoading(false);
    $("result").classList.remove("hidden");
    const route = currentRoute();
    $("r-route").textContent = `${p.route_key}${route?.destination ? " → " + route.destination : ""}`;
    $("r-where").textContent = `${p.station} · ${fmtDate(p.date)} · ${String(p.hour).padStart(2, "0")}:00`;
    const risk = { bajo: ["low", "Riesgo bajo"], medio: ["mid", "Riesgo medio"], alto: ["high", "Riesgo alto"] }[p.risk];
    $("r-risk").className = `chip ${risk[0]}`;
    $("r-risk").textContent = risk[1];
    const color = { bajo: COLORS.green, medio: COLORS.amber, alto: COLORS.delayed }[p.risk];
    $("r-prob").textContent = pct(p.probability, 0);
    $("r-prob").style.color = color;
    const arc = $("r-arc");
    arc.style.stroke = color;
    arc.style.strokeDashoffset = String(326.7 * (1 - p.probability));
    const n10 = Math.round(p.probability * 10);
    $("r-summary").innerHTML = p.delayed
      ? `Es <strong>probable</strong> que llegue con más de 5 min de retraso: en situaciones parecidas, unos <strong>${n10} de cada 10</strong> trenes lo hicieron.`
      : `Lo más probable es que llegue <strong>a tiempo</strong> (≤ 5 min de retraso). En situaciones parecidas, unos ${n10} de cada 10 trenes llegaron tarde.`;
    const notes = [];
    if (!p.usual_service) notes.push(["warn", "warning", "Esta línea no suele pasar por la estación a esa hora en las últimas 4 semanas; la estimación es menos fiable."]);
    if (p.is_holiday) notes.push(["info", "event", "Es día festivo en el estado de la estación."]);
    $("r-notices").innerHTML = notes.map(([k, i, t]) => `<div class="notice ${k}"><span class="material-symbols-outlined">${i}</span><span>${t}</span></div>`).join("");
    $("r-thr").textContent = pct(p.threshold);
    $("r-hol").textContent = p.is_holiday ? "Sí" : "No";
    $("r-asof").textContent = p.data_as_of;
    $("r-ver").textContent = `v${p.model_version} · LightGBM`;

    const hours = h?.hours?.length ? h.hours : [{ hour: p.hour, probability: p.probability }];
    const top = Math.min(1, Math.ceil((Math.max(p.threshold, ...hours.map((x) => x.probability)) + 0.05) * 5) / 5);
    chart("hourly-chart", {
      grid: { left: 40, right: 44, top: 16, bottom: 28 },
      tooltip: { ...baseChart.tooltip, trigger: "axis", valueFormatter: (v) => pct(v) },
      xAxis: { type: "category", data: hours.map((x) => `${String(x.hour).padStart(2, "0")}h`), axisTick: { show: false }, axisLine: { lineStyle: { color: COLORS.border } } },
      yAxis: { type: "value", min: 0, max: top, interval: top <= 0.4 ? 0.1 : 0.2,
        axisLabel: { formatter: (v) => Math.round(v * 100) + "%" }, splitLine: { lineStyle: { color: COLORS.soft } } },
      series: [{
        type: "bar", barMaxWidth: 28, name: "Probabilidad",
        data: hours.map((x) => ({
          value: x.probability,
          itemStyle: { borderRadius: [4, 4, 0, 0], color: x.hour === p.hour ? COLORS.red : x.probability >= p.threshold ? "#fca5a5" : "#d4d4d8" },
        })),
        markLine: { symbol: "none", silent: true, label: { formatter: "umbral", position: "end", color: COLORS.muted, fontSize: 10 }, lineStyle: { type: "dashed", color: COLORS.muted }, data: [{ yAxis: p.threshold }] },
      }],
    });
  }

  // =====================================================================
  // Mapa de puntualidad
  // =====================================================================
  const mapState = { map: null, layer: null, metric: "pct_on_time", stations: [], markers: {} };
  const pctClass = (v) => (v >= 0.85 ? "pct-good" : v >= 0.75 ? "pct-mid" : "pct-bad");

  function metricScale(metric, values) {
    if (metric === "pct_on_time") {
      return { color: (v) => (v >= 0.85 ? COLORS.green : v >= 0.75 ? COLORS.amber : COLORS.delayed),
        legend: [[COLORS.green, "≥ 85 %"], [COLORS.amber, "75–85 %"], [COLORS.delayed, "< 75 %"]] };
    }
    const s = [...values].sort((a, b) => a - b);
    const t1 = s[Math.floor(s.length / 3)], t2 = s[Math.floor((2 * s.length) / 3)];
    return { color: (v) => (v < t1 ? COLORS.green : v < t2 ? COLORS.amber : COLORS.delayed),
      legend: [[COLORS.green, `< ${dec(t1)} min`], [COLORS.amber, `${dec(t1)}–${dec(t2)} min`], [COLORS.delayed, `≥ ${dec(t2)} min`]] };
  }

  async function initMap(params, first) {
    if (!first) return;
    try {
      const stations = await data("stations.json");
      mapState.stations = stations;
      const tot = stations.reduce((a, s) => a + s.n, 0);
      const wOn = stations.reduce((a, s) => a + s.pct_on_time * s.n, 0) / tot;
      const wDelay = stations.reduce((a, s) => a + s.avg_delay * s.n, 0) / tot;
      const wCanc = stations.reduce((a, s) => a + s.pct_canceled * s.n, 0) / tot;
      $("map-kpis").innerHTML = [
        ["green", "verified", "Puntualidad media", pct(wOn), "llegadas con ≤ 5 min de retraso"],
        ["amber", "timer", "Retraso medio", `${dec(wDelay)} min`, "ponderado por volumen de paradas"],
        ["red", "block", "Cancelaciones", pct(wCanc), "paradas planificadas canceladas"],
      ].map(([c, i, l, v, n]) => `<div class="card kpi"><div class="kpi-icon ${c}"><span class="material-symbols-outlined">${i}</span></div><span class="kpi-label">${l}</span><span class="kpi-value num">${v}</span><span class="kpi-note">${n}</span></div>`).join("");

      const map = L.map("map", { zoomControl: true, scrollWheelZoom: true, minZoom: 5 }).setView([51.1, 10.4], 6);
      L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", {
        attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>', maxZoom: 18,
      }).addTo(map);
      map.zoomControl.setPosition("topright");
      mapState.map = map;
      $("station-list").innerHTML = stations.map((s) => `<option value="${esc(s.name)}">`).join("");
      $("map-search").addEventListener("change", (e) => {
        const s = stations.find((x) => x.name.toLowerCase() === e.target.value.trim().toLowerCase());
        if (s) { map.setView([s.lat, s.lon], 10); showStation(s); }
      });
      document.querySelectorAll("#map-metric button").forEach((b) => b.addEventListener("click", () => {
        document.querySelectorAll("#map-metric button").forEach((x) => x.classList.toggle("active", x === b));
        mapState.metric = b.dataset.metric;
        drawMarkers();
      }));
      drawMarkers();
      renderRanking();
    } catch (e) { showDataError("view-mapa", e); }
  }

  function drawMarkers() {
    const { map, stations, metric } = mapState;
    if (mapState.layer) mapState.layer.remove();
    const scale = metricScale(metric, stations.map((s) => s[metric]));
    const maxN = Math.max(...stations.map((s) => s.n));
    const layer = L.layerGroup();
    [...stations].sort((a, b) => b.n - a.n).forEach((s) => {
      const m = L.circleMarker([s.lat, s.lon], {
        radius: 4 + 10 * Math.sqrt(s.n / maxN), color: "#fff", weight: 1.5, fillColor: scale.color(s[metric]), fillOpacity: 0.9,
      });
      m.bindTooltip(`<strong>${esc(s.name)}</strong><br>${pct(s.pct_on_time)} puntual · ${dec(s.avg_delay)} min`, { className: "st-tip", direction: "top", offset: [0, -6] });
      m.on("click", () => showStation(s));
      m.addTo(layer);
      mapState.markers[s.eva] = m;
    });
    layer.addTo(map);
    mapState.layer = layer;
    $("map-legend").innerHTML = scale.legend.map(([c, t]) => `<span><i style="background:${c}"></i>${t}</span>`).join("");
  }

  function showStation(s) {
    const box = $("map-station");
    box.classList.remove("hidden");
    box.innerHTML = `
      <div class="eyebrow" style="color:#ffb4aa">Estación · EVA ${esc(s.eva)}</div>
      <h3 style="margin:4px 0 12px">${esc(s.name)}</h3>
      <div class="row"><span>Puntualidad (≤ 5 min)</span><strong class="num">${pct(s.pct_on_time)}</strong></div>
      <div class="row"><span>Retraso medio</span><strong class="num">${dec(s.avg_delay)} min</strong></div>
      <div class="row"><span>Cancelaciones</span><strong class="num">${pct(s.pct_canceled)}</strong></div>
      <div class="row"><span>Paradas analizadas (30 días)</span><strong class="num">${approx(s.n)}</strong></div>
      <a class="btn primary" href="#/prediccion?eva=${encodeURIComponent(s.eva)}"><span class="material-symbols-outlined">online_prediction</span>Predecir en esta estación</a>`;
    requestAnimationFrame(() => mapState.map.invalidateSize());
  }

  function renderRanking() {
    const sorted = [...mapState.stations].sort((a, b) => b.pct_on_time - a.pct_on_time);
    const row = (s) => `<div class="rank-row" data-eva="${esc(s.eva)}"><span class="rank-name">${esc(s.name)}</span><span class="num ${pctClass(s.pct_on_time)}">${pct(s.pct_on_time)}</span></div>`;
    $("rank-best").innerHTML = sorted.slice(0, 5).map(row).join("");
    $("rank-worst").innerHTML = sorted.slice(-5).reverse().map(row).join("");
    document.querySelectorAll(".rank-row").forEach((r) => r.addEventListener("click", () => {
      const s = mapState.stations.find((x) => x.eva === r.dataset.eva);
      mapState.map.setView([s.lat, s.lon], 10);
      mapState.markers[s.eva]?.openTooltip();
      showStation(s);
    }));
  }

  // =====================================================================
  // Tendencias
  // =====================================================================
  const trend = { cat: "all", metric: "pct_on_time", monthly: [], heat: null };

  async function initTrends(params, first) {
    if (!first) return;
    try {
      [trend.monthly, trend.heat] = await Promise.all([data("trends_monthly.json"), data("heatmap.json")]);
      const bind = (sel, key) => document.querySelectorAll(`${sel} button`).forEach((b) => b.addEventListener("click", () => {
        document.querySelectorAll(`${sel} button`).forEach((x) => x.classList.toggle("active", x === b));
        trend[key] = b.dataset[key === "cat" ? "cat" : "metric"];
        renderTrends();
      }));
      bind("#trend-cat", "cat");
      bind("#trend-metric", "metric");
      renderTrends();
    } catch (e) { showDataError("view-tendencias", e); }
  }

  function monthlySeries(cat) {
    const rows = cat === "all" ? trend.monthly : trend.monthly.filter((r) => r.train_category === cat);
    const by = {};
    rows.forEach((r) => {
      const m = (by[r.month] ||= { n: 0, on: 0, delay: 0 });
      m.n += r.n; m.on += r.pct_on_time * r.n; m.delay += r.avg_delay * r.n;
    });
    return Object.keys(by).sort().map((month) => ({ month, n: by[month].n, pct_on_time: by[month].on / by[month].n, avg_delay: by[month].delay / by[month].n }));
  }

  function renderTrends() {
    const months = [...new Set(trend.monthly.map((r) => r.month))].sort();
    $("trend-range").textContent = `${monthLabel(months[0])} – ${monthLabel(months[months.length - 1])} · ${months.length} meses`;
    const isPct = trend.metric === "pct_on_time";
    const fmt = (v) => (isPct ? pct(v) : `${dec(v)} min`);

    // KPIs
    const agg = monthlySeries(trend.cat);
    const last = agg[agg.length - 1];
    const yearAgo = agg.find((m) => m.month === `${+last.month.slice(0, 4) - 1}${last.month.slice(4)}`);
    const best = agg.reduce((a, b) => (b.pct_on_time > a.pct_on_time ? b : a));
    const worst = agg.reduce((a, b) => (b.pct_on_time < a.pct_on_time ? b : a));
    const heatRows = trend.cat === "all" ? trend.heat.all : trend.heat.by_category[trend.cat] || [];
    const minN = 0.2 * Math.max(...heatRows.map((r) => r.n)); // descarta franjas nocturnas con pocas llegadas
    const peak = heatRows.filter((r) => r.n >= minN).reduce((a, b) => (b.rate > a.rate ? b : a), { rate: -1 });
    const diff = yearAgo ? (last.pct_on_time - yearAgo.pct_on_time) * 100 : null;
    $("trend-kpis").innerHTML = [
      ["blue", "calendar_month", `Puntualidad en ${monthLabel(last.month)}`, pct(last.pct_on_time),
        diff === null ? "sin dato del año anterior" : `${diff >= 0 ? "+" : "−"}${dec(Math.abs(diff))} pts vs. ${monthLabel(yearAgo.month)}`],
      ["green", "thumb_up", "Mejor y peor mes", `${monthLabel(best.month)}`, `${pct(best.pct_on_time)} · peor: ${monthLabel(worst.month)} (${pct(worst.pct_on_time)})`],
      ["red", "schedule", "Franja más conflictiva", peak.rate >= 0 ? `${DAYS[sparkDowToIdx(peak.dow)]} ${String(peak.hour).padStart(2, "0")}:00` : "—",
        peak.rate >= 0 ? `${pct(peak.rate)} de llegadas con > 5 min (últimas 12 semanas)` : ""],
    ].map(([c, i, l, v, n]) => `<div class="card kpi"><div class="kpi-icon ${c}"><span class="material-symbols-outlined">${i}</span></div><span class="kpi-label">${l}</span><span class="kpi-value num">${v}</span><span class="kpi-note">${n}</span></div>`).join("");

    // Serie mensual
    const cats = trend.cat === "all" ? Object.keys(CAT_COLORS) : [trend.cat];
    const series = cats.map((c) => {
      const byMonth = Object.fromEntries(trend.monthly.filter((r) => r.train_category === c).map((r) => [r.month, r[trend.metric]]));
      return { name: c, type: "line", smooth: true, symbolSize: 5, showSymbol: false, data: months.map((m) => byMonth[m] ?? null),
        lineStyle: { width: 2.5, color: CAT_COLORS[c] }, itemStyle: { color: CAT_COLORS[c] },
        areaStyle: cats.length === 1 ? { color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [{ offset: 0, color: CAT_COLORS[c] + "33" }, { offset: 1, color: CAT_COLORS[c] + "00" }]) } : undefined };
    });
    chart("monthly-chart", {
      grid: { left: 48, right: 16, top: 40, bottom: 32 },
      legend: { top: 0, left: 0, icon: "roundRect", itemWidth: 12, itemHeight: 4, textStyle: { color: COLORS.text, fontFamily: FONT } },
      tooltip: { ...baseChart.tooltip, trigger: "axis", valueFormatter: (v) => (v == null ? "—" : fmt(v)) },
      xAxis: { type: "category", data: months.map(monthLabel), boundaryGap: false, axisTick: { show: false }, axisLine: { lineStyle: { color: COLORS.border } } },
      yAxis: { type: "value", scale: true, splitNumber: 4, axisLabel: { formatter: (v) => (isPct ? Math.round(v * 100) + "%" : v + " min") }, splitLine: { lineStyle: { color: COLORS.soft } } },
      series,
    });

    // Mapa de calor
    $("heat-range").textContent = `Tasa de llegadas con más de 5 min de retraso · ${trend.heat.from} a ${trend.heat.to}`;
    const cells = heatRows.map((r) => [r.hour, sparkDowToIdx(r.dow), r.rate, r.n]);
    const rates = heatRows.filter((r) => r.n >= minN).map((r) => r.rate);
    chart("heat-chart", {
      grid: { left: 44, right: 16, top: 8, bottom: 64 },
      tooltip: { ...baseChart.tooltip, formatter: (p) => `${DAYS[p.value[1]]} ${String(p.value[0]).padStart(2, "0")}:00<br><strong>${pct(p.value[2])}</strong> con retraso<br>${approx(p.value[3])} llegadas` },
      xAxis: { type: "category", data: Array.from({ length: 24 }, (_, h) => String(h).padStart(2, "0")), splitArea: { show: false }, axisTick: { show: false }, axisLine: { show: false } },
      yAxis: { type: "category", data: DAYS, inverse: true, axisTick: { show: false }, axisLine: { show: false } },
      visualMap: { dimension: 2, min: Math.min(...rates), max: Math.max(...rates), calculable: false, orient: "horizontal", left: "center", bottom: 0, itemHeight: 220, itemWidth: 10,
        text: ["más retraso", "menos"], textStyle: { color: COLORS.muted, fontSize: 11 }, formatter: (v) => pct(v, 0),
        inRange: { color: ["#ecfdf5", "#fde68a", "#f59e0b", "#dc2626", "#7f1d1d"] } },
      series: [{ type: "heatmap", data: cells, itemStyle: { borderColor: "#fff", borderWidth: 2, borderRadius: 3 }, emphasis: { itemStyle: { borderColor: COLORS.text } } }],
    });
  }

  // =====================================================================
  // Sobre el modelo
  // =====================================================================
  const FEATURE_NAMES = {
    eva_hour_rate_28d: ["Tasa de retraso estación × hora (28 d)", "Histórico reciente de la estación a esa hora"],
    stop_seq: ["Posición de la parada en el recorrido", "Los retrasos se acumulan a lo largo del viaje"],
    eva: ["Estación", "Variable categórica (131 estaciones)"],
    route_rate_28d: ["Tasa de retraso de la línea (28 d)", "Histórico reciente de la línea en la estación"],
    route_rate_7d: ["Tasa de retraso de la línea (7 d)", "Tendencia de la última semana"],
    eva_rate_28d: ["Tasa de retraso de la estación (28 d)", "Histórico reciente de toda la estación"],
    eva_rate_7d: ["Tasa de retraso de la estación (7 d)", "Tendencia de la última semana en la estación"],
    route_n_28d: ["Volumen de la línea (28 d)", "Paradas observadas"],
    hour: ["Hora del día", ""], dow: ["Día de la semana", ""], month: ["Mes", "Estacionalidad"],
    is_weekend: ["Fin de semana", ""], is_holiday: ["Festivo", "Calendario por estado federal"],
    state_code: ["Estado federal", ""], train_category: ["Categoría de tren", "Larga distancia, Regional o S-Bahn"],
    train_type_grp: ["Tipo de tren", "ICE, IC, RE, RB, S…"],
  };
  const MODEL_NAMES = {
    baseline_route_rate_28d: ["Baseline: tasa histórica de la línea", "Regla sin entrenamiento"],
    spark_logistic_regression: ["Regresión logística (Spark ML)", "One-hot + escalado"],
    lightgbm: ["LightGBM", "Modelo en producción"],
  };

  async function initModel(params, first) {
    if (!first) return;
    try {
      const m = await data("model.json");
      const c = m.counts, sp = m.split;
      $("m-updated").textContent = `Datos hasta ${m.updated_at}`;
      const steps = [
        ["cloud_download", "Ingesta", "HuggingFace → S3", "Parquet mensual por estación"],
        ["inventory_2", "Bronze", `${approx(c.bronze_rows)} filas`, "Auto Loader, datos sin modificar"],
        ["cleaning_services", "Silver", `${approx(c.silver_rows)} filas`, "Duplicados, tipos, categorías"],
        ["auto_awesome", "Gold", `${approx(c.feature_rows)} filas`, `Variables sin fuga de datos · ${c.model_stations} estaciones`],
        ["model_training", "Entrenamiento", "LightGBM + MLflow", "Registro en Unity Catalog"],
        ["api", "Servicio", "FastAPI + Docker", "Render · OpenAPI / Swagger"],
      ];
      $("pipeline").innerHTML = steps.map(([i, t, b, s], k) =>
        `<div class="step"><div class="step-top"><div class="step-icon"><span class="material-symbols-outlined">${i}</span></div><span class="step-num">0${k + 1}</span></div><strong>${t}</strong><span class="num" style="font-size:13px;font-weight:600">${b}</span><small>${s}</small></div>`).join("");

      $("m-test").textContent = `Conjunto de prueba: ${sp.test.from} a ${sp.test.to} · ${approx(sp.test.rows)} llegadas · ${pct(sp.test.pos_rate)} con retraso`;
      const rows = Object.entries(MODEL_NAMES).map(([k, [name, sub]]) => {
        const r = m.metrics_test[k];
        return `<tr class="${k === "lightgbm" ? "best" : ""}"><td>${name}<div class="card-sub">${sub}</div></td>${["auc_pr", "auc_roc", "f1", "precision", "recall"].map((x) => `<td class="num">${dec(r[x], 3)}</td>`).join("")}</tr>`;
      });
      rows.push(`<tr><td class="muted">Referencia aleatoria</td><td class="num muted">${dec(sp.test.pos_rate, 3)}</td><td class="num muted">0,500</td><td class="num muted" colspan="3"></td></tr>`);
      $("m-table").innerHTML = rows.join("");
      const lift = (m.metrics_test.lightgbm.auc_pr / m.metrics_test.baseline_route_rate_28d.auc_pr - 1) * 100;
      $("m-metric-note").innerHTML = `Métrica principal: <strong>AUC-PR</strong>. Solo ~${Math.round(sp.test.pos_rate * 100)} % de las llegadas tienen retraso, así que la exactitud engaña; AUC-PR mide qué tan bien se detectan los retrasos. LightGBM mejora un <strong>${Math.round(lift)} %</strong> sobre el baseline. El umbral de ${dec(m.threshold, 2)} maximiza F1 en validación.`;

      const p = m.params;
      $("m-params-note").textContent = `${m.best_iteration} árboles (early stopping) · entrenado con ${approx(+p.train_rows)} filas muestreadas de ${approx(sp.train.rows)} · split temporal: entrenamiento ${sp.train.from.slice(0, 7)} → ${sp.train.to.slice(0, 7)}, validación ${sp.val.from.slice(0, 7)}, prueba ${sp.test.from.slice(0, 7)}.`;
      $("m-params").innerHTML = [["learning_rate", p.learning_rate], ["num_leaves", p.num_leaves], ["min_child_samples", p.min_child_samples], ["subsample", p.subsample], ["colsample_bytree", p.colsample_bytree], ["reg_lambda", p.reg_lambda]]
        .map(([k, v]) => `<div class="param"><span>${k}</span><strong>${esc(v)}</strong></div>`).join("");

      $("m-counts").innerHTML = [
        ["Estaciones en los datos", approx(c.stations_total)], ["Estaciones del modelo", int(c.model_stations)],
        ["Filas Bronze → Silver", `${approx(c.bronze_rows)} → ${approx(c.silver_rows)}`], ["Filas de entrenamiento (Gold)", approx(c.feature_rows)],
        ["Etiqueta", "retraso > 5 min"],
      ].map(([l, v]) => `<div style="display:flex;justify-content:space-between;gap:12px;font-size:13px"><span class="muted">${l}</span><strong class="num">${v}</strong></div>`).join("");

      const imp = m.importance.slice(0, 8);
      const maxGain = imp[0].gain;
      $("m-importance").innerHTML = imp.map((f) => {
        const [name, note] = FEATURE_NAMES[f.feature] || [f.feature, ""];
        return `<div class="bar-row"><div class="bar-row-head"><span>${esc(name)}</span><span class="num">${pct(f.gain)}</span></div><div class="bar"><div style="width:0" data-w="${(f.gain / maxGain) * 100}"></div></div><small>${esc(note || f.feature)}</small></div>`;
      }).join("");
      requestAnimationFrame(() => document.querySelectorAll("#m-importance .bar > div").forEach((d) => { d.style.width = d.dataset.w + "%"; }));

      chart("calib-chart", {
        grid: { left: 44, right: 24, top: 12, bottom: 40 },
        tooltip: { ...baseChart.tooltip, trigger: "item", formatter: (q) => q.seriesIndex === 1 ? `Predicho ${pct(q.value[0])}<br>Real ${pct(q.value[1])}<br>${approx(q.value[2])} llegadas` : "" },
        xAxis: { type: "value", min: 0, max: 1, name: "probabilidad predicha", nameLocation: "middle", nameGap: 26, axisLabel: { formatter: (v) => Math.round(v * 100) + "%" }, splitLine: { lineStyle: { color: COLORS.soft } } },
        yAxis: { type: "value", min: 0, max: 1, axisLabel: { formatter: (v) => Math.round(v * 100) + "%" }, splitLine: { lineStyle: { color: COLORS.soft } } },
        series: [
          { type: "line", data: [[0, 0], [1, 1]], showSymbol: false, silent: true, lineStyle: { type: "dashed", color: COLORS.muted, width: 1 } },
          { type: "line", data: m.calibration.map((b) => [b.prob_media, b.tasa_real, b.n]), symbolSize: 8, lineStyle: { color: COLORS.red, width: 2 }, itemStyle: { color: COLORS.red } },
        ],
      });
    } catch (e) { showDataError("view-modelo", e); }
  }

  // ---------------- Arranque ----------------
  data("meta.json").then((m) => { $("side-meta").textContent = `Datos hasta ${m.updated_at}`; }).catch(() => {});
  checkHealth();
  route();
})();
