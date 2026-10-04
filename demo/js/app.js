/* Brownsville demo. Data comes from data/*.js, written by scripts/build_demo_data.py. */
(function () {
  "use strict";
  const D = window.DEMO;
  const $ = (id) => document.getElementById(id);
  const el = (tag, attrs, ...kids) => {
    const n = document.createElement(tag);
    for (const [k, v] of Object.entries(attrs || {})) {
      if (k === "class") n.className = v; else n.setAttribute(k, v);
    }
    for (const kid of kids) n.append(kid);
    return n;
  };
  const feet = (m, digits = 1) => `${Math.round(m * 3.28084)} ft (${m.toFixed(digits)} m)`;
  const reduced = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

  // One style table for the map and the legend, so they cannot drift apart.
  // Every class differs in line pattern or shape as well as in colour.
  const LINE = {
    flat: { color: "#46627a", weight: 2, label: "Sidewalk or path, up to 5% incline" },
    slope: { color: "#8a5200", weight: 3.5, dashArray: "7 5", label: "Sidewalk or path, 5% to 8.3%" },
    steep: { color: "#a3172b", weight: 4.5, dashArray: "1 6", lineCap: "round", label: "Sidewalk or path, over 8.3%" },
    unknown: { color: "#6b6b6b", weight: 1.5, dashArray: "2 3", label: "Sidewalk or path, no incline value" },
    steps: { color: "#111111", weight: 5, dashArray: "2 3", lineCap: "butt", label: "Steps" },
    ramped: { color: "#0f6b5c", weight: 3.5, label: "Crossing with a surveyed ramp near both ends" },
    unramped: { color: "#8c1d78", weight: 3.5, dashArray: "4 5", label: "Crossing with no surveyed ramp near one or both ends" },
  };
  // Ramp colours are not used by any line, all have at least 3:1 contrast with the map background, and each
  // marker gets a white outline so it stands out where it sits on a line. Compliant is a neutral dark slate,
  // not a "go" colour.
  const RAMP = [
    { shape: "cross", color: "#6e6e6e", label: "Surveyed ramp with no DOT assessment" },
    { shape: "circle", color: "#26323c", label: "DOT label: Compliant" },
    { shape: "square", color: "#b05a00", label: "DOT label: Pending Technical Review (not decided yet)" },
    { shape: "triangle", color: "#c4001d", label: "DOT label: Non-Compliant" },
  ];
  const REBUILT = { shape: "diamond", color: "#6b3fa0", label: "Corner rebuilt after the survey (survey values may be out of date)" };
  const ROUTE = {
    ours: { color: "#0050a0", weight: 6, label: "This graph, wheelchair profile" },
    ors_wheelchair: { color: "#c2410c", weight: 5, dashArray: "11 7", label: "OpenRouteService wheelchair, plain OSM" },
    ors_foot: { color: "#1b1f23", weight: 4, dashArray: "2 8", lineCap: "round", label: "OpenRouteService walking, plain OSM" },
  };

  /* ---------- map ---------- */
  // One canvas, no larger than the map: a padded canvas spills under the page around the map.
  const canvas = L.canvas({ padding: 0, tolerance: 6 });
  const map = L.map("map", {
    renderer: canvas, zoomAnimation: !reduced, fadeAnimation: !reduced, markerZoomAnimation: !reduced,
    zoomSnap: 0.5, minZoom: 13, maxZoom: 20, attributionControl: true, zoomControl: false,
  });
  L.control.zoom({ zoomInTitle: "Zoom in", zoomOutTitle: "Zoom out", zoomOutText: "-" }).addTo(map);
  map.attributionControl.setPrefix('<a href="https://leafletjs.com">Leaflet</a>');
  map.attributionControl.addAttribution('&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap contributors</a>');
  const bounds = L.latLngBounds(D.meta.bounds);
  map.fitBounds(bounds);
  map.setMaxBounds(bounds.pad(2));    // room to pan at the widest zoom
  L.control.scale({ imperial: true, metric: true }).addTo(map);
  D.map = map;    // for the accessibility checks

  const pane = (name, z) => { map.createPane(name).style.zIndex = z; return name; };
  const routeCanvas = L.canvas({ pane: pane("routes", 450), padding: 0 });

  L.polygon(D.meta.boundary, { renderer: canvas, color: "#1b1f23", weight: 2, dashArray: "12 6", fill: false, interactive: false }).addTo(map);

  const streets = L.layerGroup();
  for (const [coords] of D.network.streets) {
    L.polyline(coords, { renderer: canvas, color: "#c4bfb3", weight: 5, interactive: false }).addTo(map);
  }
  for (const [name, lat, lon] of D.network.street_labels) {
    L.tooltip({ permanent: true, direction: "center", className: "street-label", interactive: false, opacity: 1 })
      .setLatLng([lat, lon]).setContent(name).addTo(streets);
  }

  const inclineClass = (pct) => pct === null ? "unknown" : pct > 8.3 ? "steep" : pct > 5 ? "slope" : "flat";
  const sidewalks = L.layerGroup();
  for (const [coords, pct, width, beside] of D.network.walks) {
    const s = LINE[inclineClass(pct)];
    L.polyline(coords, { renderer: canvas, ...s }).bindPopup(
      `<strong>${beside ? "Sidewalk beside " + beside : "Sidewalk or path"}</strong><br>` +
      `Steepest incline: ${pct === null ? "not known" : pct.toFixed(1) + "%"}<br>` +
      `Average mapped width: ${width === null ? "not known" : feet(width) + ", curb to building line. The clear path is narrower."}`).addTo(sidewalks);
  }
  for (const [coords] of D.network.steps) {
    L.polyline(coords, { renderer: canvas, ...LINE.steps }).bindPopup("<strong>Steps</strong>").addTo(sidewalks);
  }
  const crossings = L.layerGroup();
  for (const [coords, ramped, over] of D.network.crossings) {
    L.polyline(coords, { renderer: canvas, ...(ramped ? LINE.ramped : LINE.unramped) }).bindPopup(
      `<strong>Crossing${over ? " over " + over : ""}</strong><br>` +
      (ramped ? "A surveyed ramp lies within 5 m of both ends." : "No surveyed ramp within 5 m of one or both ends. The wheelchair profile does not cross here.")).addTo(crossings);
  }

  // Ramp markers: a shape per DOT status, drawn on the canvas.
  const drawShape = (ctx, shape, x, y, r) => {
    ctx.beginPath();
    if (shape === "circle") ctx.arc(x, y, r, 0, Math.PI * 2);
    else if (shape === "square") ctx.rect(x - r * 0.9, y - r * 0.9, r * 1.8, r * 1.8);
    else if (shape === "triangle") { ctx.moveTo(x, y - r * 1.15); ctx.lineTo(x + r * 1.1, y + r * 0.85); ctx.lineTo(x - r * 1.1, y + r * 0.85); ctx.closePath(); }
    else if (shape === "diamond") { ctx.moveTo(x, y - r * 1.3); ctx.lineTo(x + r * 1.3, y); ctx.lineTo(x, y + r * 1.3); ctx.lineTo(x - r * 1.3, y); ctx.closePath(); }
    else { ctx.moveTo(x - r, y - r); ctx.lineTo(x + r, y + r); ctx.moveTo(x + r, y - r); ctx.lineTo(x - r, y + r); }
  };
  const ShapeMarker = L.CircleMarker.extend({
    _updatePath: function () {
      const ctx = this._renderer._ctx, p = this._point, r = Math.max(this._radius, 1);
      if (!this._renderer._drawing || this._empty()) return;
      drawShape(ctx, this.options.shape, p.x, p.y, r);
      ctx.lineWidth = this.options.weight + 3;
      ctx.strokeStyle = "#ffffff";
      ctx.globalAlpha = 1;
      ctx.stroke();
      this._renderer._fillStroke(ctx, this);
    },
  });
  const STATUS_TEXT = ["no DOT assessment", "Compliant", "Pending Technical Review", "Non-Compliant"];
  const ramps = L.layerGroup();
  for (const [lat, lon, status, rebuilt, surveyed, slope, corner, builtYear] of D.ramps) {
    const s = rebuilt ? REBUILT : RAMP[status];
    new ShapeMarker([lat, lon], {
      renderer: canvas, shape: s.shape, radius: 5, color: s.color, weight: rebuilt || s.shape === "cross" ? 2.5 : 1,
      fillColor: s.color, fillOpacity: rebuilt || s.shape === "cross" ? 0 : 0.9,
    }).bindPopup(
      `<strong>Curb ramp at ${corner}</strong><br>Surveyed ${surveyed}. DOT's assessment of that survey: ${STATUS_TEXT[status]}.<br>` +
      `Running slope in the survey: ${slope === null ? "not recorded" : slope.toFixed(1) + "%"}.<br>` +
      (rebuilt ? `<strong>DOT lists this corner as rebuilt${builtYear ? " in " + builtYear : ""}, after the survey.</strong> The values above may describe a ramp that has since been replaced.` : "DOT does not list this corner as rebuilt since the survey.")).addTo(ramps);
  }

  const zoomRadius = () => {
    const r = map.getZoom() >= 18 ? 7 : map.getZoom() >= 16.5 ? 5 : 3;
    ramps.eachLayer((m) => m.setRadius(r));
  };
  map.on("zoomend", zoomRadius);

  // Street labels must not sit on top of each other: keep the first of any that collide.
  const declutter = () => {
    const box = map.getContainer().getBoundingClientRect();
    const kept = [...map.getContainer().querySelectorAll(".leaflet-control")].map((c) => c.getBoundingClientRect());
    streets.eachLayer((t) => {
      const e = t.getElement();
      if (!e) return;
      e.style.display = "";
      const r = e.getBoundingClientRect();
      const outside = r.left < box.left || r.right > box.right || r.top < box.top || r.bottom > box.bottom;
      if (outside || kept.some((k) => r.left < k.right + 4 && r.right > k.left - 4 && r.top < k.bottom + 2 && r.bottom > k.top - 2)) e.style.display = "none";
      else kept.push(r);
    });
  };
  map.on("zoomend moveend", declutter);

  const toggles = { "show-sidewalks": sidewalks, "show-crossings": crossings, "show-ramps": ramps, "show-streets": streets };
  for (const [id, layer] of Object.entries(toggles)) {
    layer.addTo(map);
    $(id).addEventListener("change", (e) => { if (e.target.checked) { layer.addTo(map); declutter(); } else layer.remove(); });
  }
  zoomRadius();
  declutter();

  /* ---------- legend ---------- */
  const NS = "http://www.w3.org/2000/svg";
  const svg = (inner) => {
    const s = document.createElementNS(NS, "svg");
    s.setAttribute("viewBox", "0 0 40 16"); s.setAttribute("width", "40"); s.setAttribute("height", "16");
    s.setAttribute("aria-hidden", "true"); s.setAttribute("focusable", "false");
    s.innerHTML = inner;
    return s;
  };
  const lineSample = (s) => svg(`<line x1="2" y1="8" x2="38" y2="8" stroke="${s.color}" stroke-width="${s.weight}" stroke-dasharray="${s.dashArray || ""}" stroke-linecap="${s.lineCap || "butt"}"/>`);
  const shapeSample = (s, hollow) => {
    const f = hollow ? `fill="none" stroke="${s.color}" stroke-width="2.5"` : `fill="${s.color}"`;
    const shapes = {
      circle: `<circle cx="20" cy="8" r="6" ${f}/>`, square: `<rect x="14" y="2" width="12" height="12" ${f}/>`,
      triangle: `<path d="M20 1 L27 14 L13 14 Z" ${f}/>`, diamond: `<path d="M20 1 L27 8 L20 15 L13 8 Z" ${f}/>`,
      cross: `<path d="M15 3 L25 13 M25 3 L15 13" stroke="${s.color}" stroke-width="2.5"/>`,
    };
    return svg(shapes[s.shape]);
  };
  const legend = $("legend");
  legend.append(el("h3", {}, "Sidewalks, paths and crossings"));
  for (const s of Object.values(LINE)) legend.append(el("div", {}, lineSample(s), el("span", {}, s.label)));
  legend.append(el("h3", {}, "Curb ramps, by NYC DOT's assessment of its survey"));
  for (const s of RAMP.slice(1).concat(RAMP.slice(0, 1))) legend.append(el("div", {}, shapeSample(s, s.shape === "cross"), el("span", {}, s.label)));
  legend.append(el("div", {}, shapeSample(REBUILT, true), el("span", {}, REBUILT.label)));
  legend.append(el("h3", {}, "Routes for the selected trip"));
  for (const s of Object.values(ROUTE)) legend.append(el("div", {}, lineSample(s), el("span", {}, s.label)));

  /* ---------- routes ---------- */
  const routeLayer = L.layerGroup().addTo(map);
  const select = $("route-select");
  D.routes.forEach((r, i) => select.append(el("option", { value: String(i) }, `${r.from.name} (${r.from.kind}) to ${r.to.name} (${r.to.kind})`)));

  const fmt = (m) => m >= 400 ? `${(m / 1609.344).toFixed(2)} mi (${(m / 1000).toFixed(2)} km)` : feet(m, 0);
  const showRoute = (i, move) => {
    const r = D.routes[i];
    routeLayer.clearLayers();
    const all = [];
    for (const key of ["ors_foot", "ors_wheelchair", "ours"]) {
      const o = r.options[key];
      if (!o.found) continue;
      L.polyline(o.coords, { renderer: routeCanvas, color: "#ffffff", weight: ROUTE[key].weight + 3, opacity: 0.9, interactive: false }).addTo(routeLayer);
      L.polyline(o.coords, { renderer: routeCanvas, ...ROUTE[key], interactive: false }).addTo(routeLayer);
      all.push(...o.coords);
    }
    for (const [letter, p, name] of [["A", r.from, "Start"], ["B", r.to, "End"]]) {
      L.marker([p.lat, p.lon], { keyboard: false, title: `${name}: ${p.name}`, alt: `${name}: ${p.name}`,
        icon: L.divIcon({ className: `end-marker end-${letter.toLowerCase()}`, iconSize: [28, 28], html: "" }) }).addTo(routeLayer);
      all.push([p.lat, p.lon]);
    }
    if (move) map.fitBounds(L.latLngBounds(all).pad(0.15), { animate: !reduced });

    const verdict = $("route-verdict");
    verdict.replaceChildren(el("p", {}, el("strong", {}, r.verdict.headline)), el("p", {}, r.verdict.reason));
    const cards = $("route-cards");
    cards.replaceChildren();
    for (const key of ["ours", "ors_wheelchair", "ors_foot"]) {
      const o = r.options[key], s = ROUTE[key];
      const card = el("article", { class: "card", style: `border-top-color:${s.color}` });
      card.append(el("h3", {}, lineSample(s), el("span", {}, s.label)));
      if (!o.found) {
        card.append(el("p", { class: "none" }, o.note || "No route found."));
      } else {
        const facts = el("ul", { class: "facts" });
        facts.append(el("li", {}, `Length: ${fmt(o.length_m)}`));
        facts.append(el("li", {}, `Crossings: ${o.crossings}`));
        const bad = el("li", {}, `Crossings with no surveyed ramp near an end: ${o.no_ramp}`);
        if (o.no_ramp) bad.className = "flag";
        facts.append(bad);
        facts.append(el("li", {}, `Steepest stretch: ${o.steepest === null ? "not known" : o.steepest.toFixed(1) + "%"}`));
        if (o.steps) facts.append(el("li", { class: "flag" }, `Flights of steps: ${o.steps}`));
        if (o.street_m > 10) facts.append(el("li", { class: "flag" }, `In the roadway, where no sidewalk is mapped: ${fmt(o.street_m)}`));
        if (o.rebuilt) facts.append(el("li", {}, `Crossing ends at corners rebuilt after the survey: ${o.rebuilt}`));
        card.append(facts, el("h4", {}, "Step by step"));
        const ol = el("ol");
        for (const step of o.steps_text) ol.append(el("li", {}, step));
        card.append(ol);
      }
      cards.append(card);
    }
  };
  let shown = 0;
  const onSelect = () => { const i = Number(select.value); if (i !== shown) { shown = i; showRoute(i, true); } };
  select.addEventListener("change", onSelect);
  select.addEventListener("input", onSelect);
  showRoute(0, false);

  /* ---------- tables ---------- */
  const table = (caption, head, rows) => {
    const t = el("table");
    t.append(el("caption", {}, caption));
    const tr = el("tr");
    head.forEach((h, i) => tr.append(el("th", { scope: "col", class: i ? "num" : "" }, h)));
    t.append(el("thead", {}, tr));
    const body = el("tbody");
    for (const row of rows) {
      const r = el("tr");
      row.forEach((c, i) => r.append(i ? el("td", { class: "num" }, String(c)) : el("th", { scope: "row" }, String(c))));
      body.append(r);
    }
    t.append(body);
    return t;
  };
  const net = $("network-tables");
  for (const t of D.meta.network_tables) net.append(table(t.caption, t.head, t.rows));

  $("ramp-dates").textContent = D.meta.ramp_dates;
  $("ramp-table").append(table(D.meta.ramp_table.caption, D.meta.ramp_table.head, D.meta.ramp_table.rows));

  const tbody = document.querySelector("#corner-table tbody");
  const LIMIT = 60;
  const renderCorners = () => {
    const q = $("corner-filter").value.trim().toLowerCase();
    const hits = D.meta.corners.filter((c) => !q || c[0].toLowerCase().includes(q));
    tbody.replaceChildren();
    for (const [name, n, assessed, since] of hits.slice(0, LIMIT)) {
      tbody.append(el("tr", {}, el("th", { scope: "row" }, name), el("td", { class: "num" }, String(n)), el("td", {}, assessed), el("td", {}, since)));
    }
    $("corner-count").textContent = hits.length > LIMIT
      ? `Showing the first ${LIMIT} of ${hits.length} corners. Type a street name to narrow the list.`
      : `${hits.length} ${hits.length === 1 ? "corner" : "corners"}.`;
  };
  $("corner-filter").addEventListener("input", renderCorners);
  renderCorners();

  /* ---------- status and sources ---------- */
  const status = el("div", { class: "status-grid" });
  for (const block of D.meta.status) {
    const ul = el("ul");
    for (const item of block.items) ul.append(el("li", {}, item));
    status.append(el("div", {}, el("h3", {}, block.title), ul));
  }
  $("status-body").append(status);

  const src = el("table");
  src.append(el("caption", {}, "Data and software on this page"));
  src.append(el("thead", {}, el("tr", {}, el("th", { scope: "col" }, "What"), el("th", { scope: "col" }, "Source and date"), el("th", { scope: "col" }, "Licence or terms"))));
  const sb = el("tbody");
  for (const [what, source, url, licence] of D.meta.sources) {
    sb.append(el("tr", {}, el("th", { scope: "row" }, what), el("td", {}, el("a", { href: url }, source)), el("td", {}, licence)));
  }
  src.append(sb);
  $("sources-body").append(src);
})();
