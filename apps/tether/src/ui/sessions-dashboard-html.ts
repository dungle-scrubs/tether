/**
 * Self-contained operator dashboard for browsing existing Tether sessions.
 *
 * The markup, styles, and client script are embedded as one string so the live
 * server can serve the page without a bundler, static-asset copy step, or any
 * runtime file-system lookup. It talks to the existing read-only REST routes:
 * `GET /sessions`, and the per-session `debug/summary`, `debug/participants`,
 * `debug/tasks`, and `events` resources.
 */
export const sessionsDashboardHtml = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Tether Sessions</title>
    <style>
      :root {
        color-scheme: dark;
        --bg: #0b0e14;
        --bg-panel: #11151f;
        --bg-elev: #161b27;
        --bg-hover: #1c2230;
        --border: #232a39;
        --border-strong: #2e3850;
        --text: #e6e9ef;
        --text-dim: #9aa4b8;
        --text-faint: #5f6b82;
        --accent: #5b9dff;
        --accent-dim: #2a3f63;
        --green: #4ade80;
        --amber: #fbbf24;
        --red: #f87171;
        --purple: #c084fc;
        --mono: ui-monospace, "SF Mono", "JetBrains Mono", Menlo, monospace;
        --sans: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      }
      * { box-sizing: border-box; }
      html, body { height: 100%; margin: 0; }
      body {
        background: var(--bg);
        color: var(--text);
        font-family: var(--sans);
        font-size: 14px;
        line-height: 1.5;
        -webkit-font-smoothing: antialiased;
      }
      header {
        display: flex;
        align-items: center;
        gap: 16px;
        padding: 14px 22px;
        border-bottom: 1px solid var(--border);
        background: var(--bg-panel);
        position: sticky;
        top: 0;
        z-index: 5;
      }
      .brand { display: flex; align-items: baseline; gap: 10px; }
      .brand h1 { font-size: 16px; font-weight: 650; margin: 0; letter-spacing: 0.2px; }
      .brand .tag { color: var(--text-faint); font-size: 12px; }
      .count-pill {
        font-variant-numeric: tabular-nums;
        background: var(--accent-dim);
        color: #cfe0ff;
        border-radius: 999px;
        padding: 2px 10px;
        font-size: 12px;
        font-weight: 600;
      }
      .spacer { flex: 1; }
      .controls { display: flex; align-items: center; gap: 14px; }
      .toggle { display: flex; align-items: center; gap: 7px; color: var(--text-dim); font-size: 13px; cursor: pointer; user-select: none; }
      .toggle input { accent-color: var(--accent); width: 15px; height: 15px; }
      button {
        font-family: inherit;
        font-size: 13px;
        color: var(--text);
        background: var(--bg-elev);
        border: 1px solid var(--border-strong);
        border-radius: 7px;
        padding: 6px 13px;
        cursor: pointer;
        transition: background 0.12s, border-color 0.12s;
      }
      button:hover { background: var(--bg-hover); border-color: var(--accent-dim); }
      button:active { transform: translateY(1px); }
      .status-line { color: var(--text-faint); font-size: 12px; min-width: 120px; text-align: right; font-variant-numeric: tabular-nums; }

      main { display: grid; grid-template-columns: minmax(320px, 420px) 1fr; height: calc(100vh - 57px); }
      .list-pane { border-right: 1px solid var(--border); overflow-y: auto; background: var(--bg); }
      .detail-pane { overflow-y: auto; background: #0a0d13; }

      .session-card {
        padding: 13px 18px;
        border-bottom: 1px solid var(--border);
        cursor: pointer;
        transition: background 0.1s;
      }
      .session-card:hover { background: var(--bg-panel); }
      .session-card.active { background: var(--bg-elev); box-shadow: inset 3px 0 0 var(--accent); }
      .session-card .id-row { display: flex; align-items: center; gap: 8px; margin-bottom: 7px; }
      .session-card .sid { font-family: var(--mono); font-size: 13px; font-weight: 600; color: var(--text); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
      .session-card .when { color: var(--text-faint); font-size: 11.5px; white-space: nowrap; }
      .badges { display: flex; flex-wrap: wrap; gap: 6px; }
      .badge {
        display: inline-flex;
        align-items: center;
        gap: 5px;
        font-size: 11.5px;
        font-variant-numeric: tabular-nums;
        color: var(--text-dim);
        background: var(--bg-elev);
        border: 1px solid var(--border);
        border-radius: 6px;
        padding: 1px 7px;
      }
      .badge b { color: var(--text); font-weight: 600; }
      .badge.accent b { color: var(--accent); }
      .badge.green b { color: var(--green); }
      .chips { display: flex; flex-wrap: wrap; gap: 6px; margin-top: 7px; }
      .chip {
        font-size: 11px;
        font-family: var(--mono);
        color: var(--purple);
        background: rgba(192, 132, 252, 0.1);
        border: 1px solid rgba(192, 132, 252, 0.25);
        border-radius: 5px;
        padding: 1px 6px;
      }
      .archived-tag { font-size: 10.5px; color: var(--amber); border: 1px solid rgba(251, 191, 36, 0.35); border-radius: 5px; padding: 0 6px; text-transform: uppercase; letter-spacing: 0.4px; }

      .empty, .loading { padding: 40px 22px; color: var(--text-faint); text-align: center; font-size: 13px; }
      .detail-empty { display: flex; height: 100%; align-items: center; justify-content: center; color: var(--text-faint); font-size: 14px; }

      .detail-head { padding: 20px 26px 16px; border-bottom: 1px solid var(--border); position: sticky; top: 0; background: #0a0d13; z-index: 2; }
      .detail-head .sid { font-family: var(--mono); font-size: 17px; font-weight: 650; word-break: break-all; }
      .detail-head .meta { color: var(--text-faint); font-size: 12px; margin-top: 5px; display: flex; flex-wrap: wrap; gap: 14px; }
      .stat-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(108px, 1fr)); gap: 10px; padding: 18px 26px; }
      .stat {
        background: var(--bg-panel);
        border: 1px solid var(--border);
        border-radius: 9px;
        padding: 11px 13px;
      }
      .stat .label { font-size: 11px; color: var(--text-faint); text-transform: uppercase; letter-spacing: 0.5px; }
      .stat .value { font-size: 22px; font-weight: 650; font-variant-numeric: tabular-nums; margin-top: 3px; }
      .stat .value small { font-size: 13px; color: var(--text-dim); font-weight: 500; }

      section.block { padding: 6px 26px 22px; }
      section.block > h2 {
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 0.6px;
        color: var(--text-dim);
        margin: 18px 0 10px;
        display: flex;
        align-items: center;
        gap: 8px;
      }
      section.block > h2 .n { color: var(--text-faint); font-weight: 500; }

      .row {
        display: flex;
        align-items: center;
        gap: 10px;
        padding: 9px 12px;
        border: 1px solid var(--border);
        border-radius: 8px;
        margin-bottom: 7px;
        background: var(--bg-panel);
      }
      .row .mono { font-family: var(--mono); font-size: 12.5px; }
      .row .grow { flex: 1; min-width: 0; overflow: hidden; text-overflow: ellipsis; }
      .row .sub { color: var(--text-faint); font-size: 11.5px; }
      .dot { width: 8px; height: 8px; border-radius: 50%; flex: none; }
      .dot.green { background: var(--green); }
      .dot.amber { background: var(--amber); }
      .dot.red { background: var(--red); }
      .dot.gray { background: var(--text-faint); }
      .dot.blue { background: var(--accent); }
      .pill { font-size: 11px; padding: 1px 8px; border-radius: 999px; white-space: nowrap; }
      .pill.green { color: var(--green); background: rgba(74, 222, 128, 0.12); }
      .pill.amber { color: var(--amber); background: rgba(251, 191, 36, 0.12); }
      .pill.red { color: var(--red); background: rgba(248, 113, 113, 0.12); }
      .pill.gray { color: var(--text-dim); background: var(--bg-elev); }
      .pill.blue { color: var(--accent); background: rgba(91, 157, 255, 0.12); }

      .event { display: flex; gap: 11px; padding: 7px 12px; border-left: 2px solid var(--border-strong); margin-bottom: 4px; }
      .event .seq { font-family: var(--mono); color: var(--text-faint); font-size: 11.5px; min-width: 38px; text-align: right; }
      .event .etype { font-family: var(--mono); font-size: 12px; color: var(--accent); min-width: 150px; }
      .event .epayload { font-family: var(--mono); font-size: 11.5px; color: var(--text-dim); flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
      .event .etime { color: var(--text-faint); font-size: 11px; white-space: nowrap; }

      .muted { color: var(--text-faint); font-size: 12.5px; padding: 4px 2px; }
      .err { color: var(--red); padding: 16px 26px; font-family: var(--mono); font-size: 12.5px; }
      ::-webkit-scrollbar { width: 10px; height: 10px; }
      ::-webkit-scrollbar-thumb { background: #222a39; border-radius: 6px; border: 2px solid transparent; background-clip: padding-box; }
      ::-webkit-scrollbar-thumb:hover { background: #303a4f; background-clip: padding-box; }
    </style>
  </head>
  <body>
    <header>
      <div class="brand">
        <h1>Tether</h1>
        <span class="tag">sessions</span>
        <span class="count-pill" id="count">0</span>
      </div>
      <div class="spacer"></div>
      <div class="controls">
        <span class="status-line" id="status"></span>
        <label class="toggle"><input type="checkbox" id="auto" /> Auto-refresh</label>
        <button id="refresh">Refresh</button>
      </div>
    </header>
    <main>
      <div class="list-pane" id="list"><div class="loading">Loading sessions…</div></div>
      <div class="detail-pane" id="detail"><div class="detail-empty">Select a session to inspect it.</div></div>
    </main>

    <script type="module">
      const state = { sessions: [], selected: null, autoTimer: null };

      const esc = (v) =>
        String(v).replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));

      async function getJson(path) {
        const res = await fetch(path, { headers: { accept: "application/json" } });
        if (!res.ok) throw new Error(path + " -> " + res.status);
        return res.json();
      }

      function relTime(iso) {
        if (!iso) return "never";
        const ms = Date.now() - new Date(iso).getTime();
        if (ms < 0) return "just now";
        const s = Math.floor(ms / 1000);
        if (s < 45) return s + "s ago";
        const m = Math.floor(s / 60);
        if (m < 45) return m + "m ago";
        const h = Math.floor(m / 60);
        if (h < 36) return h + "h ago";
        const d = Math.floor(h / 24);
        if (d < 14) return d + "d ago";
        return new Date(iso).toLocaleDateString();
      }

      function absTime(iso) {
        return iso ? new Date(iso).toLocaleString() : "—";
      }

      async function loadSessions() {
        const status = document.getElementById("status");
        try {
          const { sessions } = await getJson("/sessions");
          state.sessions = sessions ?? [];
          document.getElementById("count").textContent = state.sessions.length;
          renderList();
          status.textContent = "updated " + new Date().toLocaleTimeString();
        } catch (e) {
          status.textContent = "error";
          document.getElementById("list").innerHTML =
            '<div class="err">Failed to load sessions: ' + esc(e.message) + "</div>";
        }
      }

      function renderList() {
        const el = document.getElementById("list");
        if (state.sessions.length === 0) {
          el.innerHTML = '<div class="empty">No sessions yet.</div>';
          return;
        }
        el.innerHTML = state.sessions
          .map((s) => {
            const active = s.sessionId === state.selected ? " active" : "";
            const chips = (s.bindings ?? [])
              .map((b) => '<span class="chip">' + esc(b.provider) + ":" + esc(b.externalId) + "</span>")
              .join("");
            return (
              '<div class="session-card' + active + '" data-id="' + esc(s.sessionId) + '">' +
              '<div class="id-row"><span class="sid">' + esc(s.sessionId) + "</span>" +
              '<span class="spacer" style="flex:1"></span>' +
              '<span class="when">' + relTime(s.lastEventAt ?? s.createdAt) + "</span></div>" +
              '<div class="badges">' +
              '<span class="badge accent"><b>' + s.participantCount + "</b> part</span>" +
              '<span class="badge green"><b>' + s.activeTaskCount + "</b>/" + s.taskCount + " tasks</span>" +
              '<span class="badge"><b>' + s.eventCount + "</b> events</span>" +
              "</div>" +
              (chips ? '<div class="chips">' + chips + "</div>" : "") +
              "</div>"
            );
          })
          .join("");
        for (const card of el.querySelectorAll(".session-card")) {
          card.addEventListener("click", () => selectSession(card.dataset.id));
        }
      }

      async function selectSession(id) {
        state.selected = id;
        renderList();
        const detail = document.getElementById("detail");
        detail.innerHTML = '<div class="loading">Loading ' + esc(id) + "…</div>";
        try {
          const [summary, participants, tasks, events] = await Promise.all([
            getJson("/sessions/" + encodeURIComponent(id) + "/debug/summary").then((r) => r.summary),
            getJson("/sessions/" + encodeURIComponent(id) + "/debug/participants").then((r) => r.participants),
            getJson("/sessions/" + encodeURIComponent(id) + "/debug/tasks").then((r) => r.tasks),
            getJson("/sessions/" + encodeURIComponent(id) + "/events?limit=500").then((r) => r.events),
          ]);
          if (state.selected !== id) return;
          renderDetail(id, summary, participants ?? [], tasks ?? [], events ?? []);
        } catch (e) {
          detail.innerHTML = '<div class="err">Failed to load session: ' + esc(e.message) + "</div>";
        }
      }

      const participantTone = {
        registered_control_active: "green",
        registered_control_inactive: "amber",
        registered_without_control: "blue",
        lease_without_presence: "gray",
      };
      const taskTone = {
        claim_active: "blue",
        completed: "green",
        unclaimed: "amber",
        claim_expired: "amber",
        claim_cleared: "gray",
        failed: "red",
        cancelled: "gray",
      };

      function stat(label, value, sub) {
        return (
          '<div class="stat"><div class="label">' + label + '</div><div class="value">' +
          value + (sub ? " <small>" + sub + "</small>" : "") + "</div></div>"
        );
      }

      function renderDetail(id, summary, participants, tasks, events) {
        const sess = state.sessions.find((s) => s.sessionId === id);
        const t = summary?.tasks ?? {};
        const p = summary?.participants ?? {};
        const l = summary?.controlLeases ?? {};

        const head =
          '<div class="detail-head"><div class="sid">' + esc(id) + "</div>" +
          '<div class="meta"><span>created ' + absTime(sess?.createdAt) + "</span>" +
          '<span>last event ' + relTime(sess?.lastEventAt ?? sess?.createdAt) + "</span>" +
          "</div></div>";

        const stats =
          '<div class="stat-grid">' +
          stat("Participants", p.total ?? 0, (p.activeControl ?? 0) + " active") +
          stat("Tasks", t.total ?? 0, (t.unclaimed ?? 0) + " open") +
          stat("Active claims", t.activeClaims ?? 0) +
          stat("Completed", t.completed ?? 0) +
          stat("Failed", (t.failed ?? 0) + (t.cancelled ?? 0)) +
          stat("Leases", l.active ?? 0, "active") +
          stat("Events", sess?.eventCount ?? events.length) +
          "</div>";

        const partRows = participants.length
          ? participants
              .map((pr) => {
                const tone = participantTone[pr.status] ?? "gray";
                const name = pr.participant?.displayName ?? pr.participantId;
                const kind = pr.participant?.runtimeKind ?? "no presence";
                const lease = pr.currentControlLease ?? pr.latestControlLease;
                const seen = pr.participant?.lastSeenAt ?? lease?.lastSeenAt;
                return (
                  '<div class="row"><span class="dot ' + tone + '"></span>' +
                  '<div class="grow"><div class="mono">' + esc(name) + "</div>" +
                  '<div class="sub">' + esc(kind) + (lease ? " · " + esc(lease.controlChannel) : "") + "</div></div>" +
                  '<span class="sub">' + relTime(seen) + "</span>" +
                  '<span class="pill ' + tone + '">' + pr.status.replace(/_/g, " ") + "</span></div>"
                );
              })
              .join("")
          : '<div class="muted">No participants.</div>';

        const taskRows = tasks.length
          ? tasks
              .map((tk) => {
                const tone = taskTone[tk.status] ?? "gray";
                return (
                  '<div class="row"><span class="dot ' + tone + '"></span>' +
                  '<div class="grow"><div class="mono">' + esc(tk.objective || tk.taskId) + "</div>" +
                  '<div class="sub">' + esc(tk.kind) + " · " + esc(tk.taskId) +
                  (tk.claimedBy ? " · " + esc(tk.claimedBy) : "") + "</div></div>" +
                  '<span class="pill ' + tone + '">' + tk.status.replace(/_/g, " ") + "</span></div>"
                );
              })
              .join("")
          : '<div class="muted">No tasks.</div>';

        const recent = events.slice(-60).reverse();
        const eventRows = recent.length
          ? recent
              .map((ev) => {
                let payload = "";
                try {
                  payload = JSON.stringify(ev.payload);
                } catch {
                  payload = "";
                }
                return (
                  '<div class="event"><span class="seq">#' + ev.seq + "</span>" +
                  '<span class="etype">' + esc(ev.type) + "</span>" +
                  '<span class="epayload">' + esc(payload.slice(0, 240)) + "</span>" +
                  '<span class="etime">' + relTime(ev.createdAt) + "</span></div>"
                );
              })
              .join("")
          : '<div class="muted">No events.</div>';

        document.getElementById("detail").innerHTML =
          head + stats +
          '<section class="block"><h2>Participants <span class="n">' + participants.length + "</span></h2>" + partRows + "</section>" +
          '<section class="block"><h2>Tasks <span class="n">' + tasks.length + "</span></h2>" + taskRows + "</section>" +
          '<section class="block"><h2>Recent events <span class="n">' + recent.length + " of " + events.length + "</span></h2>" + eventRows + "</section>";
      }

      function setAuto(on) {
        if (state.autoTimer) {
          clearInterval(state.autoTimer);
          state.autoTimer = null;
        }
        if (on) {
          state.autoTimer = setInterval(() => {
            loadSessions();
            if (state.selected) selectSession(state.selected);
          }, 5000);
        }
      }

      document.getElementById("refresh").addEventListener("click", () => {
        loadSessions();
        if (state.selected) selectSession(state.selected);
      });
      document.getElementById("auto").addEventListener("change", (e) => setAuto(e.target.checked));

      loadSessions();
    </script>
  </body>
</html>
`;
