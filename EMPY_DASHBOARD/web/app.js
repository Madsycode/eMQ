import { Element, NumberText, Bytes, ByteText, Duration, Totals, Topology, Mesh, BrokerRows, EMPY_BROKER_COLUMNS, EMPY_GRAPH_LIMIT } from "./model.js";
import { Sparkline, TrafficChart } from "./charts.js";
import { NetworkGraph } from "./topology.js";

const EMPY_VIEWS =
{
    overview: ["Network overview", "A live view of your brokers, connections and message flow."],
    brokers: ["Broker inventory", "Availability, load, peer links and resource use across your fabric."],
    sessions: ["Client sessions", "Inspect connected clients, resumable state and in-flight exchanges."],
    activity: ["Network activity", "Recent changes observed by the monitoring collector."]
};
const EMPY_PAGE_SIZE = 25;

const Get = id => document.getElementById(id);
const Text = (id, value) => { Get(id).textContent = value; };
const Title = value => value.charAt(0).toUpperCase() + value.slice(1);
const Time = value => new Date(value).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
const Rate = value => value == null ? "—" : `${NumberText(value)} msg/s`;
let m_State = null;
let m_Error = "";
let m_LastReceived = 0;
let m_Paused = false;
let m_InFlight = false;
let m_RequestVersion = 0;
let m_SelectedBroker = null;
let m_SelectedSession = null;
let m_Page = 0;
let m_View = "overview";
let m_FilterKey = "";
let m_Sessions = null;
let m_Preview = null;
let m_Detail = undefined;
let m_SessionVersion = 0;
let m_BrokerSort = { sort: "name", descending: false };
const graph = new NetworkGraph(Get("topology"), SelectBroker);

function Sources()
{
    const sources = m_State?.sources || [];
    return m_Error ? sources.map(source => ({ ...source, state: source.data ? "stale" : "unreachable", rates: null })) : sources;
}

function Badge(text, state = "") { return Element("span", `badge ${state}`, text); }

function Empty(message, detail = "")
{
    const element = Element("div", "empty-state", message);
    if (detail) element.append(Element("small", "", detail));
    return element;
}

function DetailList(entries)
{
    const list = Element("dl", "detail-list");
    for (const [name, value] of entries)
    {
        const row = Element("div");
        row.append(Element("dt", "", name), Element("dd", "", value));
        list.append(row);
    }
    return list;
}

function Capacity(label, used, maximum)
{
    const box = Element("div");
    const heading = Element("div", "capacity-heading");
    heading.append(Element("span", "", label), Element("span", "", `${NumberText(used)} / ${NumberText(maximum)}`));
    const progress = Element("progress", "capacity");
    progress.max = maximum || 1;
    progress.value = used;
    progress.setAttribute("aria-label", label);
    box.append(heading, progress);
    return box;
}

function Action(text, callback, className = "button")
{
    const button = Element("button", className, text);
    button.type = "button";
    button.addEventListener("click", callback);
    return button;
}

function SelectBroker(id)
{
    m_SelectedBroker = id;
    Render();
}

function ShowSessions(id)
{
    Get("session-broker").value = id;
    Get("session-state").value = "all";
    Get("session-role").value = "all";
    Get("session-search").value = "";
    m_Page = 0;
    location.hash = "sessions";
    LoadSessions();
}

function SetView()
{
    m_View = location.hash.slice(1);
    if (!Object.hasOwn(EMPY_VIEWS, m_View)) m_View = "overview";
    for (const panel of document.querySelectorAll("[data-panel]")) panel.hidden = panel.dataset.panel !== m_View;
    for (const link of document.querySelectorAll("[data-view]"))
    {
        const active = link.dataset.view === m_View;
        link.classList.toggle("active", active);
        if (active) link.setAttribute("aria-current", "page");
        else link.removeAttribute("aria-current");
        link.setAttribute("aria-label", EMPY_VIEWS[link.dataset.view][0]);
    }
    Text("page-title", EMPY_VIEWS[m_View][0]);
    Text("page-subtitle", EMPY_VIEWS[m_View][1]);
    document.title = `${EMPY_VIEWS[m_View][0]} · Empy`;
    Render();
    LoadSessions();
}

function RenderStatus()
{
    Text("clock", new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", timeZoneName: "short" }));
    const status = Get("connection-label");
    const sources = Sources();
    const totals = Totals(sources);
    const label = m_Paused ? "View paused" : m_Error ? "Disconnected" : !m_State ? "Connecting" : totals.reporting === totals.configured ? "Live updates" : "Partial visibility";
    status.dataset.state = m_Paused ? "paused" : m_Error || (m_State && totals.reporting < totals.configured) ? "warning" : "live";
    status.replaceChildren(Element("span", "status-dot"), document.createTextNode(label));
    Text("refresh-note", m_Paused ? "Updates paused" : m_LastReceived ? `Updated ${Duration(performance.now() - m_LastReceived)} ago` : "Waiting for telemetry");
    const messages = [];
    if (m_Error) messages.push("The dashboard connection was lost. Displayed observations are stale; reconnecting automatically.");
    else if (m_Paused) messages.push("This view is paused. Values show the last received observation. Resume to see current network state.");
    else if (m_State && totals.reporting < totals.configured)
        messages.push(`${totals.configured - totals.reporting} of ${totals.configured} telemetry sources are not providing distinct live observations. Their last observations remain visible; current totals include reporting brokers only.`);
    if (totals.omitted) messages.push(`${NumberText(totals.omitted)} sessions are outside the configured snapshot limit. Broker totals include these sessions.`);
    const notice = Get("notice");
    notice.hidden = !messages.length;
    notice.textContent = messages.join(" ");
    for (const badge of document.querySelectorAll(".tiny-badge")) badge.textContent = m_Paused ? "PAUSED" : m_Error ? "STALE" : "LIVE";
}

function RenderMetrics(sources, mesh)
{
    const totals = Totals(sources);
    Text("stat-brokers", NumberText(totals.reporting));
    Text("stat-brokers-total", `/ ${totals.configured}`);
    Text("stat-brokers-note", mesh.expected ? `${NumberText(mesh.confirmed)} of ${NumberText(mesh.expected)} possible links active`
        : totals.reporting === totals.configured ? "All telemetry sources reporting" : `${totals.configured - totals.reporting} sources need attention`);
    Text("stat-clients", totals.reporting ? NumberText(totals.clients) : "—");
    Text("stat-sessions-note", totals.reporting ? `${NumberText(totals.sessions)} sessions · ${NumberText(totals.sessions - totals.clients)} offline` : "Awaiting broker observations");
    const ingress = Bytes(totals.receivedBytes, true);
    Text("stat-ingress", ingress.value);
    Text("stat-ingress-unit", ingress.unit);
    Text("stat-egress", totals.sentBytes == null ? "Waiting for a second sample" : `${ByteText(totals.sentBytes, true)} egress`);
    Text("stat-messages", totals.publications == null ? "—" : NumberText(totals.publications));
    const busiest = sources.filter(source => source.rates).sort((a, b) => b.rates.publications - a.rates.publications)[0];
    Text("stat-messages-note", busiest ? `Busiest: ${busiest.name} · ${Rate(busiest.rates.publications)}` : "Accepted client publications");
    Text("stat-pending", totals.reporting ? NumberText(totals.pending) : "—");
    Text("stat-requests", totals.reporting ? `${NumberText(totals.requests)} active request${totals.requests === 1 ? "" : "s"} · ${totals.storedBytes == null ? "Memory state" : ByteText(totals.storedBytes) + " stored"}` : "Awaiting broker observations");
    Text("nav-brokers", NumberText(sources.length));
    Text("nav-sessions", totals.reporting ? NumberText(totals.sessions) : "—");
    const metric = Get("history-metric").value;
    Text("chart-in", metric === "messages" ? Rate(totals.publications) : ByteText(totals.receivedBytes, true));
    Text("chart-out", ByteText(totals.sentBytes, true));
    Get("chart-out-item").hidden = metric === "messages";
    Text("chart-in-label", metric === "messages" ? "Publications" : "Ingress");
    // One bar per broker would overflow; show at most 24, then the failing share.
    const bars = sources.length <= 24 ? sources.map(source => source.state === "online")
        : Array.from({ length: 24 }, (_, index) => index < Math.round(24 * totals.reporting / Math.max(1, sources.length)));
    Get("broker-health").replaceChildren(...bars.map(online => Element("i", online ? "" : "unavailable")));
    Sparkline(Get("client-spark"), m_State.history, "online", "#a099ff");
    Sparkline(Get("traffic-spark"), m_State.history, "receivedBytes", "#75e5bd");
    Sparkline(Get("message-spark"), m_State.history, "publications", "#efbd76");
}

function RenderBrokerDetail(sources)
{
    if (!sources.some(source => source.id === m_SelectedBroker))
        m_SelectedBroker = (sources.find(source => source.state === "online") || sources[0])?.id;
    const source = sources.find(source => source.id === m_SelectedBroker);
    const container = Get("broker-detail");
    if (!source) { container.replaceChildren(Empty("No broker selected")); return; }
    const fragment = document.createDocumentFragment();
    const data = source.data;
    fragment.append(Element("div", "detail-name", source.name));
    fragment.append(Element("div", "detail-endpoint mono", data ? `${data.address}:${data.port}` : "No observation received"));
    const badges = Element("div", "badges");
    badges.append(Badge(Title(source.state), source.state));
    if (data) badges.append(Badge(data.transport), Badge(data.cluster ? "Clustered" : "Standalone"));
    fragment.append(badges);
    if (data)
    {
        const links = data.cluster?.peers.filter(peer => peer.connected).length || 0;
        const rows = [["Uptime", Duration(data.uptimeMs)], ["Online clients", NumberText(data.metrics.online)],
            ["Message rate", Rate(source.rates?.publications)], ["Publications", NumberText(data.metrics.publications)],
            ["Forwarded", NumberText(data.metrics.forwarded)], ["Retransmissions", NumberText(data.metrics.retransmitted)],
            ["Rejected", NumberText(data.metrics.rejected)], ["Telemetry RTT", source.latencyMs == null ? "—" : `${NumberText(source.latencyMs)} ms`]];
        if (data.cluster)
            rows.push(["Peer links", `${links} / ${data.cluster.peers.length}`], ["Active leases", NumberText(data.cluster.activeLeases)],
                ["Routing epoch", data.cluster.epoch], ["Placement", `${data.cluster.shards} shards · ${data.cluster.replicas} replicas · fan-out ${data.cluster.fanout}`]);
        else rows.push(["Persistence", data.durable ? "Enabled" : "Memory"], ["Authentication", data.authenticationRequired ? "Required" : "Optional"]);
        fragment.append(DetailList(rows), Element("hr", "detail-divider"));
        fragment.append(Capacity("Session capacity", data.metrics.sessions, data.limits.sessions));
        const idle = data.cluster?.peers.filter(peer => !peer.connected).length || 0;
        if (idle) fragment.append(Element("p", "detail-note", `${idle} peer${idle === 1 ? "" : "s"} idle · links are dialed when brokers exchange work`));
        const inspect = Action("Inspect sessions  →", () => ShowSessions(source.id), "button subtle detail-action");
        inspect.id = "inspect-broker-sessions";
        fragment.append(inspect);
    }
    else fragment.append(Empty(source.error || "Connecting to telemetry", "Waiting for this broker to report."));
    container.replaceChildren(fragment);
}

function RenderBrokerTable(sources)
{
    const rows = BrokerRows(sources, { search: Get("broker-search").value, ...m_BrokerSort });
    Text("broker-inventory-note", `${NumberText(rows.length)} of ${NumberText(sources.length)} telemetry sources`);
    const focused = Get("broker-table").contains(document.activeElement) ? document.activeElement.dataset.key : null;
    if (!rows.length) { Get("broker-table").replaceChildren(Empty("No brokers match this search")); return; }
    const table = Element("table", "broker-table");
    const header = Element("tr");
    for (const [key, label] of EMPY_BROKER_COLUMNS)
    {
        const th = Element("th");
        th.scope = "col";
        const active = m_BrokerSort.sort === key;
        th.setAttribute("aria-sort", active ? (m_BrokerSort.descending ? "descending" : "ascending") : "none");
        const button = Action(`${label}${active ? (m_BrokerSort.descending ? " ↓" : " ↑") : ""}`, () =>
        {
            m_BrokerSort = { sort: key, descending: active ? !m_BrokerSort.descending : key !== "name" && key !== "state" };
            RenderBrokerTable(Sources());
        }, "sort-button");
        button.dataset.key = `sort-${key}`;
        th.append(button);
        header.append(th);
    }
    const head = Element("thead");
    head.append(header);
    const body = Element("tbody");
    for (const row of rows)
    {
        const line = Element("tr");
        const name = Element("td");
        const link = Action(row.name, () => { m_SelectedBroker = row.source.id; location.hash = "overview"; }, "session-link");
        link.dataset.key = `broker-${row.source.id}`;
        link.title = row.source.data?.identifier || row.source.id;
        name.append(link);
        const state = Element("td");
        state.append(Badge(Title(row.source.state), row.source.state));
        const linkText = row.links == null ? "—" : `${row.links} / ${row.peers}`;
        // Links are dialed on demand, so fewer active links than peers is normal.
        const links = Element("td", "mono", linkText);
        line.append(name, state, Element("td", "mono", NumberText(row.clients)), Element("td", "mono", NumberText(row.sessions)),
            Element("td", "mono", ByteText(row.ingress, true)), Element("td", "mono", ByteText(row.egress, true)),
            Element("td", "mono", NumberText(row.messages)), Element("td", "mono", NumberText(row.pending)), links,
            Element("td", "mono", NumberText(row.leases)), Element("td", "mono", row.latency == null ? "—" : `${NumberText(row.latency)} ms`),
            Element("td", "mono", Duration(row.uptime)));
        body.append(line);
    }
    table.append(head, body);
    Get("broker-table").replaceChildren(table);
    if (focused) Get("broker-table").querySelector(`[data-key="${CSS.escape(focused)}"]`)?.focus({ preventScroll: true });
}

function RenderEvents(container, events)
{
    const fragment = document.createDocumentFragment();
    for (const event of events)
    {
        const row = Element("div", "event");
        row.append(Element("span", `event-icon ${event.kind}`, { online: "✓", warning: "!", sessions: "↔", restart: "↻" }[event.kind] || "·"));
        const message = Element("div", "event-message", event.message);
        const time = Element("time", "", `${Time(event.time)} · ${Duration(Date.now() - event.time)} ago`);
        time.dateTime = new Date(event.time).toISOString();
        message.append(time);
        row.append(message);
        fragment.append(row);
    }
    if (!events.length) fragment.append(Empty("No changes observed yet", "Connection changes and restarts appear here."));
    container.replaceChildren(fragment);
}

function Table(rows, container, preview = false)
{
    const focused = container.contains(document.activeElement) ? document.activeElement.dataset.session : null;
    if (!rows.length)
    {
        container.replaceChildren(Empty("No sessions to display", "Sessions appear when clients connect. Check your filters if a session is missing."));
        return;
    }
    const table = Element("table");
    const head = Element("thead");
    const header = Element("tr");
    for (const label of ["Session", "Broker", "State", "Role", "Received", "Sent", "In flight"])
    {
        const th = Element("th", "", label);
        th.scope = "col";
        if (label === "Received" || label === "Sent") th.title = "MMQP bytes observed at the broker, including control packets";
        header.append(th);
    }
    head.append(header);
    table.append(head);
    const body = Element("tbody");
    for (const row of rows)
    {
        const line = Element("tr");
        const id = Element("td");
        const button = Action(`${row.session.identifier.slice(0, 8)}…`, () =>
        {
            m_SelectedSession = row.key;
            m_Detail = row;
            if (preview) location.hash = "sessions";
            RenderSessionDetail();
            LoadSessions();
            Get("session-inspector").scrollIntoView({ block: "nearest" });
        }, "session-link");
        button.title = row.session.identifier;
        button.dataset.session = row.key;
        button.setAttribute("aria-label", `Inspect session ${row.session.identifier}`);
        id.append(button);
        const state = Element("td");
        state.append(Badge(Title(row.state), row.state));
        line.append(id, Element("td", "", row.source.name), state, Element("td", "role-list", row.roles.join(" · ")),
            Element("td", "mono", ByteText(row.session.receivedBytes)), Element("td", "mono", ByteText(row.session.sentBytes)),
            Element("td", "mono", NumberText(row.session.pending)));
        body.append(line);
    }
    table.append(body);
    container.replaceChildren(table);
    if (focused) [...container.querySelectorAll("[data-session]")].find(button => button.dataset.session === focused)?.focus({ preventScroll: true });
}

function SessionFilters()
{
    return { broker: Get("session-broker").value, state: Get("session-state").value, role: Get("session-role").value,
        search: Get("session-search").value.slice(0, 128) };
}

// Sessions are filtered and paged by the collector, so large fabrics never ship every session to the browser.
async function LoadSessions()
{
    if (!m_State || (m_View !== "overview" && m_View !== "sessions")) return;
    const version = ++m_SessionVersion;
    const query = (values) => "/api/sessions?" + new URLSearchParams(values);
    const fetchJson = async url =>
    {
        const response = await fetch(url, { cache: "no-store" });
        if (response.status === 404) return null;
        if (!response.ok) throw new Error("Session query failed");
        return response.json();
    };
    try
    {
        const [sessions, preview, detail] = await Promise.all([
            m_View === "sessions" ? fetchJson(query({ ...SessionFilters(), page: m_Page, size: EMPY_PAGE_SIZE })) : m_Sessions,
            m_View === "overview" ? fetchJson(query({ size: 5 })) : m_Preview,
            m_SelectedSession ? fetchJson("/api/session?" + new URLSearchParams({ key: m_SelectedSession })) : null]);
        if (version !== m_SessionVersion) return;
        m_Sessions = sessions;
        m_Preview = preview;
        m_Detail = m_SelectedSession ? detail : undefined;
    }
    catch { if (version !== m_SessionVersion) return; }
    RenderTables();
}

function RenderTables()
{
    if (!m_State) return;
    const sources = Sources();
    const filterKey = sources.map(source => `${source.id}:${source.name}`).join("|");
    if (filterKey !== m_FilterKey)
    {
        const select = Get("session-broker"), selected = select.value;
        const all = Element("option", "", "All brokers");
        all.value = "all";
        select.replaceChildren(all, ...sources.map(source =>
        {
            const option = Element("option", "", source.name);
            option.value = source.id;
            return option;
        }));
        select.value = [...select.options].some(option => option.value === selected) ? selected : "all";
        m_FilterKey = filterKey;
    }
    if (m_View === "overview" && m_Preview) Table(m_Preview.rows, Get("session-preview"), true);
    if (m_View === "sessions" && m_Sessions)
    {
        m_Page = m_Sessions.page;
        Table(m_Sessions.rows, Get("session-table"));
        Text("session-count", `${NumberText(m_Sessions.total)} matching · ${NumberText(m_Sessions.observed)} observations${Totals(sources).omitted ? " · snapshot truncated" : ""}`);
        Text("session-page", `${m_Sessions.page + 1} / ${m_Sessions.pages}`);
        Get("previous-page").disabled = m_Sessions.page === 0;
        Get("next-page").disabled = m_Sessions.page >= m_Sessions.pages - 1;
    }
    RenderSessionDetail();
}

function RenderSessionDetail()
{
    const row = m_Detail;
    const panel = Get("session-inspector");
    panel.hidden = !m_SelectedSession;
    if (!m_SelectedSession || row === undefined) return;
    if (!row)
    {
        Text("session-detail-id", m_SelectedSession.split("/").at(-1));
        Get("session-detail").replaceChildren(Empty("This session is no longer in the latest snapshot."));
        return;
    }
    const value = row.session;
    Text("session-detail-id", value.identifier);
    const columns =
    [
        ["Connection", [["Broker", row.source.name], ["State", Title(row.state)], ["Role", row.roles.join(", ")],
            ["Transport", value.transport || "—"], ["Realm", value.realm || "Default"], ["Authenticated", value.authenticated ? "Yes" : "No"],
            ["Last activity", Duration(value.idleMs)]]],
        ["Traffic observed here", [["Received packets", NumberText(value.receivedPackets)], ["Sent packets", NumberText(value.sentPackets)],
            ["Received bytes", ByteText(value.receivedBytes)], ["Sent bytes", ByteText(value.sentBytes)],
            ["Publications", NumberText(value.publications)], ["Deliveries admitted", NumberText(value.deliveries)],
            ["Counters since", Time(value.observedAt)]]],
        ["Session state", [["Pending QoS 1", `${value.pending} / ${ByteText(value.pendingBytes)}`], ["Transport queue", ByteText(value.queuedBytes)],
            ["Replay queue", `${value.replay} / ${ByteText(value.replayBytes)}`], ["Pending requests", NumberText(value.requests)],
            ["Response routes", NumberText(value.responseRoutes)], ["Observed owner", row.state === "unknown" ? "Unknown" : value.owner ? "Yes" : "No"],
            ["Lease at sample", value.lease && row.state !== "unknown" ? `${NumberText(value.lease.remainingMs)} ms` : "—"],
            ["Expires in", value.online ? "Connected" : Duration(value.expiresInMs)]]]
    ];
    Get("session-detail").replaceChildren(...columns.map(([title, entries]) =>
    {
        const column = Element("div");
        column.append(Element("h3", "", title), DetailList(entries));
        return column;
    }));
}

function Render()
{
    const focusedId = document.activeElement?.id;
    RenderStatus();
    if (!m_State) return;
    const sources = Sources();
    const topology = Topology(sources);
    const mesh = Mesh(sources, topology);
    Text("workspace-name", m_State.name);
    Text("footer-update", `Collector updated ${Time(m_State.generatedAt)} · ${m_State.intervalMs / 1000}s polling`);
    RenderMetrics(sources, mesh);
    if (m_View === "overview")
    {
        RenderBrokerDetail(sources);
        const clients = sources.reduce((sum, source) => sum + (source.state === "online" ? source.data?.metrics.online || 0 : 0), 0);
        const failing = topology.edges.filter(edge => edge.state !== "connected" && edge.observed).length;
        Text("topology-summary", `${topology.nodes.length} brokers · ${NumberText(clients)} online clients · `
            + (mesh.expected ? `${NumberText(mesh.confirmed)} of ${NumberText(mesh.expected)} possible links active` : `${mesh.confirmed} confirmed links`)
            + (failing ? ` · ${failing} one-sided` : "")
            + (topology.nodes.length > EMPY_GRAPH_LIMIT ? ` · first ${EMPY_GRAPH_LIMIT} nodes shown` : ""));
        Get("graph-empty").hidden = sources.some(source => source.data);
        graph.Render(sources.some(source => source.data) ? sources : [], m_SelectedBroker, Get("show-clients").checked);
        TrafficChart(Get("traffic-chart"), m_State.history, Number(Get("history-window").value), Get("history-metric").value);
        Text("chart-caption", `Live sources only · ${sources.filter(source => source.state === "online").length}/${sources.length} reporting · gaps indicate missing samples · process-local counters`);
        RenderEvents(Get("recent-events"), m_State.events.slice(0, 4));
    }
    if (m_View === "brokers") RenderBrokerTable(sources);
    if (m_View === "activity") RenderEvents(Get("all-events"), m_State.events);
    if (m_View === "overview" || m_View === "sessions") RenderTables();
    if (focusedId && document.activeElement?.id !== focusedId) Get(focusedId)?.focus({ preventScroll: true });
}

async function Refresh()
{
    if (m_InFlight) return;
    m_InFlight = true;
    const version = m_RequestVersion;
    Get("refresh").disabled = true;
    const abort = new AbortController();
    const timeout = setTimeout(() => abort.abort(), 4000);
    let updated = false;
    try
    {
        const response = await fetch("/api/state", { cache: "no-store", signal: abort.signal });
        if (!response.ok) throw new Error("Dashboard unavailable");
        const state = await response.json();
        if (state.schema !== 1 || !Array.isArray(state.sources)) throw new Error("Unsupported dashboard schema");
        if (version === m_RequestVersion)
        {
            m_State = state;
            m_Error = "";
            m_LastReceived = performance.now();
            updated = true;
        }
    }
    catch (error) { if (version === m_RequestVersion) m_Error = error.message; }
    finally
    {
        clearTimeout(timeout);
        m_InFlight = false;
        Get("refresh").disabled = false;
        Render();
        if (updated) LoadSessions();
    }
}

const ReloadSessions = () => { m_Page = 0; LoadSessions(); };
Get("refresh").addEventListener("click", Refresh);
Get("pause").addEventListener("click", () =>
{
    m_Paused = !m_Paused;
    ++m_RequestVersion;
    Get("pause").textContent = m_Paused ? "Resume" : "Pause";
    Get("pause").setAttribute("aria-pressed", String(m_Paused));
    RenderStatus();
    if (!m_Paused) Refresh();
});
Get("show-clients").addEventListener("change", Render);
Get("history-window").addEventListener("change", Render);
Get("history-metric").addEventListener("change", Render);
Get("broker-search").addEventListener("input", () => RenderBrokerTable(Sources()));
Get("zoom-in").addEventListener("click", () => graph.Zoom(0.8));
Get("zoom-out").addEventListener("click", () => graph.Zoom(1.25));
Get("zoom-fit").addEventListener("click", () => graph.Fit());
let searchTimer;
Get("session-search").addEventListener("input", () => { clearTimeout(searchTimer); searchTimer = setTimeout(ReloadSessions, 200); });
for (const id of ["session-broker", "session-state", "session-role"]) Get(id).addEventListener("change", ReloadSessions);
Get("previous-page").addEventListener("click", () => { m_Page = Math.max(0, m_Page - 1); LoadSessions(); });
Get("next-page").addEventListener("click", () => { ++m_Page; LoadSessions(); });
Get("close-session").addEventListener("click", () => { m_SelectedSession = null; m_Detail = undefined; RenderSessionDetail(); });
Get("export").addEventListener("click", () =>
{
    if (!m_State) return;
    const url = URL.createObjectURL(new Blob([JSON.stringify(m_State, null, 4)], { type: "application/json" }));
    const anchor = Element("a");
    anchor.href = url;
    anchor.download = `empy-observations-${new Date().toISOString().replaceAll(":", "-")}.json`;
    anchor.click();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
});
window.addEventListener("hashchange", SetView);
let resizeTimer;
window.addEventListener("resize", () => { clearTimeout(resizeTimer); resizeTimer = setTimeout(Render, 100); });
SetView();
Refresh();
setInterval(() => { RenderStatus(); if (!m_Paused && !document.hidden) Refresh(); }, 1000);
document.addEventListener("visibilitychange", () => { if (!document.hidden && !m_Paused) Refresh(); });
