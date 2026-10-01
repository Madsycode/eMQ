const EMPY_NUMBER_FORMAT = new Intl.NumberFormat("en", { maximumFractionDigits: 1 });
export const EMPY_GRAPH_LIMIT = 64;

export function NumberText(value)
{
    return value == null ? "—" : EMPY_NUMBER_FORMAT.format(value);
}

export function Bytes(value, rate = false)
{
    if (value == null || !Number.isFinite(value)) return { value: "—", unit: rate ? "B/s" : "B" };
    const units = ["B", "KiB", "MiB", "GiB", "TiB"];
    let index = 0;
    while (Math.abs(value) >= 1024 && index < units.length - 1) { value /= 1024; ++index; }
    return { value: NumberText(Math.round(value * (index ? 10 : 1)) / (index ? 10 : 1)), unit: units[index] + (rate ? "/s" : "") };
}

export function ByteText(value, rate = false)
{
    const result = Bytes(value, rate);
    return result.value === "—" ? "—" : `${result.value} ${result.unit}`;
}

export function Duration(milliseconds)
{
    if (milliseconds == null) return "—";
    const seconds = Math.floor(Math.max(0, milliseconds) / 1000);
    if (seconds < 60) return `${seconds}s`;
    if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
    if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ${Math.floor(seconds % 3600 / 60)}m`;
    return `${Math.floor(seconds / 86400)}d ${Math.floor(seconds % 86400 / 3600)}h`;
}

export function Roles(session)
{
    const roles = [];
    if (session.requesters) roles.push("Requester");
    if (session.responders) roles.push("Responder");
    if (session.registrations > session.requesters) roles.push("Publisher");
    if (session.subscriptions > session.responders) roles.push("Subscriber");
    return roles.length ? roles : ["Connected"];
}

export function SessionState(source, session)
{
    return source.state !== "online" ? "unknown" : session.online ? "online" : "offline";
}

export function Sessions(sources, filters = {})
{
    const search = (filters.search || "").toLocaleLowerCase();
    const result = [];
    for (const source of sources)
    {
        if (filters.broker && filters.broker !== "all" && filters.broker !== source.id) continue;
        for (const session of source.data?.sessions || [])
        {
            const state = SessionState(source, session);
            const roles = Roles(session);
            if (filters.state && filters.state !== "all" && state !== filters.state) continue;
            if (filters.role && filters.role !== "all" && !roles.includes(filters.role)) continue;
            if (search && !`${session.identifier} ${source.name} ${source.data.identifier} ${session.realm}`.toLocaleLowerCase().includes(search)) continue;
            result.push({ source, session, state, roles, key: `${source.id}/${session.identifier}` });
        }
    }
    const ranks = { online: 0, unknown: 1, offline: 2 };
    return result.sort((a, b) => ranks[a.state] - ranks[b.state] || a.source.name.localeCompare(b.source.name)
        || a.session.identifier.localeCompare(b.session.identifier));
}

export function Totals(sources)
{
    const online = sources.filter(source => source.state === "online" && source.data);
    const sum = key => online.reduce((total, source) => total + source.data.metrics[key], 0);
    const rates = online.filter(source => source.rates);
    const throughput = key => rates.length ? rates.reduce((total, source) => total + source.rates[key], 0) : null;
    return { reporting: online.length, configured: sources.length, clients: sum("online"), sessions: sum("sessions"),
        pending: sum("pending"), retainedBytes: sum("retainedBytes"), receivedBytes: throughput("receivedBytes"),
        sentBytes: throughput("sentBytes"), publications: throughput("publications"),
        storedBytes: online.some(source => source.data.storedBytes != null)
            ? online.reduce((total, source) => total + (source.data.storedBytes || 0), 0) : null,
        requests: online.reduce((total, source) => total + source.data.sessions.reduce((count, session) => count + session.requests, 0), 0),
        omitted: sources.reduce((total, source) => total + (source.data?.omittedSessions || 0), 0) };
}

function BrokerKey(source)
{
    return source.data?.cluster ? `${source.data.cluster.identifier}/${source.data.identifier}`
        : source.data?.instance ? `instance/${source.data.instance}` : `source/${source.id}`;
}

export function Topology(sources)
{
    const nodes = new Map();
    const edges = new Map();
    for (const source of sources)
    {
        const key = BrokerKey(source);
        if (!nodes.has(key) || (nodes.get(key).state !== "online" && source.state === "online"))
            nodes.set(key, { key, name: source.name, identifier: source.data?.identifier || source.id,
                source, state: source.state, clients: source.data?.sessions || [] });
    }
    for (const source of sources)
    {
        if (!source.data?.cluster) continue;
        const from = BrokerKey(source);
        for (const peer of source.data.cluster.peers)
        {
            const to = `${source.data.cluster.identifier}/${peer.identifier}`;
            if (to === from) continue;
            if (!nodes.has(to)) nodes.set(to, { key: to, name: peer.identifier, identifier: peer.identifier, source: null, state: "unmonitored", clients: [] });
            const key = [from, to].sort().join("|");
            if (!edges.has(key)) edges.set(key, { key, from, to, reports: [] });
            edges.get(key).reports.push({ from, connected: peer.connected, fresh: source.state === "online" });
        }
    }
    for (const edge of edges.values())
    {
        const reports = edge.reports.filter(report => report.fresh);
        edge.state = new Set(reports.map(report => report.from)).size === 2 && reports.every(report => report.connected)
            ? "connected" : "unconfirmed";
        edge.observed = reports.some(report => report.connected);
    }
    return { nodes: [...nodes.values()], edges: [...edges.values()] };
}

export function Element(tag, className = "", text = null)
{
    const element = document.createElement(tag);
    if (className) element.className = className;
    if (text !== null) element.textContent = text;
    return element;
}

export function Svg(tag, attributes = {}, text = null)
{
    const element = document.createElementNS("http://www.w3.org/2000/svg", tag);
    for (const [key, value] of Object.entries(attributes)) element.setAttribute(key, String(value));
    if (text !== null) element.textContent = text;
    return element;
}
