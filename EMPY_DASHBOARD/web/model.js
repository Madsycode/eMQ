const EMPY_NUMBER_FORMAT = new Intl.NumberFormat("en", { maximumFractionDigits: 1 });
export const EMPY_GRAPH_LIMIT = 256;
// Above this many brokers the topology switches to a compact ring without client dots.
export const EMPY_DETAIL_NODES = 12;

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

export function Totals(sources)
{
    const online = sources.filter(source => source.state === "online" && source.data);
    const sum = key => online.reduce((total, source) => total + (source.data.metrics[key] ?? source.data[key] ?? 0), 0);
    const rates = online.filter(source => source.rates);
    const throughput = key => rates.length ? rates.reduce((total, source) => total + source.rates[key], 0) : null;
    return { reporting: online.length, configured: sources.length, clients: sum("online"), sessions: sum("sessions"),
        pending: sum("pending"), retainedBytes: sum("retainedBytes"), receivedBytes: throughput("receivedBytes"),
        sentBytes: throughput("sentBytes"), publications: throughput("publications"),
        storedBytes: online.some(source => source.data.storedBytes != null)
            ? online.reduce((total, source) => total + (source.data.storedBytes || 0), 0) : null,
        requests: sum("requests"),
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
                source, state: source.state, clients: source.data?.preview || [] });
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

// Full-mesh link health per cluster manifest: confirmed links versus n(n-1)/2.
export function Mesh(sources, topology = Topology(sources))
{
    const members = new Map();
    for (const source of sources)
    {
        const cluster = source.data?.cluster;
        if (cluster) members.set(cluster.identifier, Math.max(members.get(cluster.identifier) || 0, cluster.peers.length + 1));
    }
    const expected = [...members.values()].reduce((total, count) => total + count * (count - 1) / 2, 0);
    const confirmed = topology.edges.filter(edge => edge.state === "connected").length;
    return { clusters: members.size, brokers: [...members.values()].reduce((total, count) => total + count, 0), expected, confirmed };
}

export const EMPY_BROKER_COLUMNS =
[
    ["name", "Broker"], ["state", "State"], ["clients", "Clients"], ["sessions", "Sessions"], ["ingress", "Ingress"],
    ["egress", "Egress"], ["messages", "Msg/s"], ["pending", "Pending"], ["links", "Peer links"], ["leases", "Leases"],
    ["latency", "RTT"], ["uptime", "Uptime"]
];

export function BrokerRows(sources, { search = "", sort = "name", descending = false } = {})
{
    const text = search.toLocaleLowerCase();
    const ranks = { online: 0, stale: 1, duplicate: 2, connecting: 3, unreachable: 4 };
    const rows = sources.filter(source => !text || `${source.name} ${source.id} ${source.data?.identifier || ""}`.toLocaleLowerCase().includes(text))
        .map(source =>
        {
            const data = source.data, live = source.state === "online" && data;
            const peers = data?.cluster?.peers || [];
            return { source, name: source.name, state: ranks[source.state] ?? 5,
                clients: live ? data.metrics.online : null, sessions: live ? data.metrics.sessions : null,
                ingress: source.rates?.receivedBytes ?? null, egress: source.rates?.sentBytes ?? null,
                messages: source.rates?.publications ?? null, pending: live ? data.metrics.pending : null,
                links: data?.cluster ? peers.filter(peer => peer.connected).length : null, peers: peers.length,
                leases: data?.cluster ? data.cluster.activeLeases : null, latency: source.latencyMs ?? null,
                uptime: data ? data.uptimeMs : null };
        });
    const direction = descending ? -1 : 1;
    return rows.sort((a, b) =>
    {
        const left = a[sort], right = b[sort];
        if (left == null || right == null) return left == null && right == null ? a.name.localeCompare(b.name) : left == null ? 1 : -1;
        return (typeof left === "string" ? left.localeCompare(right) : left - right) * direction || a.name.localeCompare(b.name);
    });
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
