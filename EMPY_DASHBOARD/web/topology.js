import { Svg, Topology, EMPY_GRAPH_LIMIT, EMPY_DETAIL_NODES } from "./model.js";

// Large fabrics use one ring sized for about 46 units per broker.
function Ring(count)
{
    const radius = Math.max(220, count * 46 / (2 * Math.PI));
    return Array.from({ length: count }, (_, index) =>
    {
        const angle = -Math.PI / 2 + index * 2 * Math.PI / count;
        return { x: radius + 110 + Math.cos(angle) * radius, y: radius + 70 + Math.sin(angle) * radius };
    });
}

function Positions(count, compact)
{
    if (count > EMPY_DETAIL_NODES) return Ring(count);
    if (compact && count <= 3)
    {
        if (count === 1) return [{ x: 260, y: 205 }];
        if (count === 2) return [{ x: 140, y: 205 }, { x: 380, y: 205 }];
        if (count === 3) return [{ x: 140, y: 130 }, { x: 380, y: 130 }, { x: 260, y: 285 }];
    }
    if (count === 1) return [{ x: 450, y: 205 }];
    if (count === 2) return [{ x: 250, y: 205 }, { x: 650, y: 205 }];
    if (count === 3) return [{ x: 230, y: 145 }, { x: 670, y: 145 }, { x: 450, y: 280 }];
    const columns = Math.min(4, Math.ceil(Math.sqrt(count)));
    return Array.from({ length: count }, (_, index) => ({ x: 180 + (index % columns) * 280, y: 145 + Math.floor(index / columns) * 280 }));
}

export class NetworkGraph
{
    constructor(element, onSelect)
    {
        this.m_Element = element;
        this.m_OnSelect = onSelect;
        this.m_View = [0, 0, 900, 420];
        this.m_Bounds = [...this.m_View];
        this.m_Key = "";
        this.m_Drag = null;
        element.addEventListener("click", event =>
        {
            const node = event.target.closest("[data-source]");
            if (node && !this.m_Moved) this.m_OnSelect(node.dataset.source);
        });
        element.addEventListener("keydown", event =>
        {
            const node = event.target.closest("[data-source]");
            if (node && (event.key === "Enter" || event.key === " "))
            {
                event.preventDefault();
                this.m_OnSelect(node.dataset.source);
            }
        });
        element.addEventListener("pointerdown", event =>
        {
            if (event.button !== 0) return;
            this.m_Moved = false;
            this.m_Drag = { x: event.clientX, y: event.clientY, view: [...this.m_View] };
            if (!event.target.closest("[data-source]")) element.setPointerCapture(event.pointerId);
        });
        element.addEventListener("pointermove", event =>
        {
            if (!this.m_Drag) return;
            const dx = event.clientX - this.m_Drag.x, dy = event.clientY - this.m_Drag.y;
            if (Math.abs(dx) + Math.abs(dy) > 4) this.m_Moved = true;
            if (!this.m_Moved) return;
            const scale = Math.min(element.clientWidth / this.m_View[2], element.clientHeight / this.m_View[3]);
            this.m_View[0] = this.m_Drag.view[0] - dx / scale;
            this.m_View[1] = this.m_Drag.view[1] - dy / scale;
            this.ApplyView();
        });
        const release = () => { this.m_Drag = null; };
        element.addEventListener("pointerup", release);
        element.addEventListener("pointercancel", release);
        element.addEventListener("lostpointercapture", release);
        window.addEventListener("pointerup", release);
        window.addEventListener("pointercancel", release);
    }

    ApplyView() { this.m_Element.setAttribute("viewBox", this.m_View.join(" ")); }
    Fit() { this.m_View = [...this.m_Bounds]; this.ApplyView(); }
    Zoom(factor)
    {
        const width = Math.max(240, Math.min(5000, this.m_View[2] * factor));
        const ratio = width / this.m_View[2];
        this.m_View = [this.m_View[0] + this.m_View[2] * (1 - ratio) / 2,
            this.m_View[1] + this.m_View[3] * (1 - ratio) / 2, width, this.m_View[3] * ratio];
        this.ApplyView();
    }

    Render(sources, selected, showClients)
    {
        // Preserve keyboard focus and zoom while replacing live SVG observations.
        if (this.m_Drag) return;
        const focused = this.m_Element.contains(document.activeElement) ? document.activeElement.dataset.source : null;
        const model = Topology(sources);
        model.nodes = model.nodes.slice(0, EMPY_GRAPH_LIMIT);
        const large = model.nodes.length > EMPY_DETAIL_NODES;
        const visible = new Set(model.nodes.map(node => node.key));
        const chosen = model.nodes.find(node => node.source?.id === selected)?.key;
        // Links are dialed on demand, so most pairs of a large fabric are idle. Large views draw
        // the selection's live links and asymmetric links that only one broker reports.
        model.edges = model.edges.filter(edge => visible.has(edge.from) && visible.has(edge.to)
            && (!large || (edge.state === "connected" ? edge.from === chosen || edge.to === chosen : edge.observed)));
        const compact = this.m_Element.clientWidth < 550;
        this.m_Element.classList.toggle("compact", compact);
        this.m_Element.classList.toggle("large", large);
        const positions = Positions(model.nodes.length, compact);
        const points = new Map(model.nodes.map((node, index) => [node.key, positions[index]]));
        const key = `${compact}/` + model.nodes.map(node => node.key).join("|");
        if (key !== this.m_Key)
        {
            const margin = large ? 110 : compact ? 130 : 180;
            this.m_Bounds = [0, 0, Math.max(compact ? 520 : 900, ...positions.map(point => point.x + margin)), Math.max(420, ...positions.map(point => point.y + (large ? 70 : 135)))];
            this.m_Key = key;
            this.Fit();
        }
        const fragment = document.createDocumentFragment();
        for (const edge of model.edges)
        {
            const from = points.get(edge.from), to = points.get(edge.to);
            const line = Svg("line", { x1: from.x, y1: from.y, x2: to.x, y2: to.y,
                class: `graph-edge ${edge.state === "connected" ? "live" : "uncertain"}` });
            line.append(Svg("title", {}, edge.state === "connected" ? "Authenticated link reported by both brokers"
                : edge.observed ? "Link reported by one broker; opposite observation unavailable or different" : "No current connected link confirmed"));
            fragment.append(line);
        }
        const busiest = Math.max(1, ...model.nodes.map(node => node.source?.rates?.publications || 0));
        model.nodes.forEach((node, index) =>
        {
            const point = positions[index];
            if (showClients && !large) this.Clients(fragment, node, point, model.nodes.length, index, compact);
            const group = Svg("g", { class: `graph-node ${node.state === "online" ? "" : "unavailable"} ${node.source?.id === selected ? "selected" : ""}` });
            if (node.source)
            {
                group.setAttribute("data-source", node.source.id);
                group.setAttribute("tabindex", "0");
                group.setAttribute("role", "button");
                group.setAttribute("aria-label", `Inspect ${node.name}, ${node.state}`);
                group.setAttribute("aria-pressed", String(node.source.id === selected));
            }
            const data = node.source?.data;
            const caption = data ? `${node.state === "online" ? data.metrics.online : "?"} clients · ${data.transport}` : node.state;
            if (large)
            {
                // Halo size follows the broker's share of the busiest publication rate.
                const load = (node.source?.rates?.publications || 0) / busiest;
                group.append(Svg("circle", { cx: point.x, cy: point.y, r: 14 + load * 10, class: "node-halo load" }));
                group.append(Svg("circle", { cx: point.x, cy: point.y, r: 10, class: "node-body" }));
                if (model.nodes.length <= 48 || node.source?.id === selected)
                    group.append(Svg("text", { x: point.x, y: point.y + 27, class: "node-name small" }, node.name.length > 14 ? node.name.slice(0, 13) + "…" : node.name));
            }
            else
            {
                group.append(Svg("circle", { cx: point.x, cy: point.y, r: 42, class: "node-halo" }));
                group.append(Svg("rect", { x: point.x - 24, y: point.y - 24, width: 48, height: 48, rx: 12, class: "node-body" }));
                const icon = Svg("svg", { x: point.x - 12, y: point.y - 12, width: 24, height: 24, viewBox: "0 0 24 24", class: "node-symbol", fill: "none", stroke: "currentColor", "stroke-width": 1.3 });
                icon.append(Svg("use", { href: "#i-server" }));
                group.append(icon);
                group.append(Svg("text", { x: point.x, y: point.y + 48, class: "node-name" }, node.name.length > 23 ? node.name.slice(0, 21) + "…" : node.name));
                group.append(Svg("text", { x: point.x, y: point.y + 63, class: "node-caption" }, caption));
            }
            group.append(Svg("title", {}, `${node.name}\n${node.identifier}\n${node.state}${data ? "\n" + caption : ""}`));
            fragment.append(group);
        });
        this.m_Element.replaceChildren(fragment);
        if (focused)
        {
            const node = [...this.m_Element.querySelectorAll("[data-source]")].find(item => item.dataset.source === focused);
            node?.focus({ preventScroll: true });
        }
        this.ApplyView();
    }

    Clients(fragment, node, point, count, index, compact)
    {
        const sessions = node.clients;
        const angles = count === 3 ? [[-165, -125, -80, 155, -40, 110], [-100, -55, -15, 25, 75, -145], [15, 55, 100, 155, 185, -5]][index] : null;
        sessions.forEach((session, clientIndex) =>
        {
            const angle = (angles ? angles[clientIndex] : -165 + clientIndex * 360 / Math.max(3, sessions.length)) * Math.PI / 180;
            const radius = compact && count > 1 ? 78 : count === 1 ? 130 : 101;
            const x = point.x + Math.cos(angle) * radius, y = point.y + Math.sin(angle) * radius;
            fragment.append(Svg("line", { x1: point.x, y1: point.y, x2: x, y2: y, class: "client-edge" }));
            const client = Svg("g");
            const online = session.online && node.state === "online";
            client.append(Svg("circle", { cx: x, cy: y, r: 9, class: `client-dot ${online ? "" : "offline"}` }));
            client.append(Svg("circle", { cx: x, cy: y, r: 2, fill: online ? "#92c1df" : "#7f8591" }));
            client.append(Svg("text", { x, y: y + 23, class: "client-caption" }, `${session.roles[0].slice(0, 3).toLowerCase()} · ${session.identifier.slice(0, 4)}`));
            client.append(Svg("title", {}, `${session.identifier}\n${session.roles.join(", ")}\n${node.state === "online" ? (session.online ? "Online" : "Offline / resumable") : "State unknown: broker telemetry unavailable"}`));
            fragment.append(client);
        });
        const total = node.source?.data?.metrics.sessions || 0;
        if (total > sessions.length)
            fragment.append(Svg("text", { x: point.x, y: point.y + 78, class: "node-caption" }, `+${total - sessions.length} more sessions`));
    }
}
