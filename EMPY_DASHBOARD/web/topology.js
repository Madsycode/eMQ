import { Svg, Roles, Topology, EMPY_GRAPH_LIMIT } from "./model.js";

function Positions(count, compact)
{
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
        const visible = new Set(model.nodes.map(node => node.key));
        model.edges = model.edges.filter(edge => visible.has(edge.from) && visible.has(edge.to));
        const compact = this.m_Element.clientWidth < 550;
        this.m_Element.classList.toggle("compact", compact);
        const positions = Positions(model.nodes.length, compact);
        const points = new Map(model.nodes.map((node, index) => [node.key, positions[index]]));
        const key = `${compact}/` + model.nodes.map(node => node.key).join("|");
        if (key !== this.m_Key)
        {
            this.m_Bounds = [0, 0, Math.max(compact ? 520 : 900, ...positions.map(point => point.x + (compact ? 130 : 180))), Math.max(420, ...positions.map(point => point.y + 135))];
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
        model.nodes.forEach((node, index) =>
        {
            const point = positions[index];
            if (showClients)
            {
                const sessions = [...node.clients].sort((a, b) => Number(b.online) - Number(a.online) || a.identifier.localeCompare(b.identifier));
                const displayed = sessions.slice(0, 6);
                const angles = model.nodes.length === 3 ? [[-165, -125, -80, 155, -40, 110], [-100, -55, -15, 25, 75, -145], [15, 55, 100, 155, 185, -5]][index] : null;
                displayed.forEach((session, clientIndex) =>
                {
                    const angle = (angles ? angles[clientIndex] : -165 + clientIndex * 360 / Math.max(3, displayed.length)) * Math.PI / 180;
                    const radius = compact && model.nodes.length > 1 ? 78 : model.nodes.length === 1 ? 130 : 101;
                    const x = point.x + Math.cos(angle) * radius, y = point.y + Math.sin(angle) * radius;
                    fragment.append(Svg("line", { x1: point.x, y1: point.y, x2: x, y2: y, class: "client-edge" }));
                    const client = Svg("g");
                    const online = session.online && node.state === "online";
                    client.append(Svg("circle", { cx: x, cy: y, r: 9, class: `client-dot ${online ? "" : "offline"}` }));
                    client.append(Svg("circle", { cx: x, cy: y, r: 2, fill: online ? "#92c1df" : "#7f8591" }));
                    const roles = Roles(session);
                    client.append(Svg("text", { x, y: y + 23, class: "client-caption" }, `${roles[0].slice(0, 3).toLowerCase()} · ${session.identifier.slice(0, 4)}`));
                    client.append(Svg("title", {}, `${session.identifier}\n${roles.join(", ")}\n${node.state === "online" ? (session.online ? "Online" : "Offline / resumable") : "State unknown: broker telemetry unavailable"}`));
                    fragment.append(client);
                });
                if (sessions.length > displayed.length)
                    fragment.append(Svg("text", { x: point.x, y: point.y + 78, class: "node-caption" }, `+${sessions.length - displayed.length} more sessions`));
            }
            const group = Svg("g", { class: `graph-node ${node.state === "online" ? "" : "unavailable"} ${node.source?.id === selected ? "selected" : ""}` });
            if (node.source)
            {
                group.setAttribute("data-source", node.source.id);
                group.setAttribute("tabindex", "0");
                group.setAttribute("role", "button");
                group.setAttribute("aria-label", `Inspect ${node.name}, ${node.state}`);
                group.setAttribute("aria-pressed", String(node.source.id === selected));
            }
            group.append(Svg("circle", { cx: point.x, cy: point.y, r: 42, class: "node-halo" }));
            group.append(Svg("rect", { x: point.x - 24, y: point.y - 24, width: 48, height: 48, rx: 12, class: "node-body" }));
            const icon = Svg("svg", { x: point.x - 12, y: point.y - 12, width: 24, height: 24, viewBox: "0 0 24 24", class: "node-symbol", fill: "none", stroke: "currentColor", "stroke-width": 1.3 });
            icon.append(Svg("use", { href: "#i-server" }));
            group.append(icon);
            group.append(Svg("text", { x: point.x, y: point.y + 48, class: "node-name" }, node.name.length > 23 ? node.name.slice(0, 21) + "…" : node.name));
            group.append(Svg("text", { x: point.x, y: point.y + 63, class: "node-caption" }, node.source?.data
                ? `${node.state === "online" ? node.source.data.metrics.online : "?"} clients · ${node.source.data.transport}` : node.state));
            group.append(Svg("title", {}, `${node.name}\n${node.identifier}\n${node.state}`));
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
}
