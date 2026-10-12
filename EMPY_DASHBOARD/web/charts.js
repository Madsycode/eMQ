import { Svg, ByteText, NumberText } from "./model.js";

function Path(points)
{
    let result = "";
    let connected = false;
    for (const point of points)
    {
        if (!point) { connected = false; continue; }
        result += `${connected ? "L" : "M"}${point[0].toFixed(1)},${point[1].toFixed(1)} `;
        connected = true;
    }
    return result;
}

export function Sparkline(element, points, key, color)
{
    const values = points.slice(-40);
    element.setAttribute("viewBox", "0 0 90 44");
    element.replaceChildren();
    if (values.length < 2) return;
    const maximum = Math.max(1, ...values.map(point => point[key] || 0));
    const path = Path(values.map((point, index) => point[key] == null ? null : [index / (values.length - 1) * 90, 38 - point[key] / maximum * 28]));
    element.append(Svg("path", { d: path, fill: "none", stroke: color, "stroke-width": 1.3 }));
}

export function TrafficChart(element, history, seconds, metric = "bytes")
{
    const series = metric === "messages" ? [["publications", "#75e5bd"]] : [["receivedBytes", "#75e5bd"], ["sentBytes", "#a099ff"]];
    const label = metric === "messages" ? value => `${NumberText(value)} msg/s` : value => ByteText(value, true);
    const width = Math.max(360, element.clientWidth || 650);
    const height = element.clientHeight || 180;
    const left = 68, right = width - 22, top = 18, bottom = height - 30;
    const end = history.at(-1)?.time || Date.now();
    const start = end - seconds * 1000;
    const values = history.filter(point => point.time >= start);
    const maximum = Math.max(metric === "messages" ? 10 : 1024, ...values.flatMap(point => series.map(([key]) => point[key] || 0)));
    const scale = Math.pow(10, Math.floor(Math.log10(maximum)));
    const ceiling = Math.ceil(maximum / scale) * scale;
    element.setAttribute("viewBox", `0 0 ${width} ${height}`);
    const fragment = document.createDocumentFragment();
    const definitions = Svg("defs");
    const gradient = Svg("linearGradient", { id: "traffic-fill", x1: 0, y1: 0, x2: 0, y2: 1 });
    gradient.append(Svg("stop", { offset: "0%", "stop-color": "#75e5bd", "stop-opacity": 0.12 }),
        Svg("stop", { offset: "100%", "stop-color": "#75e5bd", "stop-opacity": 0 }));
    definitions.append(gradient);
    fragment.append(definitions);
    for (let index = 0; index <= 3; ++index)
    {
        const y = top + (bottom - top) * index / 3;
        fragment.append(Svg("line", { x1: left, x2: right, y1: y, y2: y, class: "chart-grid" }));
        fragment.append(Svg("text", { x: left - 12, y: y + 3, "text-anchor": "end", class: "chart-label" }, label(ceiling * (1 - index / 3))));
    }
    for (let index = 0; index <= 4; ++index)
    {
        const time = start + seconds * 1000 * index / 4;
        const label = index === 4 ? "Now" : new Date(time).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: seconds <= 60 ? "2-digit" : undefined });
        fragment.append(Svg("text", { x: left + (right - left) * index / 4, y: height - 8,
            "text-anchor": index === 0 ? "start" : index === 4 ? "end" : "middle", class: "chart-label" }, label));
    }
    for (const [key, color] of series)
    {
        const points = [];
        let previous = null;
        for (const point of values)
        {
            if (previous && point.time - previous.time > 3500) points.push(null);
            points.push(point[key] == null ? null : [left + (point.time - start) / (seconds * 1000) * (right - left), bottom - point[key] / ceiling * (bottom - top)]);
            previous = point;
        }
        fragment.append(Svg("path", { d: Path(points), fill: "none", stroke: color, "stroke-width": 1.7, "stroke-linecap": "round", "stroke-linejoin": "round" }));
        const last = points.at(-1);
        if (last) fragment.append(Svg("circle", { cx: last[0], cy: last[1], r: 2.5, fill: color }));
    }
    if (!values.some(point => point[series[0][0]] != null))
        fragment.append(Svg("text", { x: (left + right) / 2, y: (top + bottom) / 2, class: "chart-label", "text-anchor": "middle" }, "Waiting for two live samples"));
    element.replaceChildren(fragment);
}
