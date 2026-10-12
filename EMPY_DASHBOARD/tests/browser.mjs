// Optional browser QA; Playwright is a test tool, not a dashboard dependency.
// Run against demo.py. EMPY_PLAYWRIGHT_MODULE may point to an existing package.
import assert from "node:assert/strict";
import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
const { chromium } = await import(process.env.EMPY_PLAYWRIGHT_MODULE || "playwright");
const output = path.resolve(process.env.EMPY_QA_OUTPUT || "Targets/DashboardQA");
const url = process.env.EMPY_DASHBOARD_URL || "http://127.0.0.1:8080";
await mkdir(output, { recursive: true });
const browser = await chromium.launch({ headless: true,
    ...(process.env.EMPY_BROWSER_EXECUTABLE ? { executablePath: process.env.EMPY_BROWSER_EXECUTABLE } : {}) });
const context = await browser.newContext({ viewport: { width: 1440, height: 1050 }, deviceScaleFactor: 1 });
const page = await context.newPage();
const errors = [];
page.on("pageerror", error => errors.push(error.message));
page.on("console", message => { if (message.type() === "error" && !message.text().includes("favicon")) errors.push(message.text()); });
const evidence = [];

async function Check(name, action)
{
    await action();
    evidence.push(name);
    console.log("PASS:", name);
}

try
{
    const response = await page.request.get(url + "/api/state");
    const state = await response.json();
    const live = state.sources.filter(source => source.state === "online");
    assert(live.length > 0, "Start demo.py or a live monitored broker before browser QA");
    await page.goto(url, { waitUntil: "networkidle" });
    await page.waitForFunction(() => document.getElementById("stat-brokers").textContent !== "—");
    await Check("Live overview matches broker inventory", async () =>
    {
        assert.equal((await page.locator("#stat-brokers").innerText()).trim(), String(live.length));
        assert.equal(await page.locator(".graph-node[data-source]").count(), state.sources.length);
        assert(await page.locator("#traffic-chart path").count() >= 2);
        await page.screenshot({ path: path.join(output, "overview-desktop.png"), fullPage: true });
    });
    await Check("Broker selection and topology controls", async () =>
    {
        const source = live.at(-1);
        await page.locator(".graph-node[data-source]").filter({ has: page.locator("title", { hasText: source.name }) }).click();
        assert.equal(await page.locator(".detail-name").innerText(), source.name);
        const before = await page.locator("#topology").getAttribute("viewBox");
        await page.getByRole("button", { name: "Zoom in", exact: true }).click();
        assert.notEqual(await page.locator("#topology").getAttribute("viewBox"), before);
        await page.getByRole("button", { name: "Fit network to view" }).click();
        assert.equal(await page.locator("#topology").getAttribute("viewBox"), before);
        await page.locator("#show-clients").uncheck();
        assert.equal(await page.locator(".client-dot").count(), 0);
        await page.locator("#show-clients").check();
        // Fabrics above 12 brokers use the compact ring without client leaves.
        if (state.sources.length > 12) assert(await page.locator("#topology.large").count() === 1);
        else assert(await page.locator(".client-dot").count() > 0);
    });
    await Check("Pause and resume retain a readable snapshot", async () =>
    {
        await page.getByRole("button", { name: "Pause", exact: true }).click();
        assert.equal(await page.locator("#pause").getAttribute("aria-pressed"), "true");
        assert((await page.locator("#notice").innerText()).includes("paused"));
        await page.getByRole("button", { name: "Resume", exact: true }).click();
        await page.waitForFunction(() => document.getElementById("pause").getAttribute("aria-pressed") === "false");
    });
    await Check("Pausing discards an already pending refresh", async () =>
    {
        let release;
        let observed;
        const gate = new Promise(resolve => { release = resolve; });
        const started = new Promise(resolve => { observed = resolve; });
        await page.route("**/api/state", async route =>
        {
            const response = await route.fetch();
            const body = await response.json();
            body.generatedAt += 60000;
            observed();
            await gate;
            await route.fulfill({ response, json: body });
        });
        await started;
        const before = await page.locator("#footer-update").innerText();
        await page.locator("#pause").click();
        release();
        await page.waitForFunction(() => !document.getElementById("refresh").disabled);
        assert.equal(await page.locator("#footer-update").innerText(), before);
        await page.unroute("**/api/state");
        await page.locator("#pause").click();
    });
    await Check("Broker inventory opens the selected source", async () =>
    {
        await page.locator('[data-view="brokers"]').click();
        await page.waitForFunction(() => document.getElementById("page-title").textContent === "Broker inventory");
        await page.locator("#broker-table tbody tr").first().waitFor({ state: "visible" });
        assert.equal(await page.locator("#broker-table tbody tr").count(), state.sources.length);
        await page.getByRole("button", { name: "Msg/s" }).click();
        assert.equal(await page.locator('#broker-table th[aria-sort="descending"]').count(), 1);
        await page.screenshot({ path: path.join(output, "brokers-desktop.png"), fullPage: true });
        await page.locator("#broker-table .session-link").first().click();
        await page.waitForFunction(() => document.getElementById("page-title").textContent === "Network overview");
        assert.equal(await page.locator("#page-title").innerText(), "Network overview");
    });
    await Check("Session filters, search and detailed counters", async () =>
    {
        await page.locator('[data-view="sessions"]').click();
        await page.waitForFunction(() => document.getElementById("page-title").textContent === "Client sessions");
        const first = (await (await page.request.get(url + "/api/sessions?state=online&size=1")).json()).rows[0];
        assert(first, "The browser checks need at least one real connected client");
        const session = first.session;
        await page.locator("#session-broker").selectOption(first.source.id);
        await page.locator("#session-search").fill(session.identifier);
        await page.waitForFunction(() => document.querySelectorAll("#session-table tbody tr").length === 1);
        await page.locator("#session-table .session-link").click();
        assert.equal(await page.locator("#session-detail-id").innerText(), session.identifier);
        assert(await page.locator("#session-inspector").isVisible());
        await page.screenshot({ path: path.join(output, "session-details.png"), fullPage: true });
        await page.locator("#session-search").fill("no-such-session");
        await page.waitForFunction(() => document.getElementById("session-table").innerText.includes("No sessions"));
        await page.locator("#session-search").fill("");
        await page.locator("#session-broker").selectOption("all");
        await page.locator("#session-role").selectOption("Publisher");
        await page.waitForTimeout(500);
        for (const role of await page.locator("#session-table .role-list").allTextContents()) assert(role.includes("Publisher"));
        await page.locator("#session-role").selectOption("all");
    });
    await Check("Read-only JSON export contains observations", async () =>
    {
        await page.locator('[data-view="activity"]').click();
        const downloadEvent = page.waitForEvent("download");
        await page.locator("#export").click();
        const download = await downloadEvent;
        await download.saveAs(path.join(output, "export.json"));
        assert(download.suggestedFilename().startsWith("empy-observations-"));
    });
    await Check("Mobile layout, navigation and session table stay within viewport", async () =>
    {
        await page.setViewportSize({ width: 390, height: 844 });
        await page.locator('[data-view="overview"]').click();
        await page.waitForFunction(() => document.getElementById("page-title").textContent === "Network overview");
        await page.evaluate(() => window.scrollTo(0, 0));
        assert(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth));
        await page.screenshot({ path: path.join(output, "overview-mobile.png"), fullPage: true });
        await page.locator('[data-view="sessions"]').click();
        await page.waitForFunction(() => document.getElementById("page-title").textContent === "Client sessions");
        assert(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth));
        await page.screenshot({ path: path.join(output, "sessions-mobile.png"), fullPage: true });
    });
    await Check("Unreachable sources, invalid navigation and text escaping", async () =>
    {
        // A UI fixture, isolated to this browser page; the demo remains live.
        const unavailable = { ...state, history: [], events: [], sources: state.sources.map(source =>
            ({ ...source, name: "<img src=x onerror=alert(1)>", data: null, state: "unreachable", rates: null })) };
        await page.route("**/api/state", route => route.fulfill({ json: unavailable }));
        await page.setViewportSize({ width: 1440, height: 1050 });
        await page.goto(url + "/#constructor", { waitUntil: "networkidle" });
        await page.locator("#graph-empty").waitFor({ state: "visible" });
        assert.equal(await page.locator("#page-title").innerText(), "Network overview");
        assert(await page.locator("#graph-empty").isVisible());
        assert.equal(await page.locator(".graph-node").count(), 0);
        assert.equal(await page.locator("#stat-clients").innerText(), "—");
        assert.equal(await page.locator("img").count(), 0);
        assert.equal(await page.locator(".detail-name").innerText(), "<img src=x onerror=alert(1)>");
        await page.screenshot({ path: path.join(output, "unreachable-fixture.png"), fullPage: true });
        await page.unroute("**/api/state");
    });
    await Check("No browser JavaScript or resource errors", async () => assert.deepEqual(errors, []));
    await writeFile(path.join(output, "browser-result.json"), JSON.stringify({ url, checks: evidence, errors }, null, 4));
}
catch (error)
{
    await page.screenshot({ path: path.join(output, "failure.png"), fullPage: true });
    await writeFile(path.join(output, "browser-failure.json"), JSON.stringify({ message: error.message, checks: evidence, errors }, null, 4));
    throw error;
}
finally { await browser.close(); }
