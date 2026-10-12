import assert from "node:assert/strict";
import test from "node:test";
import { Topology, Totals, Mesh, BrokerRows, Roles, Bytes, Duration } from "../web/model.js";

function Source(id, peer, state = "online", cluster = "fabric-A")
{
    return { id, name: id, state, rates: { receivedBytes: 10, sentBytes: 20, publications: 2 }, data:
    {
        identifier: id, cluster: { identifier: cluster, peers: [{ identifier: peer, connected: true }] },
        metrics: { online: 1, sessions: 2, pending: 3, retainedBytes: 4 }, omittedSessions: 0, requests: 1, uptimeMs: 5,
        preview: [{ identifier: "sid", online: true, roles: ["Publisher"] }]
    } };
}

test("topology requires fresh reports from both endpoints for a confirmed edge", () =>
{
    const a = Source("a", "b"), b = Source("b", "a");
    const network = Topology([a, b]);
    assert.equal(network.nodes.length, 2);
    assert.equal(network.edges.length, 1);
    assert.equal(network.edges[0].state, "connected");
    b.state = "stale";
    assert.equal(Topology([a, b]).edges[0].state, "unconfirmed");
    a.data.cluster.peers[0].connected = false;
    assert.equal(Topology([a, b]).edges[0].observed, false);
});

test("discovery keeps unmonitored peers visible without inventing their metrics", () =>
{
    const network = Topology([Source("a", "b")]);
    assert.equal(network.nodes.length, 2);
    assert.equal(network.nodes[1].state, "unmonitored");
    assert.equal(network.nodes[1].source, null);
    assert.equal(network.edges[0].state, "unconfirmed");
    assert.equal(network.edges[0].observed, true);
});

test("different cluster manifests never merge identical broker identifiers", () =>
{
    const a = Source("a", "b"), other = Source("a", "b", "online", "fabric-B");
    other.id = "other-source";
    const network = Topology([a, other]);
    assert.equal(network.nodes.length, 4);
    assert.equal(network.edges.length, 2);
});

test("stale telemetry does not contribute to live totals or imply online clients", () =>
{
    const a = Source("a", "b"), b = Source("b", "a", "unreachable");
    assert.deepEqual(Totals([a, b]), { reporting: 1, configured: 2, clients: 1, sessions: 2, pending: 3,
        retainedBytes: 4, receivedBytes: 10, sentBytes: 20, publications: 2, requests: 1, omitted: 0, storedBytes: null });
    assert.equal(Totals([b]).receivedBytes, null);
});

test("roles distinguish request roles from ordinary publish/subscribe profiles", () =>
{
    assert.deepEqual(Roles({ requesters: 1, responders: 1, registrations: 1, subscriptions: 1 }), ["Requester", "Responder"]);
    assert.deepEqual(Roles({ requesters: 0, responders: 0, registrations: 0, subscriptions: 0 }), ["Connected"]);
});

test("mesh health counts confirmed links against a full mesh per manifest", () =>
{
    const names = ["a", "b", "c"];
    const sources = names.map(name =>
    {
        const source = Source(name, "x");
        source.data.cluster.peers = names.filter(peer => peer !== name).map(peer => ({ identifier: peer, connected: true }));
        return source;
    });
    assert.deepEqual(Mesh(sources), { clusters: 1, brokers: 3, expected: 3, confirmed: 3 });
    sources[2].state = "unreachable";
    assert.equal(Mesh(sources).confirmed, 1);
    assert.deepEqual(Mesh([]), { clusters: 0, brokers: 0, expected: 0, confirmed: 0 });
});

test("broker rows search, sort by load and keep unavailable values last", () =>
{
    const a = Source("alpha", "beta"), b = Source("beta", "alpha"), c = Source("gamma", "alpha", "unreachable");
    b.rates.publications = 50;
    c.rates = null;
    const rows = BrokerRows([a, b, c], { sort: "messages", descending: true });
    assert.deepEqual(rows.map(row => row.name), ["beta", "alpha", "gamma"]);
    assert.equal(rows[2].clients, null);
    assert.deepEqual(BrokerRows([a, b, c], { search: "GAM" }).map(row => row.name), ["gamma"]);
    assert.equal(BrokerRows([a]).at(0).links, 1);
});

test("empty networks and unavailable counters remain explicit", () =>
{
    assert.deepEqual(Topology([]), { nodes: [], edges: [] });
    assert.equal(Totals([]).receivedBytes, null);
    assert.equal(Bytes(null).value, "—");
    assert.deepEqual(Bytes(2048, true), { value: "2", unit: "KiB/s" });
    assert.equal(Duration(61000), "1m 1s");
});

test("two telemetry sources for one broker cannot confirm both ends of a link", () =>
{
    const a = Source("a", "b"), alias = Source("a", "b");
    alias.id = "alias";
    const topology = Topology([a, alias]);
    assert.equal(topology.nodes.length, 2);
    assert.equal(topology.edges[0].state, "unconfirmed");
    assert.equal(topology.nodes[0].source.id, "a");
    a.data.cluster = alias.data.cluster = null;
    a.data.instance = alias.data.instance = "same-instance";
    assert.equal(Topology([a, alias]).nodes.length, 1);
});
