#include "Checks.h"
#include <eMQ/Cluster/Replication.h>

using namespace eMQ;

namespace
{
    ProfileAssertion Assertion(std::string origin, uint64_t version, std::string broker = "b")
    {
        ProfileEntry profile;
        profile.type = MMQP_PROREQ_TYPE_SUB;
        profile.topic = "tenant/+/temperature";
        profile.qos = 1;
        profile.request = true;
        return {{std::move(origin), version}, std::move(broker), "sensor", profile};
    }

    void Placement()
    {
        FabricOptions options;
        for (size_t i = 0; i < 31; ++i) options.brokers.push_back("broker-" + std::to_string(i));
        Fabric fabric(options);
        std::reverse(options.brokers.begin(), options.brokers.end());
        Fabric shuffled(options);
        EMPY_CHECK(fabric.Shard("//tenant/room//temperature/") == fabric.Shard("tenant/room/temperature"));
        for (uint32_t shard = 0; shard < options.shards; ++shard)
        {
            auto replicas = fabric.Replicas(shard);
            EMPY_CHECK(replicas == shuffled.Replicas(shard));
            EMPY_CHECK(replicas.size() == 3 && std::set<std::string>(replicas.begin(), replicas.end()).size() == 3);
        }
        EMPY_CHECK(fabric.SessionReplicas("SID") == shuffled.SessionReplicas("SID"));
        EMPY_CHECK(fabric.ProfileShards("tenant/#").size() == options.shards);
        EMPY_CHECK(fabric.ProfileShards("$share/workers/tenant/+/temperature").size() == options.shards);
        EMPY_CHECK(fabric.ProfileShards("tenant/room/temperature") == std::vector<uint32_t>{fabric.Shard("tenant/room/temperature")});
        Throws([&] { fabric.Replicas(options.shards); });
        auto tree = fabric.Tree("tenant/room/temperature", "broker-15", options.brokers);
        EMPY_CHECK(tree.brokers.front() == "broker-15" && tree.brokers.size() == options.brokers.size());
        EMPY_CHECK(tree.brokers == shuffled.Tree("tenant/room/temperature", "broker-15", options.brokers).brokers);
        EMPY_CHECK(!tree.Parent(0));
        EMPY_CHECK(tree.Subtree(0).size() == options.brokers.size());
        for (size_t rank = 1; rank < tree.brokers.size(); ++rank)
        {
            auto parent = tree.Parent(rank);
            EMPY_CHECK(parent && *parent < rank && tree.Edge(tree.brokers[*parent], tree.brokers[rank]));
            EMPY_CHECK(!tree.Edge(tree.brokers[rank], tree.brokers[*parent]));
            auto children = tree.Children(*parent);
            EMPY_CHECK(std::find(children.begin(), children.end(), rank) != children.end());
            EMPY_CHECK(children.size() <= options.fanout);
        }
        auto duplicates = options.brokers;
        duplicates.push_back(duplicates.back());
        EMPY_CHECK(fabric.Tree("tenant/t", "broker-15", duplicates).brokers.size() == options.brokers.size());
        Throws([&] { fabric.Tree("tenant/t", "unknown", options.brokers); });
        Throws([&] { fabric.Tree("tenant/t", "broker-15", {"unknown"}); });
        options.brokers.push_back(options.brokers.front());
        Throws([&] { Fabric invalid(options); });

        TenantPolicy tenant{"tenant", {fabric.Shard("tenant/t")}};
        EMPY_CHECK(tenant.Allows(fabric, "tenant/t"));
        EMPY_CHECK(!tenant.Allows(fabric, "tenant-other/t"));
        EMPY_CHECK(!tenant.Allows(fabric, "other/t"));
        // A wildcard needs every shard it can match; an empty allowlist is unrestricted.
        EMPY_CHECK(!tenant.Allows(fabric, "tenant/#") && !tenant.Allows(fabric, "tenant/+/temperature"));
        std::set<uint32_t> all;
        for (uint32_t shard = 0; shard < options.shards; ++shard) all.insert(shard);
        TenantPolicy everywhere{"tenant", all};
        EMPY_CHECK(everywhere.Allows(fabric, "tenant/#") && everywhere.Allows(fabric, "tenant/+/temperature"));
        EMPY_CHECK(!everywhere.Allows(fabric, "other/#"));
        TenantPolicy unrestricted{"tenant", {}};
        EMPY_CHECK(unrestricted.Allows(fabric, "tenant/#") && unrestricted.Allows(fabric, "$bwm/tenant/device"));
        EMPY_CHECK(!unrestricted.Allows(fabric, "other/#") && !unrestricted.Allows(fabric, "$bwm/other/device"));
    }

    void Forwarding()
    {
        FabricOptions options;
        options.brokers = {"a", "b", "c", "d", "e"};
        Fabric fabric(options);
        Buffer key(32, 0x5a);
        ForwardRecord record;
        record.acceptedEpoch = record.routingEpoch = 1;
        record.sequence = 1;
        record.expires = 1000;
        record.session = "SID";
        record.ingress = "a";
        record.destinations = {"b", "c", "d", "e"};
        record.publication.topic = "tenant/t";
        record.publication.alias = 1;
        record.publication.payload = {1, 2, 3};
        record.identity = MessageIdentity(record.session, record.sequence, record.publication.topic,
            record.acceptedEpoch, record.publication.payload);
        auto bytes = FabricCodec::Encode(record, key);
        auto decoded = FabricCodec::Decode(bytes, key);
        EMPY_CHECK(decoded.identity == record.identity && decoded.publication.payload == record.publication.payload);
        auto tree = fabric.Tree(record.publication.topic, record.ingress, record.destinations);
        for (size_t rank = 1; rank < tree.brokers.size(); ++rank)
        {
            decoded.senderRank = static_cast<uint32_t>(*tree.Parent(rank));
            auto validated = FabricCodec::Validate(decoded, fabric, tree.brokers[decoded.senderRank], tree.brokers[rank], 1);
            EMPY_CHECK(validated.brokers == tree.brokers);
            Throws([&] { FabricCodec::Validate(decoded, fabric, "unknown", tree.brokers[rank], 1); });
            Throws([&] { FabricCodec::Validate(decoded, fabric, tree.brokers[decoded.senderRank], tree.brokers[rank], 1000); });
        }
        // Relays rewrite only the authenticated sender rank, byte-identical to a re-encoding.
        auto relayed = FabricCodec::Encode(record, key);
        auto parsed = FabricCodec::Decode(relayed, key);
        FabricCodec::Rebind(relayed, parsed, 2, key);
        auto ranked = record;
        ranked.senderRank = 2;
        EMPY_CHECK(relayed == FabricCodec::Encode(ranked, key) && FabricCodec::Decode(relayed, key).senderRank == 2);
        Throws([&] { FabricCodec::Rebind(relayed, parsed, 1, key); });
        bytes[8] ^= 1;
        Throws([&] { FabricCodec::Decode(bytes, key); });
        auto wrongKey = key;
        wrongKey[0] ^= 1;
        Throws([&] { FabricCodec::Decode(FabricCodec::Encode(record, key), wrongKey); });
        Throws([&] { FabricCodec::Decode(FabricCodec::Encode(record, key), key, 64); });
        Throws([&] { FabricCodec::Decode(FabricCodec::Encode(record, key), key, 10000, 2); });
        EpochFilter filter(1, 2);
        EMPY_CHECK(filter.Accept(1, record.identity) && !filter.Accept(1, record.identity));
        filter.Advance(2);
        record.routingEpoch = 2;
        options.epoch = 2;
        Fabric next(options);
        EMPY_CHECK(!filter.Accept(2, record.identity));
        record.sequence = 2;
        record.identity = MessageIdentity(record.session, record.sequence, record.publication.topic,
            record.acceptedEpoch, record.publication.payload);
        EMPY_CHECK(filter.Accept(2, record.identity));
        record.sequence = 3;
        record.identity = MessageIdentity(record.session, record.sequence, record.publication.topic,
            record.acceptedEpoch, record.publication.payload);
        EMPY_CHECK(filter.Accept(2, record.identity));
        Throws([&] { filter.Accept(2, Digest{}); });
        Throws([&] { filter.Advance(1); });
        filter.Advance(3);
        Throws([&] { filter.Accept(1, Digest{}); });
    }

    void Convergence()
    {
        std::set<std::string> replicas{"a", "b", "c"};
        ObservedProfiles a("a", replicas), b("b", replicas), c("c", replicas);
        auto first = Assertion("a", 1);
        a.Add(first);
        auto stale = a;
        b.Merge(a);
        auto seen = b.Observe("b", "sensor", "tenant/+/temperature");
        EMPY_CHECK(seen.size() == 1);
        b.Remove({{"b", 1}, seen});
        // Removal can arrive before the assertion it names.
        c.Remove({{"b", 1}, seen});
        c.Add(first);
        EMPY_CHECK(c.Destinations("tenant/room/temperature", 1).empty());
        // A concurrent assertion survives an observed-remove of the first dot.
        auto concurrent = Assertion("a", 2, "c");
        a.Add(concurrent);
        a.Merge(b);
        b.Merge(a);
        c.Merge(b);
        for (auto* replica : {&a, &b, &c})
            EMPY_CHECK(replica->Destinations("tenant/room/temperature", 1) == std::vector<std::string>{"c"});
        a.Compact();
        EMPY_CHECK(a.Tombstones() == 1);
        a.Acknowledge("b", b.Versions());
        a.Acknowledge("c", VersionVector{{"a", 2}});
        a.Compact();
        EMPY_CHECK(a.Tombstones() == 1); // c has not acknowledged b's removal.
        a.Acknowledge("c", c.Versions());
        a.Compact();
        EMPY_CHECK(a.Tombstones() == 0);
        a.Merge(stale);
        EMPY_CHECK(a.Destinations("tenant/room/temperature", 1) == std::vector<std::string>{"c"});
        stale.Merge(a);
        EMPY_CHECK(stale.Destinations("tenant/room/temperature", 1) == std::vector<std::string>{"c"});
        a.Add(first);
        EMPY_CHECK(a.Observe("b", "sensor", "tenant/+/temperature").empty());
        Throws([&] { a.Acknowledge("c", {}); });
        Throws([&] { a.Acknowledge("unknown", {}); });

        ObservedProfiles gaps("a", replicas);
        gaps.Add(Assertion("a", 3));
        EMPY_CHECK(gaps.Versions().at("a") == 0);
        gaps.Add(first);
        EMPY_CHECK(gaps.Versions().at("a") == 1);
        ObservedProfiles middle("b", replicas);
        middle.Add(first);
        middle.Add(concurrent);
        gaps.Merge(middle);
        EMPY_CHECK(gaps.Versions().at("a") == 3);
        auto conflict = first;
        conflict.broker = "different";
        Throws([&] { gaps.Add(conflict); });
        Throws([&] { gaps.Remove({first.dot, {concurrent.dot}}); });
        Throws([&] { gaps.Add(Assertion("a", 0)); });
        ObservedProfiles limited("a", replicas, 2);
        limited.Add(first);
        Throws([&] { limited.Remove({{"b", 1}, {first.dot}}); });
        limited.Add(concurrent);
        Throws([&] { limited.Add(Assertion("a", 3)); });
        EMPY_CHECK(limited.Destinations("tenant/room/temperature", 1).size() == 2);
    }

    void QuorumsAndBudgets()
    {
        SessionQuorum quorum(7, {"a", "b", "c"}, 64);
        EMPY_CHECK(quorum.Required() == 2 && quorum.CommittedVersion() == 0);
        Throws([&] { quorum.Snapshot(); });
        auto hash = quorum.Propose(1, {1, 2, 3});
        EMPY_CHECK(!quorum.Acknowledge("a", 7, 1, hash));
        EMPY_CHECK(!quorum.Acknowledge("a", 7, 1, hash));
        Throws([&] { quorum.Acknowledge("b", 6, 1, hash); });
        Throws([&] { quorum.Acknowledge("unknown", 7, 1, hash); });
        Throws([&] { quorum.Acknowledge("b", 7, 1, Digest{}); });
        Throws([&] { quorum.Propose(1, {2, 3, 4}); });
        EMPY_CHECK(quorum.Acknowledge("b", 7, 1, hash));
        EMPY_CHECK(quorum.CommittedVersion() == 1 && quorum.Snapshot() == Buffer({1, 2, 3}));
        auto second = quorum.Propose(2, {4, 5});
        EMPY_CHECK(!quorum.Acknowledge("c", 7, 2, second));
        EMPY_CHECK(quorum.Snapshot() == Buffer({1, 2, 3}));
        EMPY_CHECK(quorum.Acknowledge("b", 7, 2, second));
        EMPY_CHECK(quorum.Snapshot() == Buffer({4, 5}));

        AdmissionBudget budget;
        budget.maxRecords = 1;
        budget.maxBytes = 10;
        EMPY_CHECK(budget.Reserve(1, 7, true) == Admission::Accept);
        EMPY_CHECK(budget.Reserve(0, 1, true) == Admission::Drop);
        EMPY_CHECK(budget.Reserve(1, 1, true) == Admission::Reject);
        EMPY_CHECK(budget.Reserve(1, 1, true, true) == Admission::Defer);
        Throws([&] { budget.Release(8); });
        budget.Release(7);
        EMPY_CHECK(budget.Bytes() == 0 && budget.Records() == 0);
        EMPY_CHECK(budget.Reserve(1, 1, false) == Admission::Reject);
        EMPY_CHECK(budget.Reserve(0, 11, true) == Admission::Drop);
    }
}

void TestCluster()
{
    Placement();
    Forwarding();
    Convergence();
    QuorumsAndBudgets();
}
