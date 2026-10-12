#include <eMQ/Network/QuicPolicy.h>

namespace eMQ
{
    QuicPlan::QuicPlan(QuicPolicy policy): m_Policy(std::move(policy))
    {
        if (!m_Policy.strategy) m_Policy.strategy = MMQP_QUIC_STRATEGY_SINGLE;
        auto strategy = *m_Policy.strategy;
        if (strategy != MMQP_QUIC_STRATEGY_SINGLE && strategy != MMQP_QUIC_STRATEGY_SEMANTIC
            && strategy != MMQP_QUIC_STRATEGY_BULK)
            throw ProtocolError("This transport does not provide an application-defined stream allocation strategy",
                MMQP_REASON_POLICY_VIOLATION);
        if (m_Policy.bulk && (strategy != MMQP_QUIC_STRATEGY_BULK || !m_Policy.bulk->streams
            || m_Policy.bulk->streams > 32 || !m_Policy.bulk->threshold))
            throw ProtocolError("Bulk policy requires SAS 3, 1..32 streams and a positive threshold",
                MMQP_REASON_POLICY_VIOLATION);
        if (strategy == MMQP_QUIC_STRATEGY_BULK && !m_Policy.bulk) m_Policy.bulk = BulkPolicy{};
        if (m_Policy.datagram && !*m_Policy.datagram)
            throw ProtocolError("Datagram payload threshold must be positive", MMQP_REASON_POLICY_VIOLATION);
    }

    QuicPolicy QuicPlan::Negotiate(const QuicPolicy& requested, const QuicPolicy& limits)
    {
        auto result = QuicPlan(requested).Policy();
        auto maximum = QuicPlan(limits).Policy();
        if (*maximum.strategy == MMQP_QUIC_STRATEGY_SINGLE) result.strategy = MMQP_QUIC_STRATEGY_SINGLE;
        else if (*result.strategy == MMQP_QUIC_STRATEGY_BULK && *maximum.strategy != MMQP_QUIC_STRATEGY_BULK)
            result.strategy = MMQP_QUIC_STRATEGY_SEMANTIC;
        if (*result.strategy != MMQP_QUIC_STRATEGY_BULK) result.bulk.reset();
        else
        {
            result.bulk->streams = std::min(result.bulk->streams, maximum.bulk->streams);
            result.bulk->threshold = std::max(result.bulk->threshold, maximum.bulk->threshold);
        }
        if (result.datagram && maximum.datagram) result.datagram = std::min(*result.datagram, *maximum.datagram);
        else result.datagram.reset();
        return result;
    }

    QuicChannel QuicPlan::Select(const PacketSummary& packet, size_t frameSize, uint16_t datagramLimit, size_t activeBulk) const
    {
        if (!packet.Publication()) return QuicChannel::Control;
        if (!packet.qos && m_Policy.datagram && packet.payload <= *m_Policy.datagram
            && frameSize <= datagramLimit) return QuicChannel::Datagram;
        if (*m_Policy.strategy == MMQP_QUIC_STRATEGY_SINGLE) return QuicChannel::Control;
        if (m_Policy.bulk && packet.payload > m_Policy.bulk->threshold
            && activeBulk < m_Policy.bulk->streams) return QuicChannel::Bulk;
        return packet.qos ? QuicChannel::Reliable : QuicChannel::BestEffort;
    }

    void QuicPlan::Validate(QuicChannel channel, const PacketSummary& packet, size_t frameSize) const
    {
        auto publication = packet.Publication() ? &packet : nullptr;
        bool valid = false;
        switch (channel)
        {
        case QuicChannel::Control:
            valid = !publication || *m_Policy.strategy == MMQP_QUIC_STRATEGY_SINGLE;
            break;
        case QuicChannel::BestEffort:
        case QuicChannel::Reliable:
            valid = publication && *m_Policy.strategy != MMQP_QUIC_STRATEGY_SINGLE
                && publication->qos == (channel == QuicChannel::Reliable ? 1 : 0);
            break;
        case QuicChannel::Bulk:
            valid = publication && m_Policy.bulk && publication->payload > m_Policy.bulk->threshold;
            break;
        case QuicChannel::Datagram:
            valid = publication && !publication->qos && m_Policy.datagram
                && publication->payload <= *m_Policy.datagram && frameSize <= UINT16_MAX;
            break;
        }
        if (!valid) throw ProtocolError("Packet arrived on an incompatible QUIC channel", MMQP_REASON_POLICY_VIOLATION);
    }

    const QuicPolicy& QuicPlan::Policy() const { return m_Policy; }
}
