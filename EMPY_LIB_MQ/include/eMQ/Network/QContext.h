#pragma once
#include "IOContext.h"

namespace eMQ
{
    struct EMPY_MQ_API QcContext
    {
        explicit QcContext(asio::any_io_executor executor);
        ~QcContext();
        QcContext(const QcContext&) = delete;
        QcContext& operator=(const QcContext&) = delete;

        uint16_t Listen(std::string_view host, uint16_t port, TransportOptions options,
            IoContext::Accept accept, size_t maxConnections);
        Connection::Pointer Connect(std::string host, uint16_t port, TransportOptions options,
            IoContext::Accept connected, Connection::Closed closed);
        void Stop();
        static bool Available();

    private:
        struct Impl;
        std::shared_ptr<Impl> m_Impl;
    };
}
