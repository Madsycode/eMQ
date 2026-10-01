#pragma once
#ifndef ASIO_STANDALONE
    #define ASIO_STANDALONE
#endif
#include "Connection.h"
#include <asio.hpp>
#include <future>

namespace eMQ
{
    struct EMPY_MQ_API IoContext
    {
        using Accept = std::function<void(Connection::Pointer)>;

        IoContext();
        ~IoContext();
        IoContext(const IoContext&) = delete;
        IoContext& operator=(const IoContext&) = delete;

        uint16_t Listen(std::string_view host, uint16_t port, TransportOptions options,
            Accept accept, size_t maxConnections = 1024);
        Connection::Pointer Connect(std::string host, uint16_t port, TransportOptions options,
            Accept connected, Connection::Closed closed);
        asio::any_io_executor Executor();
        void Init(uint32_t workers = 1);
        void Stop();
        void Post(std::function<void()> task);

        template <typename _Task, typename..._Args>
        auto Submit(_Task&& task, _Args&&...args)
            -> std::future<std::invoke_result_t<std::decay_t<_Task>, std::decay_t<_Args>...>>
        {
            using Result = std::invoke_result_t<std::decay_t<_Task>, std::decay_t<_Args>...>;
            auto work = std::make_shared<std::packaged_task<Result()>>(std::bind(
                std::forward<_Task>(task), std::forward<_Args>(args)...));
            auto result = work->get_future();
            this->Post([work] { (*work)(); });
            return result;
        }

    private:
        struct Impl;
        std::unique_ptr<Impl> m_Impl;
    };
}
