#pragma once

#include "common/zmq_transport.hpp"

#include <atomic>
#include <future>
#include <thread>

namespace zmq_template::server::test {

// A directed link proxy. Every socket belongs to its worker thread.
class FaultProxy {
public:
    FaultProxy(std::string endpoint, std::string target) {
        std::promise<void> ready;
        auto started = ready.get_future();
        worker_ = std::thread([this, endpoint, target, ready = std::move(ready)]() mutable {
            try {
                zmq::context_t context{1};
                zmq::socket_t incoming{context, zmq::socket_type::router};
                configure_socket(incoming);
                incoming.set(zmq::sockopt::linger, 0);
                incoming.bind(endpoint);
                ready.set_value();
                while (!stop_) {
                    zmq::pollitem_t item{incoming.handle(), 0, ZMQ_POLLIN, 0};
                    zmq::poll(&item, 1, std::chrono::milliseconds{5});
                    if (!(item.revents & ZMQ_POLLIN)) continue;
                    std::string route;
                    auto message = receive_routed_message(incoming, route);
                    if (drop_requests) continue;
                    zmq::socket_t outgoing{context, zmq::socket_type::req};
                    configure_socket(outgoing);
                    outgoing.set(zmq::sockopt::linger, 0);
                    outgoing.set(zmq::sockopt::sndtimeo, 0);
                    outgoing.connect(target);
                    try {
                        send_message(outgoing, message);
                        zmq::pollitem_t reply{outgoing.handle(), 0, ZMQ_POLLIN, 0};
                        zmq::poll(&reply, 1, std::chrono::milliseconds{200});
                        if (!(reply.revents & ZMQ_POLLIN)) continue;
                        auto response = receive_message(outgoing);
                        if (drop_responses) continue;
                        std::this_thread::sleep_for(std::chrono::milliseconds{delay_ms.load()});
                        send_routed_message(incoming, route, response);
                    } catch (const std::exception&) {
                        // A vanished sender/destination models a lost packet.
                    }
                }
            } catch (...) {
                try { ready.set_exception(std::current_exception()); } catch (...) {}
            }
        });
        try { started.get(); } catch (...) { stop_ = true; worker_.join(); throw; }
    }
    ~FaultProxy() { stop_ = true; worker_.join(); }
    std::atomic<bool> drop_requests{false};
    std::atomic<bool> drop_responses{false};
    std::atomic<int> delay_ms{0};
private:
    std::atomic<bool> stop_{false};
    std::thread worker_;
};

} // namespace zmq_template::server::test
