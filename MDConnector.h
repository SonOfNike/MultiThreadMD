#pragma once

#include <websocketpp/config/asio_client.hpp>
#include <websocketpp/client.hpp>
#include <iostream>
#include "../Utils/simdjson/simdjson.h"
#include <string>
#include "ShmemManager.h"
#include "MDProcessor.h"
#include "../Utils/MDupdate.h"
#include "../Utils/SymbolIDManager.h"

typedef websocketpp::client<websocketpp::config::asio_tls_client> client;
typedef websocketpp::lib::shared_ptr<websocketpp::lib::asio::ssl::context> context_ptr;

using websocketpp::lib::placeholders::_1;
using websocketpp::lib::placeholders::_2;
using websocketpp::lib::bind;

class MDConnector {
public:
    void on_init();

    static MDConnector* getInstance();
    void connect();

private:

    ShmemManager* mShmemManager;
    static SymbolIDManager* mSymIDManager;
    static MDProcessor*  mMDProcessor;

    static MDConnector* uniqueInstance;

    void scheduleReconnect();

    static void on_open(websocketpp::connection_hdl hdl, client* c);
    static void on_message(websocketpp::connection_hdl, client::message_ptr msg);
    static void on_fail(websocketpp::connection_hdl hdl);
    static void on_close(websocketpp::connection_hdl hdl);

    client c;
    std::string hostname = "stream.data.alpaca.markets/v2/sip";
    std::string uri = "wss://" + hostname;
    static simdjson::dom::parser parser;

    int m_backoff = 1;

    static context_ptr on_tls_init(const char * hostname, websocketpp::connection_hdl) {
        context_ptr ctx = websocketpp::lib::make_shared<boost::asio::ssl::context>(boost::asio::ssl::context::sslv23);
        try {
            ctx->set_options(boost::asio::ssl::context::default_workarounds |
                            boost::asio::ssl::context::no_sslv2 |
                            boost::asio::ssl::context::no_sslv3 |
                            boost::asio::ssl::context::single_dh_use);
        } catch (std::exception& e) {
            std::cout << "TLS Initialization Error: " << e.what() << std::endl;
        }

        return ctx;
    }
};