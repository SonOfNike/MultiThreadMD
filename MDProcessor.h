#pragma once

#include "../Utils/MDShmem.h"
#include "../Utils/MDupdate.h"
#include "../Utils/simdjson/simdjson.h"
#include "ShmemManager.h"
#include "../Utils/SymbolIDManager.h"
#include <atomic>

struct Slot{
    std::string data;
    std::atomic<bool> is_ready{false};
};

struct MDSlot{
    MDupdate data;
    std::atomic<bool> is_ready{false};
};

struct RawData{
    Slot data[256];
    alignas(64) std::atomic<uint8_t> next_write_index = 0;
    alignas(64) std::atomic<uint8_t> next_read_index = 0;
};

struct ProcessedData{
    MDSlot data[256];
    alignas(64) std::atomic<uint8_t> next_write_index = 0;
    alignas(64) std::atomic<uint8_t> next_read_index = 0;
};

class MDProcessor {
private:

    ShmemManager* mShmemManager;
    SymbolIDManager* mSymIDManager;

    MDupdate    currentMD;

    static MDProcessor* uniqueInstance;
    MDProcessor(){;}

    RawData data_queue;
    ProcessedData processed_data_queue;

public:

    static MDProcessor* getInstance();
    void startUp();
    void shutDown();
    void push_raw_data(std::string raw_json);
    void process_raw_data();
    bool try_pop(std::string& output);
    bool try_pop(MDupdate& output);
    void write_to_schmem();
    void process_quote(const simdjson::dom::object& obj);
    void process_trade(const simdjson::dom::object& obj);
};