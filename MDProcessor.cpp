#include "MDProcessor.h"
#include "../Utils/Time_functions.h"
#include "../Utils/math_functions.h"
#include "glog/logging.h"
#include <string>
#include <string_view>
#include <ctime>
#include <cstdint>
#include <iostream>

MDProcessor* MDProcessor::uniqueInstance = nullptr;

MDProcessor* MDProcessor::getInstance(){
    if(uniqueInstance == nullptr){
        uniqueInstance = new MDProcessor();
    }
    return uniqueInstance;
}

void MDProcessor::startUp(){
    mShmemManager = ShmemManager::getInstance();
    mSymIDManager = SymbolIDManager::getInstance();
}

void MDProcessor::shutDown(){
    
}

void MDProcessor::process_quote(const simdjson::dom::object& _obj, ProcessedData& _data, MDupdate& md){
    md.m_type = md_type::QUOTE;
    md.m_symbolId = mSymIDManager->getID(_obj["S"].get_string());
    md.m_bid_price = roundToNearestCent(Price(_obj["bp"].get_double() * DOLLAR));
    md.m_ask_price = roundToNearestCent(Price(_obj["ap"].get_double() * DOLLAR));
    md.m_bid_quant = Shares(_obj["bs"].get_int64() * 100);
    md.m_ask_quant = Shares(_obj["as"].get_int64() * 100);

    //Timestamp conversion
    md.m_timestamp = parse_timestring(_obj["t"].get_string());

    while(_data.data[data_queue.next_write_index].is_ready.load(std::memory_order_acquire));
    _data.data[data_queue.next_write_index].data = md;
    _data.data[data_queue.next_write_index].is_ready.store(true, std::memory_order_release);
    _data.next_write_index.fetch_add(1, std::memory_order_release);
}
    
void MDProcessor::process_trade(const simdjson::dom::object& _obj, ProcessedData& _data, MDupdate& md){
    md.m_type = md_type::PRINT;
    md.m_symbolId = mSymIDManager->getID(_obj["S"].get_string());
    md.m_bid_price = roundToNearestCent(Price(_obj["p"].get_double() * DOLLAR));
    md.m_bid_quant = Shares(_obj["s"].get_int64() * 100);

    //Timestamp conversion
    md.m_timestamp = parse_timestring(_obj["t"].get_string());

    while(_data.data[data_queue.next_write_index].is_ready.load(std::memory_order_acquire));
    _data.data[data_queue.next_write_index].data = md;
    _data.data[data_queue.next_write_index].is_ready.store(true, std::memory_order_release);
    _data.next_write_index.fetch_add(1, std::memory_order_release);
}

void MDProcessor::push_raw_data(std::string raw_json){
    while(data_queue.data[data_queue.next_write_index].is_ready.load(std::memory_order_acquire));
    data_queue.data[data_queue.next_write_index].data = std::move(raw_json);
    data_queue.data[data_queue.next_write_index].is_ready.store(true, std::memory_order_release);
    data_queue.next_write_index.fetch_add(1, std::memory_order_release);
}

void MDProcessor::process_raw_data1(){
    std::string data;
    MDupdate    currentMD;
    simdjson::dom::parser parser;

    while(true){
        if(try_pop(data)){
            simdjson::padded_string padded_json_string(data);

            for(simdjson::dom::object obj : parser.parse(padded_json_string)){
                for(const auto& key_value : obj) {
                    if(key_value.key == "T"){
                        std::string_view value = obj["T"].get_string();
                        if(value == "q"){
                            process_quote(obj, thread_1_data, currentMD);
                            continue;
                        }
                        else if(value == "t"){
                            process_trade(obj, thread_1_data, currentMD);
                            continue;
                        }
                        else{
                            continue;
                        }
                    }
                }
            }
        }
    }
}

void MDProcessor::process_raw_data2(){
    std::string data;
    MDupdate    currentMD;
    simdjson::dom::parser parser;

    while(true){
        if(try_pop(data)){
            simdjson::padded_string padded_json_string(data);

            for(simdjson::dom::object obj : parser.parse(padded_json_string)){
                for(const auto& key_value : obj) {
                    if(key_value.key == "T"){
                        std::string_view value = obj["T"].get_string();
                        if(value == "q"){
                            process_quote(obj, thread_2_data, currentMD);
                            continue;
                        }
                        else if(value == "t"){
                            process_trade(obj, thread_2_data, currentMD);
                            continue;
                        }
                        else{
                            continue;
                        }
                    }
                }
            }
        }
    }
}

void MDProcessor::process_raw_data3(){
    std::string data;
    MDupdate    currentMD;
    simdjson::dom::parser parser;

    while(true){
        if(try_pop(data)){
            simdjson::padded_string padded_json_string(data);

            for(simdjson::dom::object obj : parser.parse(padded_json_string)){
                for(const auto& key_value : obj) {
                    if(key_value.key == "T"){
                        std::string_view value = obj["T"].get_string();
                        if(value == "q"){
                            process_quote(obj, thread_3_data, currentMD);
                            continue;
                        }
                        else if(value == "t"){
                            process_trade(obj, thread_3_data, currentMD);
                            continue;
                        }
                        else{
                            continue;
                        }
                    }
                }
            }
        }
    }
}

bool MDProcessor::try_pop(std::string& output){
    uint8_t current_idx = data_queue.next_read_index.load(std::memory_order_acquire);
    uint8_t initial_write_idx = data_queue.next_write_index.load(std::memory_order_acquire);
    // Check if data is ready
    if (current_idx == initial_write_idx) return false;

    // Try to claim this slot atomically
    if (data_queue.next_read_index.compare_exchange_weak(current_idx, current_idx + 1)) {
        output = std::move(data_queue.data[current_idx].data);
        data_queue.data[current_idx].is_ready.store(false, std::memory_order_release);
        return true;
    }

    return false; // Someone else beat us to it
}

bool MDProcessor::try_pop(MDupdate& output, ProcessedData& data){
    uint8_t current_idx = data.next_read_index.load(std::memory_order_acquire);
    uint8_t initial_write_idx = data.next_write_index.load(std::memory_order_acquire);
    // Check if data is ready
    if (current_idx == initial_write_idx) return false;

    // Try to claim this slot atomically
    if (data.next_read_index.compare_exchange_weak(current_idx, current_idx + 1)) {
        output = std::move(data.data[current_idx].data);
        data.data[current_idx].is_ready.store(false, std::memory_order_release);
        return true;
    }

    return false; // Someone else beat us to it
}

void MDProcessor::write_to_schmem(){
    MDupdate cur_md_1;
    MDupdate cur_md_2;
    MDupdate cur_md_3;
    bool queue1 = false;
    bool queue2 = false;
    bool queue3 = false;
    while(true){
        if(!queue1)
            queue1 = try_pop(cur_md_1,thread_1_data);
        if(!queue2)
            queue2 = try_pop(cur_md_2,thread_2_data);
        if(!queue3)
            queue3 = try_pop(cur_md_3,thread_3_data);

        if(!queue1){
            if(!queue2){
                if(!queue3){
                    continue;
                }
                else{
                    mShmemManager->write_MD(cur_md_3);
                    queue3 = false;
                }
            }
            else{
                if(!queue3){
                    mShmemManager->write_MD(cur_md_2);
                    queue2 = false;
                }
                else{
                    if(cur_md_2.m_timestamp<=cur_md_3.m_timestamp){
                        mShmemManager->write_MD(cur_md_2);
                        queue2 = false;
                    }
                    else{
                        mShmemManager->write_MD(cur_md_3);
                        queue3 = false;
                    }
                }
            }
        }
        else{
            if(!queue2){
                if(!queue3){
                    mShmemManager->write_MD(cur_md_1);
                    queue1 = false;
                }
                else{
                    if(cur_md_1.m_timestamp<=cur_md_3.m_timestamp){
                        mShmemManager->write_MD(cur_md_1);
                        queue1 = false;
                    }
                    else{
                        mShmemManager->write_MD(cur_md_3);
                        queue3 = false;
                    }
                }
            }
            else{
                if(!queue3){
                    if(cur_md_1.m_timestamp<=cur_md_2.m_timestamp){
                        mShmemManager->write_MD(cur_md_1);
                        queue1 = false;
                    }
                    else{
                        mShmemManager->write_MD(cur_md_2);
                        queue2 = false;
                    }
                }
                else{
                    if(cur_md_2.m_timestamp<=cur_md_3.m_timestamp){
                        if(cur_md_1.m_timestamp<=cur_md_2.m_timestamp){
                            mShmemManager->write_MD(cur_md_1);
                            queue1 = false;
                        }
                        else{
                            mShmemManager->write_MD(cur_md_2);
                            queue2 = false;
                        }
                    }
                    else{
                        if(cur_md_1.m_timestamp<=cur_md_3.m_timestamp){
                            mShmemManager->write_MD(cur_md_1);
                            queue1 = false;
                        }
                        else{
                            mShmemManager->write_MD(cur_md_3);
                            queue3 = false;
                        }
                    }
                }
            }
        }
    }
}

