#include "MDConnector.h"

int main(int argc, char* argv[]) {
    MDConnector* con = MDConnector::getInstance();
    con->on_init();

    std::thread t1(&MDProcessor::write_to_schmem, MDProcessor::getInstance());
    std::thread t2(&MDProcessor::process_raw_data1, MDProcessor::getInstance());
    std::thread t3(&MDProcessor::process_raw_data2, MDProcessor::getInstance());
    std::thread t4(&MDProcessor::process_raw_data3, MDProcessor::getInstance());

    con->connect();
}