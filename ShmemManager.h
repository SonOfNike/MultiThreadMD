#pragma once

#include "../Utils/MDShmem.h"
#include "../Utils/MDupdate.h"

class ShmemManager {
private:
    MDShmem* md_shmem = nullptr;

    static ShmemManager* uniqueInstance;
    ShmemManager(){;}

public:
    static ShmemManager* getInstance();
    void startUp();
    void shutDown();
    void write_MD(const MDupdate& _md_update);
};