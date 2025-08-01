#pragma once

#include "cinttypes"
#include <string>

struct ROBCacheProfileData
{   
    uint64_t inst_pc = 0;
    uint64_t uarch_id = 0;
    uint16_t thread_id = -1;
    uint64_t opcode = 0;

    // The first time an instruction is seen in the ROB
    uint64_t cycle_first_appended = 0;

    // Instruction type
    uint8_t inst_type = 0;

    // IPC per instruction at the time of retirement
    double latest_ipc = 0.0;

    // Memory OP information
    uint8_t mem_region = 0;

    // DCache information (on load/store)
    bool l1dcache_hit = false;
    bool l1dcache_miss = false;

    // Branch information
    uint32_t mispred_penalty = 0;
    bool is_mispred = false;
    bool is_taken = false;

    // Shared parameter for ld/st and branch targets
    uint64_t target_v_addr = 0;

    // Determine retirement cost
    uint32_t cycles_cost = 0;

    // Flush data
    bool is_flush = false;

    // Number of times the instruction was seen
    uint64_t encoded_inst_metadata = 0;

    // Disassembly string
    /*
    In a 100 million instruction trace with 8322 disassembly strings,
    the average length of each string is ~20 characters
    */
    std::string disassembly = "xxxxxxxxxxxxxxxxxxxxx";

    ROBCacheProfileData() = default;

    ROBCacheProfileData(uint64_t inst_pc, uint32_t opcode) : 
        inst_pc(inst_pc),
        opcode(opcode) {}

    template<typename Archive>
    void serialize(Archive& ar, const unsigned int /*version*/)
    {
        ar & inst_pc;
        ar & uarch_id;
        ar & thread_id;
        ar & opcode;
        ar & cycle_first_appended;
        ar & inst_type;
        ar & latest_ipc;
        ar & mem_region;
        ar & l1dcache_hit;
        ar & l1dcache_miss;
        ar & mispred_penalty;
        ar & is_mispred;
        ar & is_taken;
        ar & target_v_addr;
        ar & cycles_cost;
        ar & is_flush;
        ar & encoded_inst_metadata;;
    }

};


// TODO ztaufique: This can be included in the SimDB serializer pipeline
struct ROBCacheSummaryData
{
    uint64_t inst_pc = 0;
    uint16_t thread_id = -1;
    uint64_t opcode = 0;

    // The first time an instruction is seen in the ROB
    uint64_t cycle_first_appended = 0;

    // Instruction type
    uint8_t inst_type = 0;

    // IPC per instruction at the time of retirement
    double latest_ipc = 0.0;

    // DCache information (on load/store)
    uint32_t l1dcache_hits = 0;
    uint32_t l1dcache_misses = 0;

    // Branch information
    uint32_t total_mispred_penalty = 0;
    uint32_t num_mispred = false;
    uint32_t num_taken = false;

    // Determine retirement cost
    uint32_t cycles_cost = 0;

    // Flush data
    uint32_t num_flushes = 0;

    // TODO ztaufique: we can probably just do this in the serializer
    // Number of times the instruction was seen
    uint32_t num_seen = 1;
};
