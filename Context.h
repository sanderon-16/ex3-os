//
// Created by TLP-299 on 02/07/2024.
//

#ifndef EX3_CONTEXT_H
#define EX3_CONTEXT_H

#include <atomic>
#include "Barrier.h"
#include "MapReduceClient.h"
#include "MapReduceFramework.h"

struct JobContext {
    Barrier *barrier;
    const MapReduceClient *client;
    stage_t stage;
    std::atomic<uint32_t> *atomic_counter;

    const InputVec *input_vec;
    IntermediateVec *intermediate_vec;
    OutputVec *output_vec;
    std::vector<std::vector<std::pair<K2 *, V2 *>>> shuffle_vec;
};

struct ThreadContext {
    JobContext *job_context;
    int threadID;

    // each thread has its own intermediateVec to sort
    IntermediateVec *intermediate_vec;
};

#endif //EX3_CONTEXT_H
