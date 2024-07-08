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
    stage_t stage = UNDEFINED_STAGE;
    std::atomic<uint64_t> *atomic_counter;
    int num_threads;
    bool waiting;
    pthread_t* threads_p;

    const InputVec *input_vec;
    OutputVec *output_vec;
    std::vector<IntermediateVec*> *personal_vecs;
    std::vector<IntermediateVec> *shuffle_vec;
};

struct ThreadContext {
    JobContext *job_context;
    int threadID;
};


#endif //EX3_CONTEXT_H
