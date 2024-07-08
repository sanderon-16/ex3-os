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
    std::atomic<uint64_t> *atomic_counter;
    int num_threads;
    bool waiting;
    pthread_t* threads_p;
    uint64_t total_pairs;

    const InputVec *input_vec;
    pthread_mutex_t input_vec_mutex;
    OutputVec *output_vec;
    pthread_mutex_t output_vec_mutex;
    std::vector<IntermediateVec*> *personal_vecs;
    pthread_mutex_t personal_vecs_mutex;
    std::vector<IntermediateVec*> *shuffle_vec;
    pthread_mutex_t shuffle_vec_mutex;
};

struct ThreadContext {
    JobContext *job_context;
    int threadID;
};


#endif //EX3_CONTEXT_H
