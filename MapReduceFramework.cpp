#include <pthread.h>
# include "MapReduceFramework.h"
# include "Barrier.h"

struct JobContext {
    Barrier *barrier;
    const MapReduceClient *client;
    const InputVec *inputVec;
    IntermediateVec *intermediateVec;
    OutputVec *outputVec;
    std::vector<std::vector<std::pair<K3 *, V3 *>>> shuffleVec;
    stage_t stage;
};

struct ThreadContext {
    const JobContext *jobContext;

    // each thread has its own intermediateVec to sort and reduce to outputVec
    IntermediateVec *intermediateVec;
};

JobHandle startMapReduceJob(const MapReduceClient &client, const InputVec &inputVec,
                            OutputVec &outputVec, int multiThreadLevel) {

    // initializing the threads, the global job context, the barrier and a thread context for each thread
    pthread_t threads[multiThreadLevel];
    ThreadContext contexts[multiThreadLevel];
    auto jc = new JobContext();
    Barrier barrier(inputVec.size());

    // initializing the attributes of the global job context
    jc->barrier = &barrier;
    jc->inputVec = &inputVec;
    jc->client = &client;
    jc->outputVec = &outputVec;
    jc->stage = UNDEFINED_STAGE;

    // assigning the job context to each of the thread contexts
    for (int i = 0; i < multiThreadLevel; i++) {
        contexts[i].jobContext = jc;
    }

    // changing to map stage and start mapping
    jc->stage = MAP_STAGE;
    for (int i = 0; i < multiThreadLevel; i++) {
        pthread_create(threads + i, NULL, thread_action, contexts + i);
    }


}

void waitForJob(JobHandle job) {

}

void getJobState(JobHandle job, JobState *state) {

}

void closeJobHandle(JobHandle job) {

}

void emit2(K2 *key, V2 *value, void *context) {
    ((ThreadContext*)context)->intermediateVec->push_back(std::pair<K2 *, V2 *>(key, value));
}

void emit3(K3 *key, V3 *value, void *context) {
    ((ThreadContext*)context)->jobContext->outputVec->push_back(std::pair<K3 *, V3 *>(key, value));
}

