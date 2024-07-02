# include "MapReduceFramework.h"
# include "ThreadAction.h"
#include <pthread.h>

JobHandle startMapReduceJob(const MapReduceClient &client, const InputVec &inputVec,
                            OutputVec &outputVec, int multiThreadLevel) {

    // initializing the threads, the global job context, the barrier and a thread context for each thread
    pthread_t threads[multiThreadLevel];
    ThreadContext contexts[multiThreadLevel];
    auto jc = new JobContext();
    Barrier barrier(inputVec.size());

    // initializing the attributes of the global job context
    jc->barrier = &barrier;
    jc->input_vec = &inputVec;
    jc->client = &client;
    jc->output_vec = &outputVec;
    jc->stage = UNDEFINED_STAGE;

    // assigning the job context to each of the thread contexts
    for (int i = 0; i < multiThreadLevel; i++) {
        contexts[i].job_context = jc;
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
    ((ThreadContext*)context)->intermediate_vec->push_back(std::pair<K2 *, V2 *>(key, value));
}

void emit3(K3 *key, V3 *value, void *context) {
    ((ThreadContext*)context)->job_context->output_vec->push_back(std::pair<K3 *, V3 *>(key, value));
}

