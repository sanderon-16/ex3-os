#include "MapReduceFramework.h"
#include "ThreadAction.h"
#include <pthread.h>

JobHandle startMapReduceJob(const MapReduceClient &client, const InputVec &inputVec,
                            OutputVec &outputVec, int multiThreadLevel) {

    // initializing the threads, the global job context, the barrier and a thread context for each thread
    pthread_t threads[multiThreadLevel];
    auto contexts = new ThreadContext[multiThreadLevel];
    auto jc = new JobContext();
    Barrier barrier(multiThreadLevel);

    // initializing the attributes of the global job context TODO check if by reference is ok
    jc->barrier = &barrier;
    jc->input_vec = &inputVec;
    jc->client = &client;
    jc->output_vec = &outputVec;
    jc->stage = UNDEFINED_STAGE;
    jc->atomic_counter = new std::atomic<uint64_t>(0); //todo check for errors in init
    // todo maybe init shuffle vec

    // assigning the job context to each of the thread contexts
    for (int i = 0; i < multiThreadLevel; i++) {
        contexts[i].job_context = jc;
        contexts[i].intermediate_vec = new std::vector<IntermediatePair>();
    }

    // changing to map stage and start mapping
    jc->stage = MAP_STAGE;
    for (int i = 0; i < multiThreadLevel; i++) {
        //TODO check for errors
        pthread_create(threads + i, nullptr, thread_action, contexts + i);
    }

    return static_cast<JobHandle>(jc);
}

void waitForJob(JobHandle job) {

}

void getJobState(JobHandle job, JobState *state) {
    auto jc = (JobContext*) job;

    // the jc stage and percentage is updated independently inside startmapreducejob
    state->stage = jc->stage;
    if (state->stage == MAP_STAGE)
    {
        state->percentage =  100 * (((float)*jc->atomic_counter) / (float) jc->input_vec->size());
    }
    else
    {
        state->percentage = -1; // todo fill this
    }
}

void closeJobHandle(JobHandle job) {

}

void emit2(K2 *key, V2 *value, void *context) {
    ((ThreadContext*)context)->intermediate_vec->push_back(std::pair<K2 *, V2 *>(key, value));
}

void emit3(K3 *key, V3 *value, void *context) {
    ((ThreadContext*)context)->job_context->output_vec->push_back(std::pair<K3 *, V3 *>(key, value));
}

