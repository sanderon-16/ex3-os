#include "MapReduceFramework.h"
#include "ThreadAction.h"
#include <pthread.h>
#include <iostream>

JobHandle startMapReduceJob(const MapReduceClient &client, const InputVec &inputVec,
                            OutputVec &outputVec, int multiThreadLevel) {

    // initializing the threads, the global job context, the barrier and a thread context for each thread
    pthread_t threads[multiThreadLevel];
    auto contexts = new ThreadContext[multiThreadLevel];
    auto jc = new JobContext();

    // initializing the attributes of the global job context TODO check if by reference is ok
    jc->barrier = new Barrier(multiThreadLevel);
    jc->input_vec = &inputVec;
    jc->client = &client;
    jc->output_vec = &outputVec;
    jc->stage = UNDEFINED_STAGE;
    jc->threads_p = threads;
    jc->atomic_counter = new std::atomic<uint64_t>(0x0000000000000000); //todo check for errors in init
    jc->personal_vecs = new std::vector<IntermediateVec *>(0);
    jc->shuffle_vec = new std::vector<IntermediateVec *>(0);
    jc->num_threads = multiThreadLevel;
    jc->waiting = false;

    // assigning the job context to each of the thread contexts
    for (int i = 0; i < multiThreadLevel; i++) {
        contexts[i].job_context = jc;
        contexts[i].threadID = i;
        jc->personal_vecs->push_back(new IntermediateVec());
        if (jc->personal_vecs->back() == nullptr) {
            delete jc;
            delete[] contexts;
            return nullptr;
        }
    }

    if (jc->barrier == nullptr || jc->atomic_counter == nullptr || jc->personal_vecs == nullptr ||
        jc->shuffle_vec == nullptr) {
        delete jc;
        delete[] contexts;
        return nullptr;
    }
    // changing to map stage and start mapping
    jc->stage = MAP_STAGE;
    *(jc->atomic_counter) = SET_LEFT_NUMBER((uint64_t) jc->atomic_counter->load(), (uint64_t) 1);
    for (int i = 0; i < multiThreadLevel; i++) {
        if (pthread_create(threads + i, nullptr, thread_action, contexts + i) != 0) {
            delete jc;
            delete[] contexts;
            return nullptr;
        }
    }

    return static_cast<JobHandle>(jc);
}

void waitForJob(JobHandle job) {
    auto jc = (JobContext *) job;
    if (!jc->waiting) {
        for (int i = 0; i < jc->num_threads; i++) {
            pthread_join(jc->threads_p[0], nullptr);
            jc->waiting = false;
        }
    }
}

void getJobState(JobHandle job, JobState *state) {
    auto jc = (JobContext *) job;

    // the jc stage and percentage is updated independently inside startmapreducejob
    state->stage = (stage_t) GET_LEFT_NUMBER(jc->atomic_counter->load());
    if (state->stage == MAP_STAGE) {
        uint64_t current_count = GET_RIGHT_NUMBER(jc->atomic_counter->load());
        uint64_t size = jc->input_vec->size();
        state->percentage = 100 * (((float) current_count) / (float) size);
    } else if (state->stage == SHUFFLE_STAGE) {
        uint64_t current_count = GET_MIDDLE_NUMBER(jc->atomic_counter->load());
        uint64_t size = jc->total_pairs;
        if (size == 0) {
            state->percentage = 0;
        } else {
            state->percentage = 100 * (((float) current_count) / (float) size);
        }
    } else if (state->stage == REDUCE_STAGE) {
        uint64_t current_count = GET_MIDDLE_NUMBER(jc->atomic_counter->load());
        uint64_t size = GET_RIGHT_NUMBER(jc->atomic_counter->load());
        state->percentage = 100 * (((float) current_count) / (float) size);
    }
}

void closeJobHandle(JobHandle job) {
    std::cout<< "closing job handle";
    auto jc = (JobContext *) job;
    std::cout<<".";
    waitForJob(job);
    std::cout<<".";
    delete jc->atomic_counter;
    std::cout<<".";
    delete jc;
    std::cout<<"!";
}

void emit2(K2 *key, V2 *value, void *context) {
    int id = ((ThreadContext *) context)->threadID;
    (*((ThreadContext *) context)->job_context->personal_vecs)[id]->push_back(std::pair<K2 *, V2 *>(key, value));
}

void emit3(K3 *key, V3 *value, void *context) {
    ((ThreadContext *) context)->job_context->output_vec->push_back(std::pair<K3 *, V3 *>(key, value));
}

