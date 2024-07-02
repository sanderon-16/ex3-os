#include <pthread.h>
# include "MapReduceFramework.h"

struct JobContext {
    int threadID;
    Barrier* barrier;
};
JobHandle startMapReduceJob(const MapReduceClient &client, const InputVec &inputVec,
                            OutputVec &outputVec, int multiThreadLevel) {
    pthread_t threads[multiThreadLevel];
    JobContext contexts[multiThreadLevel];
}

void waitForJob(JobHandle job) {

}

void getJobState(JobHandle job, JobState *state) {

}

void closeJobHandle(JobHandle job) {

}

void emit2(K2 *key, V2 *value, void *context) {

}

void emit3(K3 *key, V3 *value, void *context) {

}

