# include "MapReduceFramework.h"
# include "Barrier.h"

struct JobContext{
    Barrier* barrier;
    const MapReduceClient* client;
    const InputVec* inputVec;
    IntermediateVec* intermediateVec;
    OutputVec* outputVec;
    std::vector<std::vector<std::pair<K2*, V2*>>> shuffleVec;
    stage_t stage;
};

struct ThreadContext{
    const JobContext* jobContext;

    // each thread has its own intermediateVec to sort and reduce to outputVec
    IntermediateVec* intermediateVec;
    OutputVec* outputVec;
};

JobHandle startMapReduceJob(const MapReduceClient &client,
                            const InputVec &inputVec, OutputVec &outputVec,
                            int multiThreadLevel) {

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

