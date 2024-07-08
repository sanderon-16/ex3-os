# include "ThreadAction.h"
# include <iostream>

void *thread_action(void *context) {

    auto thread_context = (ThreadContext *) context;
    JobContext *job_context = thread_context->job_context;

    uint64_t input_vector_size = job_context->atomic_counter->load();
    input_vector_size = GET_MIDDLE_NUMBER(input_vector_size);

    // loop map
    while (GET_RIGHT_NUMBER((uint64_t)*(job_context->atomic_counter)) < input_vector_size) // exit when all the input was mapped
    {
        auto old_value = (uint64_t)(job_context->atomic_counter->fetch_add(INC_RIGHT)); // advance the atomic timer
        job_context->client->map(job_context->input_vec->at(GET_RIGHT_NUMBER(old_value)).first,
                                 job_context->input_vec->at(GET_RIGHT_NUMBER(old_value)).second, context);
    }

    // sort intermediate
    std::sort(thread_context->intermediate_vec->begin(), thread_context->intermediate_vec->end(), compare_pairs);

    // shuffle if the id is 0
    if (thread_context->threadID == 0) {
        job_context->stage = SHUFFLE_STAGE;
        job_context->atomic_counter->fetch_add(INC_LEFT);
        // todo fill this
    }


    // loop reduce
}

/**
 * compares pairs of Intermediate pairs by key
 * @param a first pair
 * @param b second pair
 * @return the comparison between them
 */
int compare_pairs(const IntermediatePair &a, const IntermediatePair &b) {
    return *(a.first) < *(b.first);
}