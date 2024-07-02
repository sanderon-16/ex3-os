# include "ThreadAction.h"

void *thread_action(void *context) {

    JobContext* job_context = context->job_context;

    // loop map
    while (*(job_context->atomic_counter) < job_context->input_vec->size()) // exit when all the input was mapped
    {
        uint32_t old_value = (*(job_context->atomic_counter))++; // advance the atomic timer

        job_context->client->map(job_context->input_vec->at(old_value).first,
                                          job_context->input_vec->at(old_value).second, context);
    }

    // sort intermediate
    std::sort(context->intermediate_vec->begin(), context->intermediate_vec->end(), compare_pairs);


    // shuffle if the id is 0

    // loop reduce
}

/**
 * compares pairs of Intermediate pairs by key
 * @param a first pair
 * @param b second pair
 * @return the comparison between them
 */
int compare_pairs(const IntermediatePair& a, const IntermediatePair& b)
{
    return *(a.first) < *(b.first);
}