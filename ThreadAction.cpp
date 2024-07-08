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
    std::sort(job_context->personal_vecs[thread_context->threadID]->begin(),
              job_context->personal_vecs[thread_context->threadID]->end(), compare_pairs);


    // shuffle if the id is 0
    if (thread_context->threadID == 0) {
        job_context->stage = SHUFFLE_STAGE;
        job_context->atomic_counter->fetch_add(INC_LEFT);
        // todo fill this
        //TODO ac=0

        while (GET_MIDDLE_NUMBER(job_context->atomic_counter->load()) >
               GET_RIGHT_NUMBER(job_context->atomic_counter->load())) {
            job_context->shuffle_vec->emplace_back();

            K2 *lowest_k2 = nullptr;
            for (int i = 0; i < job_context->num_threads; i++) {
                if (!job_context->personal_vecs[i]->empty() && lowest_k2 == nullptr) {
                    lowest_k2 = job_context->personal_vecs[i]->front().first;
                } else if (!job_context->personal_vecs[i]->empty()) {
                    if (lowest_k2 > job_context->personal_vecs[i]->front().first) {
                        lowest_k2 = job_context->personal_vecs[i]->front().first;
                    }
                }
            }

            for (int i = 0; i < job_context->num_threads; i++) {
                if (!job_context->personal_vecs[i]->empty()) {
                    if (!(job_context->personal_vecs[i]->front().first > lowest_k2)) {
                        while (!(job_context->personal_vecs[i]->front().first > lowest_k2)) {
                            job_context->shuffle_vec->back().push_back(
                                    std::move(job_context->personal_vecs[i]->front()));
                            job_context->personal_vecs[i]->erase(job_context->personal_vecs[i]->begin());
                            job_context->atomic_counter++;
                        }
                    }
                }
            }
        }

        job_context->barrier->barrier();
    } else {
        job_context->barrier->barrier();
    }


    // reduce loop
    while (*(job_context->atomic_counter) < input_vector_size) // exit when all the input was reduced
    {
        uint32_t old_value = (*(job_context->atomic_counter))++; // advance the atomic timer
        job_context->client->map(job_context->input_vec->at(old_value).first,
                                 job_context->input_vec->at(old_value).second, context);
    }
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