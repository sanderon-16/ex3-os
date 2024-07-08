# include "ThreadAction.h"
# include <iostream>
#include <bitset>

void *thread_action(void *context) {

    auto thread_context = (ThreadContext *) context;
    JobContext *job_context = thread_context->job_context;

    uint64_t input_vector_size = job_context->input_vec->size();

    // loop map
    while (GET_RIGHT_NUMBER((uint64_t) *(job_context->atomic_counter)) <
           input_vector_size) // exit when all the input was mapped
    {
        auto old_value = (uint64_t) (job_context->atomic_counter->fetch_add(INC_RIGHT)); // advance the atomic timer
        job_context->client->map(job_context->input_vec->at(GET_RIGHT_NUMBER(old_value)).first,
                                 job_context->input_vec->at(GET_RIGHT_NUMBER(old_value)).second, context);
//        std::cout << "atomic_counter: " << std::bitset<64>((uint64_t) *(job_context->atomic_counter)) << "\n";
    }


    // sort intermediate
    std::sort((*job_context->personal_vecs)[thread_context->threadID]->begin(),
              (*job_context->personal_vecs)[thread_context->threadID]->end(), compare_pairs);

    // shuffle if the id is 0
    if (thread_context->threadID == 0) {
        job_context->stage = SHUFFLE_STAGE;
        job_context->total_pairs = 0;
        for (int i = 0; i < job_context->personal_vecs->size(); i++) {
            job_context->total_pairs += (*job_context->personal_vecs)[i]->size();
        }

        job_context->atomic_counter->fetch_add(INC_LEFT);
        *(job_context->atomic_counter) = SET_RIGHT_NUMBER((uint64_t) *(job_context->atomic_counter), (uint64_t) 0);

        while (!(*job_context->personal_vecs).empty()) {

            // create vector for new k
            job_context->shuffle_vec->push_back(new IntermediateVec());
            job_context->atomic_counter->fetch_add(INC_RIGHT);

            // find minimal k
            K2 *lowest_k2 = nullptr;
            for (int i = 0; i < job_context->personal_vecs->size(); i++) {
                if (!(*job_context->personal_vecs)[i]->empty() && lowest_k2 == nullptr) {
                    lowest_k2 = (*job_context->personal_vecs)[i]->front().first;
                } else if (!(*job_context->personal_vecs)[i]->empty()) {
                    if (*(*job_context->personal_vecs)[i]->front().first < *lowest_k2) {
                        lowest_k2 = (*job_context->personal_vecs)[i]->front().first;
                    }
                }
            }

            // add all pairs
            for (int i = 0; i < job_context->personal_vecs->size(); i++) {
                while (!(*job_context->personal_vecs)[i]->empty()) {
                    if (!(*lowest_k2 < *(*job_context->personal_vecs)[i]->front().first) &&
                        !((*(*job_context->personal_vecs)[i]->front().first) < *lowest_k2)) {
                        job_context->shuffle_vec->back()->push_back(
                                std::move((*job_context->personal_vecs)[i]->front()));
                        (*job_context->personal_vecs)[i]->erase((*job_context->personal_vecs)[i]->begin());
                        job_context->atomic_counter->fetch_add(INC_MIDDLE);

                    } else {
                        break;
                    }
                }
                if ((*job_context->personal_vecs)[i]->empty()) {
                    delete (*job_context->personal_vecs)[i];
                    (*job_context->personal_vecs).erase((*job_context->personal_vecs).begin() + i);
                }
            }
        }
        // this is thread safe because only thread-0 is allowed to do it.
        *(job_context->atomic_counter) = SET_MIDDLE_NUMBER((uint64_t) *(job_context->atomic_counter), (uint64_t) 0);
        // inc the left number
        job_context->stage = REDUCE_STAGE;
        job_context->atomic_counter->fetch_add(INC_LEFT);
    }

    job_context->barrier->barrier();

    // loop reduce
    while (GET_MIDDLE_NUMBER((uint64_t) *(job_context->atomic_counter)) <
           job_context->shuffle_vec->size()) // exit when all the input was reduced
    {
        auto old_value = (uint64_t) (job_context->atomic_counter->fetch_add(INC_MIDDLE)); // advance the atomic timer
        std::cout << "trying to access shuffle vec in the index: " << GET_MIDDLE_NUMBER(old_value) << "out of: "
                  << job_context->shuffle_vec->size() << "\n";
        job_context->client->reduce((*(job_context->shuffle_vec))[GET_MIDDLE_NUMBER(old_value)], context);
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