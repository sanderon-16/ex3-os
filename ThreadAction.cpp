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
        int res = pthread_mutex_lock(&job_context->input_vec_mutex); //because of the call to it in emit2
        if (res != 0) {
            std::cerr << "system error: mutex lock failed in map phase\n";
            return nullptr;
        }
        job_context->client->map(job_context->input_vec->at(GET_RIGHT_NUMBER(old_value)).first,
                                 job_context->input_vec->at(GET_RIGHT_NUMBER(old_value)).second, context);
        res = pthread_mutex_unlock(&job_context->input_vec_mutex);
        if (res != 0) {
            std::cerr << "system error: mutex unlock failed in map phase\n";
            return nullptr;
        }
//        std::cout << "atomic_counter: " << std::bitset<64>((uint64_t) *(job_context->atomic_counter)) << "\n";
    }

    std::cout << "thread id: " << thread_context->threadID << " finished map\n";
    thread_context->job_context->barrier->barrier();

    job_context->total_pairs = 0;
    for (int i = 0; i < job_context->personal_vecs->size(); i++) {
        job_context->total_pairs += (*job_context->personal_vecs)[i]->size();
    }
    std::cout << "total pairs: " << job_context->total_pairs << "\n";

    // sort intermediate
    std::sort((*job_context->personal_vecs)[thread_context->threadID]->begin(),
              (*job_context->personal_vecs)[thread_context->threadID]->end(), compare_pairs);

    std::cout << "thread id: " << thread_context->threadID << " finished sort\n";
    thread_context->job_context->barrier->barrier();

    // shuffle if the id is 0
    if (thread_context->threadID == 0) {
        job_context->atomic_counter->fetch_add(INC_LEFT); //moved a stage
        *(job_context->atomic_counter) = SET_RIGHT_NUMBER((uint64_t) *(job_context->atomic_counter), (uint64_t) 0);

        job_context->total_pairs = 0;
        for (int i = 0; i < job_context->personal_vecs->size(); i++) {
            job_context->total_pairs += (*job_context->personal_vecs)[i]->size();
        }

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
        // plaster
        bool flag_miss = true;
        while (flag_miss) {
            flag_miss = false;
            for (int i = 0; i < job_context->shuffle_vec->size() - 1; i++) {
                // check if two neighboring keys are the same
                if (!(*(*job_context->shuffle_vec)[i]->back().first <
                      *(*job_context->shuffle_vec)[i + 1]->back().first) &&
                    !(*(*job_context->shuffle_vec)[i + 1]->back().first <
                      *(*job_context->shuffle_vec)[i]->back().first)) {
                    (*job_context->shuffle_vec)[i]->insert((*job_context->shuffle_vec)[i]->end(),
                                                           (*job_context->shuffle_vec)[i + 1]->begin(),
                                                           (*job_context->shuffle_vec)[i + 1]->end());
                    delete (*job_context->shuffle_vec)[i + 1];
                    (*job_context->shuffle_vec).erase((*job_context->shuffle_vec).begin() + i + 1);
                    job_context->atomic_counter->fetch_sub(INC_RIGHT);
                    std::cout<<"plaster\n";
                    flag_miss = true;
                    break;
                } else {
                    continue;
                }
            }
        }

        std::cout<<"shuffle size: "<<job_context->shuffle_vec->size()<<"\n";
        // this is thread safe because only thread-0 is allowed to do it.
        *(job_context->atomic_counter) = SET_MIDDLE_NUMBER((uint64_t) *(job_context->atomic_counter), (uint64_t) 0);
        // inc the left number
        job_context->atomic_counter->fetch_add(INC_LEFT);
    }

    job_context->barrier->barrier();
    // loop reduce
    while (GET_MIDDLE_NUMBER((uint64_t) *(job_context->atomic_counter)) <
           job_context->shuffle_vec->size()) // exit when all the input was reduced
    {
        auto old_value = (uint64_t) (job_context->atomic_counter->fetch_add(
                INC_MIDDLE)); // advance the atomic timer

        int res = pthread_mutex_lock(&job_context->shuffle_vec_mutex);
        if (res != 0) {
            std::cerr << "system error: mutex lock failed in reduce phase\n";
            return nullptr;
        }
        job_context->client->reduce((*(job_context->shuffle_vec))[GET_MIDDLE_NUMBER(old_value)], context);

        res = pthread_mutex_unlock(&job_context->shuffle_vec_mutex);
        if (res != 0) {
            std::cerr << "system error: mutex unlock failed in reduce phase\n";
            return nullptr;
        }
    }
    thread_context->job_context->barrier->barrier();
    std::cout << "thread id: " << thread_context->threadID << " finished reduce\n";
    return nullptr;
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