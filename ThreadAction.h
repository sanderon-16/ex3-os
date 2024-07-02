//
// Created by TLP-299 on 02/07/2024.
//

#ifndef EX3_THREADACTION_H
#define EX3_THREADACTION_H

#include "Barrier.h"
#include <algorithm>

void *threadAction(void *arg);

int compare_pairs(const IntermediatePair& a, const IntermediatePair& b);

#endif //EX3_THREADACTION_H
