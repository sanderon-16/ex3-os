//
// Created by TLP-299 on 02/07/2024.
//

#ifndef EX3_THREADACTION_H
#define EX3_THREADACTION_H

#define RIGHT_NUMBER_MASK 0x000000007FFFFFFF
#define MIDDLE_NUMBER_MASK 0x3FFFFFFF80000000
#define MIDDLE_NUMBER_INDEX 31
#define LEFT_NUMBER_INDEX 62
#define GET_RIGHT_NUMBER(ac) (ac&RIGHT_NUMBER_MASK)
#define GET_MIDDLE_NUMBER(ac) ((ac&MIDDLE_NUMBER_MASK)>>MIDDLE_NUMBER_INDEX)
#define SET_MIDDLE_NUMBER(ac, num) ((num<<MIDDLE_NUMBER_INDEX)&(ac|MIDDLE_NUMBER_MASK))


#include "Barrier.h"
#include "Context.h"

#include <algorithm>

void *thread_action(void *context);

int compare_pairs(const IntermediatePair& a, const IntermediatePair& b);

#endif //EX3_THREADACTION_H
