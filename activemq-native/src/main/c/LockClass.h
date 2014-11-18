/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

#ifndef LOCKCLASS_H_
#define LOCKCLASS_H_

#include <pthread.h>

class LockClass
{
protected:
    pthread_mutex_t* _m;
public:
    inline LockClass(pthread_mutex_t* m) : _m(m)
    {
        ::pthread_mutex_lock(_m);
    }
    inline ~LockClass()
    {
        ::pthread_mutex_unlock(_m);
    }
};


#endif /*LOCKCLASS_H_*/
