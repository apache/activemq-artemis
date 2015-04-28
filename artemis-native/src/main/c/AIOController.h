/*
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements. See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License. You may obtain a copy of the License at
  *
  *     http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */


#ifndef AIOCONTROLLER_H_
#define AIOCONTROLLER_H_
#include <jni.h>
#include <string>
#include "JAIODatatypes.h"
#include "AsyncFile.h"

class AIOController
{
public:
	jmethodID done;
	jmethodID error;

	jobject logger;
	
	jmethodID loggerError;
	jmethodID loggerWarn;
	jmethodID loggerDebug;
	jmethodID loggerInfo;

	/*
	 * level = 0-error, 1-warn, 2-info, 3-debug
	 */
	void log(THREAD_CONTEXT threadContext, short level, const char * message);
	
	AsyncFile fileOutput;
	
	void destroy(THREAD_CONTEXT context);
	
	AIOController(std::string fileName, int maxIO);
	virtual ~AIOController();
};
#endif /*AIOCONTROLLER_H_*/
