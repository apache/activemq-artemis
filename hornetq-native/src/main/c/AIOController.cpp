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


#include <string>
#include "AIOController.h"
#include "JavaUtilities.h"
#include "JAIODatatypes.h"

AIOController::AIOController(std::string fileName, int maxIO) : logger(0), fileOutput(fileName, this, maxIO) 
{
}

void AIOController::log(THREAD_CONTEXT threadContext, short level, const char * message)
{
	jmethodID methodID = 0;
	
	switch (level)
	{
	case 0: methodID = loggerError; break;
	case 1: methodID = loggerWarn; break;
	case 2: methodID = loggerInfo; break;
	case 3: methodID = loggerDebug; break;
	default: methodID = loggerDebug; break;
	}

#ifdef DEBUG
	fprintf (stderr,"Callig log methodID=%ld, message=%s, logger=%ld, threadContext = %ld\n", (long) methodID, message, (long) logger, (long) threadContext); fflush(stderr);
#endif
	threadContext->CallVoidMethod(logger,methodID,threadContext->NewStringUTF(message));
}


void AIOController::destroy(THREAD_CONTEXT context)
{
	if (logger != 0)
	{
		context->DeleteGlobalRef(logger);
	}
}

/*
 * level = 0-error, 1-warn, 2-info, 3-debug
 */


AIOController::~AIOController()
{
}
