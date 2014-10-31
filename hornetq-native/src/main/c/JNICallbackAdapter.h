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

#ifndef JNIBUFFERADAPTER_H_
#define JNIBUFFERADAPTER_H_

#include <iostream>

#include "CallbackAdapter.h"
#include "AIOController.h"
#include "JAIODatatypes.h"


class JNICallbackAdapter : public CallbackAdapter
{
private:

	AIOController * controller;
	
	jobject callback;
	
	jobject fileController;
	
	jobject bufferReference;
	
	jlong sequence;
	
	// Is this a read operation
	short isRead;

	void release(THREAD_CONTEXT threadContext)
	{
		JNI_ENV(threadContext)->DeleteGlobalRef(callback);
		JNI_ENV(threadContext)->DeleteGlobalRef(fileController);
		JNI_ENV(threadContext)->DeleteGlobalRef(bufferReference);
		delete this;
		return;
	}
	
	
public:
	// _ob must be a global Reference (use createGloblReferente before calling the constructor)
	JNICallbackAdapter(AIOController * _controller, jlong sequence, jobject _callback, jobject _fileController, jobject _bufferReference, short _isRead);
	virtual ~JNICallbackAdapter();

	void done(THREAD_CONTEXT threadContext);

	void onError(THREAD_CONTEXT , long , std::string );

	
};
#endif /*JNIBUFFERADAPTER_H_*/
