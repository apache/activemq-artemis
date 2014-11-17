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

#include <jni.h>
#include "JNICallbackAdapter.h"
#include <iostream>
#include "JavaUtilities.h"

jobject nullObj = NULL;

JNICallbackAdapter::JNICallbackAdapter(AIOController * _controller, jlong _sequence, jobject _callback, jobject _fileController, jobject _bufferReference, short _isRead) : CallbackAdapter()
{
	controller = _controller;

	sequence = _sequence;

	callback = _callback;

	fileController = _fileController;

	bufferReference = _bufferReference;

	isRead = _isRead;

}

JNICallbackAdapter::~JNICallbackAdapter()
{
}

void JNICallbackAdapter::done(THREAD_CONTEXT threadContext)
{
	JNI_ENV(threadContext)->CallVoidMethod(fileController, controller->done, callback,  sequence, isRead ? nullObj : bufferReference); 

	release(threadContext);
}

void JNICallbackAdapter::onError(THREAD_CONTEXT threadContext, long errorCode, std::string error)
{
	controller->log(threadContext, 0, "Libaio event generated errors, callback object was informed about it");

	jstring strError = JNI_ENV(threadContext)->NewStringUTF(error.data());

	JNI_ENV(threadContext)->CallVoidMethod(fileController, controller->error, callback, sequence, isRead ? nullObj : bufferReference, (jint)errorCode, strError);

	release(threadContext);
}

