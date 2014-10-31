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

#include <stdio.h>
#include <iostream>
#include <string>
#include "JavaUtilities.h"


void throwRuntimeException(JNIEnv * env, const char * message)
{
  jclass exceptionClass = env->FindClass("java/lang/RuntimeException");
  env->ThrowNew(exceptionClass,message);
  
}

void throwException(JNIEnv * env, const int code, const char * message)
{
  jclass exceptionClass = env->FindClass("org/hornetq/api/core/HornetQException");
  if (exceptionClass==NULL) 
  {
     std::cerr << "Couldn't throw exception message:= " << message << "\n";
     throwRuntimeException (env, "Can't find Exception class");
     return;
  }

  jmethodID constructor = env->GetMethodID(exceptionClass, "<init>", "(ILjava/lang/String;)V");
  if (constructor == NULL)
  {
       std::cerr << "Couldn't find the constructor ***";
       throwRuntimeException (env, "Can't find Constructor for Exception");
       return;
  }

  jstring strError = env->NewStringUTF(message);
  jthrowable ex = (jthrowable)env->NewObject(exceptionClass, constructor, code, strError);
  env->Throw(ex);
  
}

std::string convertJavaString(JNIEnv * env, jstring& jstr)
{
	const char * valueStr = env->GetStringUTFChars(jstr, NULL);
	std::string data(valueStr);
	env->ReleaseStringUTFChars(jstr, valueStr);
	return data;
}

