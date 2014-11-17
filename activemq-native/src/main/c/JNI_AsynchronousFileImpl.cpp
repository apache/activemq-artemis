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
#include <stdlib.h>
#include <iostream>
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <string>
#include <time.h>
#include <sys/file.h>

#include "org.apache.activemq6_core_libaio_Native.h"


#include "JavaUtilities.h"
#include "AIOController.h"
#include "JNICallbackAdapter.h"
#include "AIOException.h"
#include "Version.h"


// This value is set here globally, to avoid passing stuff on stack between java and the native layer on every sleep call
struct timespec nanoTime;

inline AIOController * getController(JNIEnv *env, jobject & controllerAddress)
{
     return (AIOController *) env->GetDirectBufferAddress(controllerAddress);
} 

/* Inaccessible static: log */
/* Inaccessible static: totalMaxIO */
/* Inaccessible static: loaded */
/* Inaccessible static: EXPECTED_NATIVE_VERSION */
/*
 * Class:     org.apache.activemq6_core_asyncio_impl_AsynchronousFileImpl
 * Method:    openFile
 * Signature: (Ljava/lang/String;)I
 */
JNIEXPORT jint JNICALL Java_org.apache.activemq6_core_libaio_Native_openFile
  (JNIEnv * env , jclass , jstring jstrFileName)
{
	std::string fileName = convertJavaString(env, jstrFileName);

    return open(fileName.data(), O_RDWR | O_CREAT, 0666);
}

/*
 * Class:     org.apache.activemq6_core_asyncio_impl_AsynchronousFileImpl
 * Method:    closeFile
 * Signature: (I)V
 */
JNIEXPORT void JNICALL Java_org.apache.activemq6_core_libaio_Native_closeFile
  (JNIEnv * , jclass , jint handle)
{
   close(handle);
}

/*
 * Class:     org.apache.activemq6_core_asyncio_impl_AsynchronousFileImpl
 * Method:    flock
 * Signature: (I)Z
 */
JNIEXPORT jboolean JNICALL Java_org.apache.activemq6_core_libaio_Native_flock
  (JNIEnv * , jclass , jint handle)
{
    return flock(handle, LOCK_EX | LOCK_NB) == 0;
}



/*
 * Class:     org_jboss_jaio_libaioimpl_LibAIOController
 * Method:    init
 * Signature: (Ljava/lang/String;Ljava/lang/Class;)J
 */
JNIEXPORT jobject JNICALL Java_org.apache.activemq6_core_libaio_Native_init
  (JNIEnv * env, jclass, jclass controllerClazz, jstring jstrFileName, jint maxIO, jobject logger)
{
	AIOController * controller = 0;
	try
	{
		std::string fileName = convertJavaString(env, jstrFileName);

		controller = new AIOController(fileName, (int) maxIO);
		controller->done = env->GetMethodID(controllerClazz,"callbackDone","(Lorg.apache.activemq6/core/asyncio/AIOCallback;JLjava/nio/ByteBuffer;)V");
		if (!controller->done)
		{
		   throwException (env, -1, "can't get callbackDone method");
		   return 0;
		}

		controller->error = env->GetMethodID(controllerClazz, "callbackError", "(Lorg.apache.activemq6/core/asyncio/AIOCallback;JLjava/nio/ByteBuffer;ILjava/lang/String;)V");
		if (!controller->done)
		{
		   throwException (env, -1, "can't get callbackError method");
		   return 0;
		}

        jclass loggerClass = env->GetObjectClass(logger);

        if (!(controller->loggerDebug = env->GetMethodID(loggerClass, "debug", "(Ljava/lang/Object;)V"))) return 0;
        if (!(controller->loggerWarn = env->GetMethodID(loggerClass, "warn", "(Ljava/lang/Object;)V"))) return 0;
        if (!(controller->loggerInfo = env->GetMethodID(loggerClass, "info", "(Ljava/lang/Object;)V"))) return 0;
        if (!(controller->loggerError = env->GetMethodID(loggerClass, "error", "(Ljava/lang/Object;)V"))) return 0;

        controller->logger = env->NewGlobalRef(logger);

		return env->NewDirectByteBuffer(controller, 0);
	}
	catch (AIOException& e){
		if (controller != 0)
		{
			delete controller;
		}
		throwException(env, e.getErrorCode(), e.what());
		return 0;
	}
}

/**
* objThis here is passed as a parameter at the java layer. It used to be a JNI this and now it's a java static method
  where the intended reference is now passed as an argument
*/
JNIEXPORT void JNICALL Java_org.apache.activemq6_core_libaio_Native_read
  (JNIEnv *env, jclass, jobject objThis, jobject controllerAddress, jlong position, jlong size, jobject jbuffer, jobject callback)
{
	try
	{
		AIOController * controller = getController(env, controllerAddress);
		void * buffer = env->GetDirectBufferAddress(jbuffer);

		if (buffer == 0)
		{
			throwException(env, NATIVE_ERROR_INVALID_BUFFER, "Invalid Buffer used, libaio requires NativeBuffer instead of Java ByteBuffer");
			return;
		}

		if (((long)buffer) % 512)
		{
			throwException(env, NATIVE_ERROR_NOT_ALIGNED, "Buffer not aligned for use with DMA");
			return;
		}

		CallbackAdapter * adapter = new JNICallbackAdapter(controller, -1, env->NewGlobalRef(callback), env->NewGlobalRef(objThis), env->NewGlobalRef(jbuffer), true);

		controller->fileOutput.read(env, position, (size_t)size, buffer, adapter);
	}
	catch (AIOException& e)
	{
		throwException(env, e.getErrorCode(), e.what());
	}
}


// Fast memset on buffer
JNIEXPORT void JNICALL Java_org.apache.activemq6_core_libaio_Native_resetBuffer
  (JNIEnv *env, jclass, jobject jbuffer, jint size)
{
	void * buffer = env->GetDirectBufferAddress(jbuffer);

	if (buffer == 0)
	{
		throwException(env, NATIVE_ERROR_INVALID_BUFFER, "Invalid Buffer used, libaio requires NativeBuffer instead of Java ByteBuffer");
		return;
	}

	memset(buffer, 0, (size_t)size);

}

JNIEXPORT void JNICALL Java_org.apache.activemq6_core_libaio_Native_destroyBuffer
  (JNIEnv * env, jclass, jobject jbuffer)
{
    if (jbuffer == 0)
    {
		throwException(env, NATIVE_ERROR_INVALID_BUFFER, "Null Buffer");
		return;
    }
	void *  buffer = env->GetDirectBufferAddress(jbuffer);
	free(buffer);
}

JNIEXPORT jobject JNICALL Java_org.apache.activemq6_core_libaio_Native_newNativeBuffer
  (JNIEnv * env, jclass, jlong size)
{
	try
	{

		if (size % ALIGNMENT)
		{
			throwException(env, NATIVE_ERROR_INVALID_BUFFER, "Buffer size needs to be aligned to 512");
			return 0;
		}


		// This will allocate a buffer, aligned by 512.
		// Buffers created here need to be manually destroyed by destroyBuffer, or this would leak on the process heap away of Java's GC managed memory
		void * buffer = 0;
		if (::posix_memalign(&buffer, 512, size))
		{
			throwException(env, NATIVE_ERROR_INTERNAL, "Error on posix_memalign");
			return 0;
		}

		memset(buffer, 0, (size_t)size);

		jobject jbuffer = env->NewDirectByteBuffer(buffer, size);
		return jbuffer;
	}
	catch (AIOException& e)
	{
		throwException(env, e.getErrorCode(), e.what());
		return 0;
	}
}

/**
* objThis here is passed as a parameter at the java layer. It used to be a JNI this and now it's a java static method
  where the intended reference is now passed as an argument
*/
JNIEXPORT void JNICALL Java_org.apache.activemq6_core_libaio_Native_write
  (JNIEnv *env, jclass, jobject objThis, jobject controllerAddress, jlong sequence, jlong position, jlong size, jobject jbuffer, jobject callback)
{
	try
	{
		AIOController * controller = getController(env, controllerAddress);
		void * buffer = env->GetDirectBufferAddress(jbuffer);

		if (buffer == 0)
		{
			throwException(env, NATIVE_ERROR_INVALID_BUFFER, "Invalid Buffer used, libaio requires NativeBuffer instead of Java ByteBuffer");
			return;
		}


		CallbackAdapter * adapter = new JNICallbackAdapter(controller, sequence, env->NewGlobalRef(callback), env->NewGlobalRef(objThis), env->NewGlobalRef(jbuffer), false);

		controller->fileOutput.write(env, position, (size_t)size, buffer, adapter);
	}
	catch (AIOException& e)
	{
		throwException(env, e.getErrorCode(), e.what());
	}
}

JNIEXPORT void JNICALL Java_org.apache.activemq6_core_libaio_Native_writeInternal
  (JNIEnv * env, jclass, jobject controllerAddress, jlong positionToWrite, jlong size, jobject jbuffer)
{
	try
	{
		AIOController * controller = getController(env, controllerAddress);
		void * buffer = env->GetDirectBufferAddress(jbuffer);

		if (buffer == 0)
		{
			throwException(env, NATIVE_ERROR_INVALID_BUFFER, "Invalid Buffer used, libaio requires NativeBuffer instead of Java ByteBuffer");
			return;
		}

		controller->fileOutput.writeInternal(env, positionToWrite, (size_t)size, buffer);
	}
	catch (AIOException& e)
	{
		throwException(env, e.getErrorCode(), e.what());
	}
}


JNIEXPORT void Java_org.apache.activemq6_core_libaio_Native_internalPollEvents
  (JNIEnv *env, jclass, jobject controllerAddress)
{
	try
	{
		AIOController * controller = getController(env, controllerAddress);
		controller->fileOutput.pollEvents(env);
	}
	catch (AIOException& e)
	{
		throwException(env, e.getErrorCode(), e.what());
	}
}

JNIEXPORT void JNICALL Java_org.apache.activemq6_core_libaio_Native_stopPoller
  (JNIEnv *env, jclass, jobject controllerAddress)
{
	try
	{
		AIOController * controller = getController(env, controllerAddress);
		controller->fileOutput.stopPoller(env);
	}
	catch (AIOException& e)
	{
		throwException(env, e.getErrorCode(), e.what());
	}
}

JNIEXPORT void JNICALL Java_org.apache.activemq6_core_libaio_Native_closeInternal
  (JNIEnv *env, jclass, jobject controllerAddress)
{
	try
	{
		AIOController * controller = getController(env, controllerAddress);
		controller->destroy(env);
		delete controller;
	}
	catch (AIOException& e)
	{
		throwException(env, e.getErrorCode(), e.what());
	}
}


JNIEXPORT void JNICALL Java_org.apache.activemq6_core_libaio_Native_fill
  (JNIEnv * env, jclass, jobject controllerAddress, jlong position, jint blocks, jlong size, jbyte fillChar)
{
	try
	{
		AIOController * controller = getController(env, controllerAddress);

		controller->fileOutput.preAllocate(env, position, blocks, size, fillChar);

	}
	catch (AIOException& e)
	{
		throwException(env, e.getErrorCode(), e.what());
	}
}



/** It does nothing... just return true to make sure it has all the binary dependencies */
JNIEXPORT jint JNICALL Java_org.apache.activemq6_core_libaio_Native_getNativeVersion
  (JNIEnv *, jclass)

{
     return _VERSION_NATIVE_AIO;
}


JNIEXPORT jlong JNICALL Java_org.apache.activemq6_core_libaio_Native_size0
  (JNIEnv * env, jclass, jobject controllerAddress)
{
	try
	{
		AIOController * controller = getController(env, controllerAddress);

		long size = controller->fileOutput.getSize();
		if (size < 0)
		{
			throwException(env, NATIVE_ERROR_INTERNAL, "InternalError on Native Layer: method size failed");
			return -1l;
		}
		return size;
	}
	catch (AIOException& e)
	{
		throwException(env, e.getErrorCode(), e.what());
		return -1l;
	}

}
