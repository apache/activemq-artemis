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

#ifndef _GNU_SOURCE
// libaio, O_DIRECT and other things won't be available without this define
#define _GNU_SOURCE
#endif

//#define DEBUG

#include <jni.h>
#include <unistd.h>
#include <errno.h>
#include <libaio.h>
#include <sys/types.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <pthread.h>
#include <limits.h>
#include "org_apache_activemq_artemis_jlibaio_LibaioContext.h"
#include "exception_helper.h"

struct io_control {
    io_context_t ioContext;
    struct io_event * events;

    jobject thisObject;

    // This is used to make sure we don't return IOCB while something else is using them
    // this is to guarantee the submits could be done concurrently with polling
    pthread_mutex_t iocbLock;

    pthread_mutex_t pollLock;

    // a reusable pool of iocb
    struct iocb ** iocb;
    int queueSize;
    int iocbPut;
    int iocbGet;
    int used;

};

// We need a fast and reliable way to stop the blocked poller
// for that we need a dumb file,
// We are using a temporary file for this.
int dumbWriteHandler = 0;
char dumbPath[PATH_MAX];

#define ONE_MEGA 1048576l
void * oneMegaBuffer = 0;
pthread_mutex_t oneMegaMutex;


jclass submitClass = NULL;
jmethodID errorMethod = NULL;
jmethodID doneMethod = NULL;
jmethodID libaioContextDone = NULL;

jclass libaioContextClass = NULL;
jclass runtimeExceptionClass = NULL;
jclass ioExceptionClass = NULL;

// util methods
void throwRuntimeException(JNIEnv* env, char* message) {
    (*env)->ThrowNew(env, runtimeExceptionClass, message);
}

void throwRuntimeExceptionErrorNo(JNIEnv* env, char* message, int errorNumber) {
    char* allocatedMessage = exceptionMessage(message, errorNumber);
    (*env)->ThrowNew(env, runtimeExceptionClass, allocatedMessage);
    free(allocatedMessage);
}

void throwIOException(JNIEnv* env, char* message) {
    (*env)->ThrowNew(env, ioExceptionClass, message);
}

void throwIOExceptionErrorNo(JNIEnv* env, char* message, int errorNumber) {
    char* allocatedMessage = exceptionMessage(message, errorNumber);
    (*env)->ThrowNew(env, ioExceptionClass, allocatedMessage);
    free(allocatedMessage);
}

void throwOutOfMemoryError(JNIEnv* env) {
    jclass exceptionClass = (*env)->FindClass(env, "java/lang/OutOfMemoryError");
    (*env)->ThrowNew(env, exceptionClass, "");
}

/** Notice: every usage of exceptionMessage needs to release the allocated memory for the sequence of char */
char* exceptionMessage(char* msg, int error) {
    if (error < 0) {
        // some functions return negative values
        // and it's hard to keep track of when to send -error and when not
        // this will just take care when things are forgotten
        // what would generate a proper error
        error = error * -1;
    }
    //strerror is returning a constant, so no need to free anything coming from strerror
    char* err = strerror(error);
    char* result = malloc(strlen(msg) + strlen(err) + 1);
    strcpy(result, msg);
    strcat(result, err);
    return result;
}

static inline short verifyBuffer(int alignment) {
    pthread_mutex_lock(&oneMegaMutex);

    if (oneMegaBuffer == 0) {
       #ifdef DEBUG
          fprintf (stdout, "oneMegaBuffer %ld\n", (long) oneMegaBuffer);
       #endif
       if (posix_memalign(&oneMegaBuffer, alignment, ONE_MEGA) != 0) {
            fprintf(stderr, "Could not allocate the 1 Mega Buffer for initializing files\n");
            pthread_mutex_unlock(&oneMegaMutex);
            return -1;
       }
        memset(oneMegaBuffer, 0, ONE_MEGA);
    }

    pthread_mutex_unlock(&oneMegaMutex);

    return 0;

}


jint JNI_OnLoad(JavaVM* vm, void* reserved) {
    JNIEnv* env;
    if ((*vm)->GetEnv(vm, (void**) &env, JNI_VERSION_1_6) != JNI_OK) {
        return JNI_ERR;
    } else {

        int res = pthread_mutex_init(&oneMegaMutex, 0);
        if (res) {
             fprintf(stderr, "could not initialize mutex on on_load, %d", res);
             return JNI_ERR;
        }
        sprintf (dumbPath, "%s/artemisJLHandler_XXXXXX", P_tmpdir);
        dumbWriteHandler = mkstemp (dumbPath);

        #ifdef DEBUG
           fprintf (stdout, "Creating temp file %s for dumb writes\n", dumbPath);
           fflush(stdout);
        #endif

        if (dumbWriteHandler < 0) {
           fprintf (stderr, "couldn't create stop file handler %s\n", dumbPath);
           return JNI_ERR;
        }

        //
        // Accordingly to previous experiences we must hold Global Refs on Classes
        // And
        //
        // Accordingly to IBM recommendations here:
        // We don't need to hold a global reference on methods:
        // http://www.ibm.com/developerworks/java/library/j-jni/index.html#notc
        // Which actually caused core dumps

        jclass localRuntimeExceptionClass = (*env)->FindClass(env, "java/lang/RuntimeException");
        if (localRuntimeExceptionClass == NULL) {
            // pending exception...
            return JNI_ERR;
        }
        runtimeExceptionClass = (jclass) (*env)->NewGlobalRef(env, localRuntimeExceptionClass);
        if (runtimeExceptionClass == NULL) {
            // out-of-memory!
            throwOutOfMemoryError(env);
            return JNI_ERR;
        }

        jclass localIoExceptionClass = (*env)->FindClass(env, "java/io/IOException");
        if (localIoExceptionClass == NULL) {
            // pending exception...
            return JNI_ERR;
        }
        ioExceptionClass = (jclass) (*env)->NewGlobalRef(env, localIoExceptionClass);
        if (ioExceptionClass == NULL) {
            // out-of-memory!
            throwOutOfMemoryError(env);
            return JNI_ERR;
        }

        submitClass = (*env)->FindClass(env, "org/apache/activemq/artemis/jlibaio/SubmitInfo");
        if (submitClass == NULL) {
           return JNI_ERR;
        }

        submitClass = (jclass)(*env)->NewGlobalRef(env, (jobject)submitClass);

        errorMethod = (*env)->GetMethodID(env, submitClass, "onError", "(ILjava/lang/String;)V");
        if (errorMethod == NULL) {
           return JNI_ERR;
        }

        doneMethod = (*env)->GetMethodID(env, submitClass, "done", "()V");
        if (doneMethod == NULL) {
           return JNI_ERR;
        }

        libaioContextClass = (*env)->FindClass(env, "org/apache/activemq/artemis/jlibaio/LibaioContext");
        if (libaioContextClass == NULL) {
           return JNI_ERR;
        }
        libaioContextClass = (jclass)(*env)->NewGlobalRef(env, (jobject)libaioContextClass);

        libaioContextDone = (*env)->GetMethodID(env, libaioContextClass, "done", "(Lorg/apache/activemq/artemis/jlibaio/SubmitInfo;)V");
        if (libaioContextDone == NULL) {
           return JNI_ERR;
        }

        return JNI_VERSION_1_6;
    }
}

inline void closeDumbHandlers() {
    if (dumbWriteHandler != 0) {
        #ifdef DEBUG
           fprintf (stdout, "Closing and removing dump handler %s\n", dumbPath);
        #endif
        dumbWriteHandler = 0;
        close(dumbWriteHandler);
        unlink(dumbPath);
    }
}

void JNI_OnUnload(JavaVM* vm, void* reserved) {
    JNIEnv* env;
    if ((*vm)->GetEnv(vm, (void**) &env, JNI_VERSION_1_6) != JNI_OK) {
        // Something is wrong but nothing we can do about this :(
        return;
    } else {
        closeDumbHandlers();

        if (oneMegaBuffer != 0) {
           free(oneMegaBuffer);
           oneMegaBuffer = 0;
        }

        pthread_mutex_destroy(&oneMegaMutex);

        // delete global references so the GC can collect them
        if (runtimeExceptionClass != NULL) {
            (*env)->DeleteGlobalRef(env, runtimeExceptionClass);
        }
        if (ioExceptionClass != NULL) {
            (*env)->DeleteGlobalRef(env, ioExceptionClass);
        }

        if (submitClass != NULL) {
            (*env)->DeleteGlobalRef(env, (jobject)submitClass);
        }

        if (libaioContextClass != NULL) {
            (*env)->DeleteGlobalRef(env, (jobject)libaioContextClass);
        }
    }
}

JNIEXPORT void JNICALL Java_org_apache_activemq_artemis_jlibaio_LibaioContext_shutdownHook
  (JNIEnv * env, jclass clazz) {
    closeDumbHandlers();
}


static inline struct io_control * getIOControl(JNIEnv* env, jobject pointer) {
    struct io_control * ioControl = (struct io_control *) (*env)->GetDirectBufferAddress(env, pointer);
    if (ioControl == NULL) {
       throwRuntimeException(env, "Controller not initialized");
    }
    return ioControl;
}

/**
 * remove an iocb from the pool of IOCBs. Returns null if full
 */
static inline struct iocb * getIOCB(struct io_control * control) {
    struct iocb * iocb = 0;

    pthread_mutex_lock(&(control->iocbLock));

    #ifdef DEBUG
       fprintf (stdout, "getIOCB::used=%d, queueSize=%d, get=%d, put=%d\n", control->used, control->queueSize, control->iocbGet, control->iocbPut);
    #endif

    if (control->used < control->queueSize) {
        control->used++;
        iocb = control->iocb[control->iocbGet++];

        if (control->iocbGet >= control->queueSize) {
           control->iocbGet = 0;
        }
    }

    pthread_mutex_unlock(&(control->iocbLock));
    return iocb;
}

/**
 * Put an iocb back on the pool of IOCBs
 */
static inline void putIOCB(struct io_control * control, struct iocb * iocbBack) {
    pthread_mutex_lock(&(control->iocbLock));

    #ifdef DEBUG
       fprintf (stdout, "putIOCB::used=%d, queueSize=%d, get=%d, put=%d\n", control->used, control->queueSize, control->iocbGet, control->iocbPut);
    #endif

    control->used--;
    control->iocb[control->iocbPut++] = iocbBack;
    if (control->iocbPut >= control->queueSize) {
       control->iocbPut = 0;
    }
    pthread_mutex_unlock(&(control->iocbLock));
}

static inline short submit(JNIEnv * env, struct io_control * theControl, struct iocb * iocb) {
    int result = io_submit(theControl->ioContext, 1, &iocb);

    if (result < 0) {
        // Putting the Global Ref and IOCB back in case of a failure
        if (iocb->data != NULL && iocb->data != (void *) -1) {
            (*env)->DeleteGlobalRef(env, (jobject)iocb->data);
        }
        putIOCB(theControl, iocb);

        throwIOExceptionErrorNo(env, "Error while submitting IO: ", -result);
        return 0;
    }

    return 1;
}

static inline void * getBuffer(JNIEnv* env, jobject pointer) {
    return (*env)->GetDirectBufferAddress(env, pointer);
}

JNIEXPORT jboolean JNICALL Java_org_apache_activemq_artemis_jlibaio_LibaioContext_lock
  (JNIEnv * env, jclass  clazz, jint handle) {
    return flock(handle, LOCK_EX | LOCK_NB) == 0;
}

/**
 * Everything that is allocated here will be freed at deleteContext when the class is unloaded.
 */
JNIEXPORT jobject JNICALL Java_org_apache_activemq_artemis_jlibaio_LibaioContext_newContext(JNIEnv* env, jobject thisObject, jint queueSize) {
    io_context_t libaioContext;
    int i = 0;

    #ifdef DEBUG
        fprintf (stdout, "Initializing context\n");
    #endif

    int res = io_queue_init(queueSize, &libaioContext);
    if (res) {
        // Error, so need to release whatever was done before
        free(libaioContext);

        throwRuntimeExceptionErrorNo(env, "Cannot initialize queue:", res);
        return NULL;
    }

    struct iocb ** iocb = (struct iocb **)malloc((sizeof(struct iocb *) * (size_t)queueSize));
    if (iocb == NULL) {
       throwOutOfMemoryError(env);
       return NULL;
    }

    for (i = 0; i < queueSize; i++) {
       iocb[i] = (struct iocb *)malloc(sizeof(struct iocb));
       if (iocb[i] == NULL) {
           // it's unlikely this would happen at this point
           // for that reason I'm not cleaning up individual IOCBs here
           // we could increase support here with a cleanup of any previously allocated iocb
           // But I'm afraid of adding not needed complexity here
           throwOutOfMemoryError(env);
           return NULL;
       }
    }

    struct io_control * theControl = (struct io_control *) malloc(sizeof(struct io_control));
    if (theControl == NULL) {
        throwOutOfMemoryError(env);
        return NULL;
    }

    res = pthread_mutex_init(&(theControl->iocbLock), 0);
    if (res) {
        free(theControl);
        free(libaioContext);
        throwRuntimeExceptionErrorNo(env, "Can't initialize mutext:", res);
        return NULL;
    }

    res = pthread_mutex_init(&(theControl->pollLock), 0);
    if (res) {
        free(theControl);
        free(libaioContext);
        throwRuntimeExceptionErrorNo(env, "Can't initialize mutext:", res);
        return NULL;
    }

    struct io_event * events = (struct io_event *)malloc(sizeof(struct io_event) * (size_t)queueSize);

    theControl->ioContext = libaioContext;
    theControl->events = events;
    theControl->iocb = iocb;
    theControl->queueSize = queueSize;
    theControl->iocbPut = 0;
    theControl->iocbGet = 0;
    theControl->used = 0;
    theControl->thisObject = (*env)->NewGlobalRef(env, thisObject);

    return (*env)->NewDirectByteBuffer(env, theControl, sizeof(struct io_control));
}

JNIEXPORT void JNICALL Java_org_apache_activemq_artemis_jlibaio_LibaioContext_deleteContext(JNIEnv* env, jclass clazz, jobject contextPointer) {
    int i;
    struct io_control * theControl = getIOControl(env, contextPointer);
    if (theControl == NULL) {
      return;
    }

    struct iocb * iocb = getIOCB(theControl);

    if (iocb == NULL) {
        throwIOException(env, "Not enough space in libaio queue");
        return;
    }

    // Submitting a dumb write so the loop finishes
    io_prep_pwrite(iocb, dumbWriteHandler, 0, 0, 0);
    iocb->data = (void *) -1;
    if (!submit(env, theControl, iocb)) {
        return;
    }

    // to make sure the poll has finished
    pthread_mutex_lock(&(theControl->pollLock));
    pthread_mutex_unlock(&(theControl->pollLock));

    // To return any pending IOCBs
    int result = io_getevents(theControl->ioContext, 0, 1, theControl->events, 0);
    for (i = 0; i < result; i++) {
        struct io_event * event = &(theControl->events[i]);
        struct iocb * iocbp = event->obj;
        putIOCB(theControl, iocbp);
    }

    io_queue_release(theControl->ioContext);

    pthread_mutex_destroy(&(theControl->pollLock));
    pthread_mutex_destroy(&(theControl->iocbLock));

    // Releasing each individual iocb
    for (i = 0; i < theControl->queueSize; i++) {
       free(theControl->iocb[i]);
    }

    (*env)->DeleteGlobalRef(env, theControl->thisObject);

    free(theControl->iocb);
    free(theControl->events);
    free(theControl);
}

JNIEXPORT void JNICALL Java_org_apache_activemq_artemis_jlibaio_LibaioContext_close(JNIEnv* env, jclass clazz, jint fd) {
   if (close(fd) < 0) {
       throwIOExceptionErrorNo(env, "Error closing file:", errno);
   }
}

JNIEXPORT int JNICALL Java_org_apache_activemq_artemis_jlibaio_LibaioContext_open(JNIEnv* env, jclass clazz,
                        jstring path, jboolean direct) {
    const char* f_path = (*env)->GetStringUTFChars(env, path, 0);

    int res;
    if (direct) {
      res = open(f_path, O_RDWR | O_CREAT | O_DIRECT, 0666);
    } else {
      res = open(f_path, O_RDWR | O_CREAT, 0666);
    }

    (*env)->ReleaseStringUTFChars(env, path, f_path);

    if (res < 0) {
       throwIOExceptionErrorNo(env, "Cannot open file:", errno);
    }

    return res;
}

JNIEXPORT void JNICALL Java_org_apache_activemq_artemis_jlibaio_LibaioContext_submitWrite
  (JNIEnv * env, jclass clazz, jint fileHandle, jobject contextPointer, jlong position, jint size, jobject bufferWrite, jobject callback) {
    struct io_control * theControl = getIOControl(env, contextPointer);
    if (theControl == NULL) {
      return;
    }

    #ifdef DEBUG
       fprintf (stdout, "submitWrite position %ld, size %d\n", position, size);
    #endif

    struct iocb * iocb = getIOCB(theControl);

    if (iocb == NULL) {
        throwIOException(env, "Not enough space in libaio queue");
        return;
    }

    io_prep_pwrite(iocb, fileHandle, getBuffer(env, bufferWrite), (size_t)size, position);

    // The GlobalRef will be deleted when poll is called. this is done so
    // the vm wouldn't crash if the Callback passed by the user is GCed between submission
    // and callback.
    // also as the real intention is to hold the reference until the life cycle is complete
    iocb->data = (void *) (*env)->NewGlobalRef(env, callback);

    submit(env, theControl, iocb);
}

JNIEXPORT void JNICALL Java_org_apache_activemq_artemis_jlibaio_LibaioContext_submitRead
  (JNIEnv * env, jclass clazz, jint fileHandle, jobject contextPointer, jlong position, jint size, jobject bufferRead, jobject callback) {
    struct io_control * theControl = getIOControl(env, contextPointer);
    if (theControl == NULL) {
      return;
    }

    struct iocb * iocb = getIOCB(theControl);

    if (iocb == NULL) {
        throwIOException(env, "Not enough space in libaio queue");
        return;
    }

    io_prep_pread(iocb, fileHandle, getBuffer(env, bufferRead), (size_t)size, position);

    // The GlobalRef will be deleted when poll is called. this is done so
    // the vm wouldn't crash if the Callback passed by the user is GCed between submission
    // and callback.
    // also as the real intention is to hold the reference until the life cycle is complete
    iocb->data = (void *) (*env)->NewGlobalRef(env, callback);

    submit(env, theControl, iocb);
}

JNIEXPORT void JNICALL Java_org_apache_activemq_artemis_jlibaio_LibaioContext_blockedPoll
  (JNIEnv * env, jobject thisObject, jobject contextPointer, jboolean useFdatasync) {

    #ifdef DEBUG
       fprintf (stdout, "Running blockedPoll\n");
       fflush(stdout);
    #endif

    int i;
    struct io_control * theControl = getIOControl(env, contextPointer);
    if (theControl == NULL) {
      return;
    }
    int max = theControl->queueSize;
    pthread_mutex_lock(&(theControl->pollLock));

    short running = 1;

    int lastFile = -1;

    while (running) {

        int result = io_getevents(theControl->ioContext, 1, max, theControl->events, 0);

        if (result == -EINTR)
        {
           // ARTEMIS-353: jmap will issue some weird interrupt signal what would break the execution here
           // we need to ignore such calls here
           continue;
        }

        if (result < 0)
        {
            throwIOExceptionErrorNo(env, "Error while calling io_getevents IO: ", -result);
            break;
        }
        #ifdef DEBUG
           fprintf (stdout, "blockedPoll returned %d events\n", result);
           fflush(stdout);
        #endif

        lastFile = -1;

        for (i = 0; i < result; i++)
        {
            #ifdef DEBUG
               fprintf (stdout, "blockedPoll treating event %d\n", i);
               fflush(stdout);
            #endif
            struct io_event * event = &(theControl->events[i]);
            struct iocb * iocbp = event->obj;

            if (iocbp->aio_fildes == dumbWriteHandler) {
               #ifdef DEBUG
                  fprintf (stdout, "Dumb write arrived, giving up the loop\n");
                  fflush(stdout);
               #endif
               putIOCB(theControl, iocbp);
               running = 0;
               break;
            }

            if (useFdatasync && lastFile != iocbp->aio_fildes) {
                lastFile = iocbp->aio_fildes;
                fdatasync(lastFile);
            }


            int eventResult = (int)event->res;

            #ifdef DEBUG
                fprintf (stdout, "Poll res: %d totalRes=%d\n", eventResult, result);
                fflush (stdout);
            #endif

            if (eventResult < 0) {
                #ifdef DEBUG
                    fprintf (stdout, "Error: %s\n", strerror(-eventResult));
                    fflush (stdout);
                #endif

                jstring jstrError = (*env)->NewStringUTF(env, strerror(-eventResult));

                if (iocbp->data != NULL) {
                    (*env)->CallVoidMethod(env, (jobject)(iocbp->data), errorMethod, (jint)(-eventResult), jstrError);
                }
            }

            jobject obj = (jobject)iocbp->data;
            putIOCB(theControl, iocbp);

            if (obj != NULL) {
                (*env)->CallVoidMethod(env, theControl->thisObject, libaioContextDone,obj);
                // We delete the globalRef after the completion of the callback
                (*env)->DeleteGlobalRef(env, obj);
            }

        }
    }

    pthread_mutex_unlock(&(theControl->pollLock));

}

JNIEXPORT jint JNICALL Java_org_apache_activemq_artemis_jlibaio_LibaioContext_poll
  (JNIEnv * env, jobject obj, jobject contextPointer, jobjectArray callbacks, jint min, jint max) {
    int i = 0;
    struct io_control * theControl = getIOControl(env, contextPointer);
    if (theControl == NULL) {
      return 0;
    }


    int result = io_getevents(theControl->ioContext, min, max, theControl->events, 0);
    int retVal = result;

    for (i = 0; i < result; i++) {
        struct io_event * event = &(theControl->events[i]);
        struct iocb * iocbp = event->obj;
        int eventResult = (int)event->res;

        #ifdef DEBUG
            fprintf (stdout, "Poll res: %d totalRes=%d\n", eventResult, result);
        #endif

        if (eventResult < 0) {
            #ifdef DEBUG
                fprintf (stdout, "Error: %s\n", strerror(-eventResult));
            #endif

            if (iocbp->data != NULL && iocbp->data != (void *) -1) {
                jstring jstrError = (*env)->NewStringUTF(env, strerror(-eventResult));

                (*env)->CallVoidMethod(env, (jobject)(iocbp->data), errorMethod, (jint)(-eventResult), jstrError);
            }
        }

        if (iocbp->data != NULL && iocbp->data != (void *) -1) {
            (*env)->SetObjectArrayElement(env, callbacks, i, (jobject)iocbp->data);
            // We delete the globalRef after the completion of the callback
            (*env)->DeleteGlobalRef(env, (jobject)iocbp->data);
        }

        putIOCB(theControl, iocbp);
    }

    return retVal;
}

JNIEXPORT jobject JNICALL Java_org_apache_activemq_artemis_jlibaio_LibaioContext_newAlignedBuffer
(JNIEnv * env, jclass clazz, jint size, jint alignment) {
    if (size % alignment != 0) {
        throwRuntimeException(env, "Buffer size needs to be aligned to passed argument");
        return NULL;
    }

    // This will allocate a buffer, aligned by alignment.
    // Buffers created here need to be manually destroyed by destroyBuffer, or this would leak on the process heap away of Java's GC managed memory
    // NOTE: this buffer will contain non initialized data, you must fill it up properly
    void * buffer;
    int result = posix_memalign(&buffer, (size_t)alignment, (size_t)size);

    if (result) {
        throwRuntimeExceptionErrorNo(env, "Can't allocate posix buffer:", result);
        return NULL;
    }

    memset(buffer, 0, (size_t)size);

    return (*env)->NewDirectByteBuffer(env, buffer, size);
}

JNIEXPORT void JNICALL Java_org_apache_activemq_artemis_jlibaio_LibaioContext_freeBuffer
  (JNIEnv * env, jclass clazz, jobject jbuffer) {
    if (jbuffer == NULL)
    {
       throwRuntimeException(env, "Null pointer");
       return;
    }
  	void *  buffer = (*env)->GetDirectBufferAddress(env, jbuffer);
  	free(buffer);
}


/** It does nothing... just return true to make sure it has all the binary dependencies */
JNIEXPORT jint JNICALL Java_org_apache_activemq_artemis_jlibaio_LibaioContext_getNativeVersion
  (JNIEnv * env, jclass clazz)

{
     return org_apache_activemq_artemis_jlibaio_LibaioContext_EXPECTED_NATIVE_VERSION;
}

JNIEXPORT jlong JNICALL Java_org_apache_activemq_artemis_jlibaio_LibaioContext_getSize
  (JNIEnv * env, jclass clazz, jint fd)
{
    struct stat statBuffer;

    if (fstat(fd, &statBuffer) < 0)
    {
        throwIOExceptionErrorNo(env, "Cannot determine file size:", errno);
        return -1l;
    }
    return statBuffer.st_size;
}

JNIEXPORT jint JNICALL Java_org_apache_activemq_artemis_jlibaio_LibaioContext_getBlockSizeFD
  (JNIEnv * env, jclass clazz, jint fd)
{
    struct stat statBuffer;

    if (fstat(fd, &statBuffer) < 0)
    {
        throwIOExceptionErrorNo(env, "Cannot determine file size:", errno);
        return -1l;
    }
    return statBuffer.st_blksize;
}

JNIEXPORT jint JNICALL Java_org_apache_activemq_artemis_jlibaio_LibaioContext_getBlockSize
  (JNIEnv * env, jclass clazz, jstring path)
{
    const char* f_path = (*env)->GetStringUTFChars(env, path, 0);
    struct stat statBuffer;

    if (stat(f_path, &statBuffer) < 0)
    {
        throwIOExceptionErrorNo(env, "Cannot determine file size:", errno);
        return -1l;
    }

    (*env)->ReleaseStringUTFChars(env, path, f_path);

    return statBuffer.st_blksize;
}

JNIEXPORT void JNICALL Java_org_apache_activemq_artemis_jlibaio_LibaioContext_fallocate
  (JNIEnv * env, jclass clazz, jint fd, jlong size)
{
    if (fallocate(fd, 0, 0, (off_t) size) < 0)
    {
        throwIOExceptionErrorNo(env, "Could not preallocate file", errno);
    }
    fsync(fd);
    lseek (fd, 0, SEEK_SET);
}

JNIEXPORT void JNICALL Java_org_apache_activemq_artemis_jlibaio_LibaioContext_fill
  (JNIEnv * env, jclass clazz, jint fd, jint alignment, jlong size)
{

    int i;
    int blocks = size / ONE_MEGA;
    int rest = size % ONE_MEGA;

    #ifdef DEBUG
        fprintf (stdout, "calling fill ... blocks = %d, rest=%d, alignment=%d\n", blocks, rest, alignment);
    #endif


    verifyBuffer(alignment);

    lseek (fd, 0, SEEK_SET);
    for (i = 0; i < blocks; i++)
    {
        if (write(fd, oneMegaBuffer, ONE_MEGA) < 0)
        {
            #ifdef DEBUG
               fprintf (stdout, "Errno is %d\n", errno);
            #endif
            throwIOException(env, "Cannot initialize file");
            return;
        }
    }

    if (rest != 0l)
    {
       if (write(fd, oneMegaBuffer, rest) < 0)
       {
            #ifdef DEBUG
               fprintf (stdout, "Errno is %d\n", errno);
            #endif
           throwIOException(env, "Cannot initialize file with final rest");
           return;
       }
    }
    lseek (fd, 0, SEEK_SET);
}

JNIEXPORT void JNICALL Java_org_apache_activemq_artemis_jlibaio_LibaioContext_memsetBuffer
  (JNIEnv *env, jclass clazz, jobject jbuffer, jint size)
{
    #ifdef DEBUG
        fprintf (stdout, "Mem setting buffer with %d bytes\n", size);
    #endif
    void * buffer = (*env)->GetDirectBufferAddress(env, jbuffer);

    if (buffer == 0)
    {
        throwRuntimeException(env, "Invalid Buffer used, libaio requires NativeBuffer instead of Java ByteBuffer");
        return;
    }

    memset(buffer, 0, (size_t)size);
}
