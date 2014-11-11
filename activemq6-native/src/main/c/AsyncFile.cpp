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

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif


#include <stdlib.h>
#include <list>
#include <iostream>
#include <sstream>
#include <memory.h>
#include <errno.h>
#include <libaio.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include "AsyncFile.h"
#include "AIOController.h"
#include "AIOException.h"
#include "pthread.h"
#include "LockClass.h"
#include "CallbackAdapter.h"
#include "LockClass.h"

//#define DEBUG

#define WAIT_FOR_SPOT 10000
#define TRIES_BEFORE_WARN 0
#define TRIES_BEFORE_ERROR 500


std::string io_error(int rc)
{
	std::stringstream buffer;

	if (rc == -ENOSYS)
		buffer << "AIO not in this kernel";
	else
		buffer << "Error:= " << strerror((int)-rc);

	return buffer.str();
}


AsyncFile::AsyncFile(std::string & _fileName, AIOController * _controller, int _maxIO) : aioContext(0), events(0), fileHandle(0), controller(_controller), pollerRunning(0)
{
	::pthread_mutex_init(&fileMutex,0);
	::pthread_mutex_init(&pollerMutex,0);

	maxIO = _maxIO;
	fileName = _fileName;
	if (io_queue_init(maxIO, &aioContext))
	{
		throw AIOException(NATIVE_ERROR_CANT_INITIALIZE_AIO, "Can't initialize aio, out of AIO Handlers");
	}

	fileHandle = ::open(fileName.data(),  O_RDWR | O_CREAT | O_DIRECT, 0666);
	if (fileHandle < 0)
	{
		io_queue_release(aioContext);
		throw AIOException(NATIVE_ERROR_CANT_OPEN_CLOSE_FILE, "Can't open file");
	}

#ifdef DEBUG
	fprintf (stderr,"File Handle %d", fileHandle);
#endif

	events = (struct io_event *)malloc (maxIO * sizeof (struct io_event));

	if (events == 0)
	{
		throw AIOException (NATIVE_ERROR_CANT_ALLOCATE_QUEUE, "Can't allocate ioEvents");
	}

}

AsyncFile::~AsyncFile()
{
	if (io_queue_release(aioContext))
	{
		throw AIOException(NATIVE_ERROR_CANT_RELEASE_AIO,"Can't release aio");
	}
	if (::close(fileHandle))
	{
		throw AIOException(NATIVE_ERROR_CANT_OPEN_CLOSE_FILE,"Can't close file");
	}
	free(events);
	::pthread_mutex_destroy(&fileMutex);
	::pthread_mutex_destroy(&pollerMutex);
}

int isException (THREAD_CONTEXT threadContext)
{
	return JNI_ENV(threadContext)->ExceptionOccurred() != 0;
}

void AsyncFile::pollEvents(THREAD_CONTEXT threadContext)
{

	LockClass lock(&pollerMutex);
	pollerRunning=1;


	while (pollerRunning)
	{
		if (isException(threadContext))
		{
			return;
		}
		int result = io_getevents(this->aioContext, 1, maxIO, events, 0);


#ifdef DEBUG
		fprintf (stderr, "poll, pollerRunning=%d\n", pollerRunning); fflush(stderr);
#endif

		if (result > 0)
		{

#ifdef DEBUG
			fprintf (stdout, "Received %d events\n", result);
			fflush(stdout);
#endif
		}

		for (int i=0; i<result; i++)
		{

			struct iocb * iocbp = events[i].obj;

			if (iocbp->data == (void *) -1)
			{
				pollerRunning = 0;
#ifdef DEBUG
				controller->log(threadContext, 2, "Received poller request to stop");
#endif
			}
			else
			{
				CallbackAdapter * adapter = (CallbackAdapter *) iocbp->data;

				long result = events[i].res;
				if (result < 0)
				{
					std::string strerror = io_error((int)result);
					adapter->onError(threadContext, result, strerror);
				}
				else
				{
					adapter->done(threadContext);
				}
			}

			delete iocbp;
		}
	}
#ifdef DEBUG
	controller->log(threadContext, 2, "Poller finished execution");
#endif
}


void AsyncFile::preAllocate(THREAD_CONTEXT , off_t position, int blocks, size_t size, int fillChar)
{

	if (size % ALIGNMENT != 0)
	{
		throw AIOException (NATIVE_ERROR_PREALLOCATE_FILE, "You can only pre allocate files in multiples of 512");
	}

	void * preAllocBuffer = 0;
	if (posix_memalign(&preAllocBuffer, 512, size))
	{
		throw AIOException(NATIVE_ERROR_ALLOCATE_MEMORY, "Error on posix_memalign");
	}

	memset(preAllocBuffer, fillChar, size);


	if (::lseek (fileHandle, position, SEEK_SET) < 0) throw AIOException (11, "Error positioning the file");

	for (int i=0; i<blocks; i++)
	{
		if (::write(fileHandle, preAllocBuffer, size)<0)
		{
			throw AIOException (NATIVE_ERROR_PREALLOCATE_FILE, "Error pre allocating the file");
		}
	}

	if (::lseek (fileHandle, position, SEEK_SET) < 0) throw AIOException (NATIVE_ERROR_IO, "Error positioning the file");

	free (preAllocBuffer);
}


/** Write directly to the file without using libaio queue */
void AsyncFile::writeInternal(THREAD_CONTEXT, long position, size_t size, void *& buffer)
{
	if (::lseek (fileHandle, position, SEEK_SET) < 0) throw AIOException (11, "Error positioning the file");

	if (::write(fileHandle, buffer, size)<0)
	{
		throw AIOException (NATIVE_ERROR_IO, "Error writing file");
	}
	
	if (::fsync(fileHandle) < 0)
	{
		throw AIOException (NATIVE_ERROR_IO, "Error on synchronizing file");
	}
	

}


void AsyncFile::write(THREAD_CONTEXT threadContext, long position, size_t size, void *& buffer, CallbackAdapter *& adapter)
{

	struct iocb * iocb = new struct iocb();
	::io_prep_pwrite(iocb, fileHandle, buffer, size, position);
	iocb->data = (void *) adapter;

	int tries = 0;
	int result = 0;

	while ((result = ::io_submit(aioContext, 1, &iocb)) == (-EAGAIN))
	{
#ifdef DEBUG
		fprintf (stderr, "Retrying block as iocb was full (retry=%d)\n", tries);
#endif
		tries ++;
		if (tries > TRIES_BEFORE_WARN)
		{
#ifdef DEBUG
		    fprintf (stderr, "Warning level on retries, informing logger (retry=%d)\n", tries);
#endif
			controller->log(threadContext, 1, "You should consider expanding AIOLimit if this message appears too many times");
		}

		if (tries > TRIES_BEFORE_ERROR)
		{
#ifdef DEBUG
		    fprintf (stderr, "Error level on retries, throwing exception (retry=%d)\n", tries);
#endif
			throw AIOException(NATIVE_ERROR_AIO_FULL, "Too many retries (500) waiting for a valid iocb block, please increase MAX_IO limit");
		}
		::usleep(WAIT_FOR_SPOT);
	}

	if (result<0)
	{
		std::stringstream str;
		str<< "Problem on submit block, errorCode=" << result;
		throw AIOException (NATIVE_ERROR_IO, str.str());
	}
}

void AsyncFile::read(THREAD_CONTEXT threadContext, long position, size_t size, void *& buffer, CallbackAdapter *& adapter)
{

	struct iocb * iocb = new struct iocb();
	::io_prep_pread(iocb, fileHandle, buffer, size, position);
	iocb->data = (void *) adapter;

	int tries = 0;
	int result = 0;

	while ((result = ::io_submit(aioContext, 1, &iocb)) == (-EAGAIN))
	{
#ifdef DEBUG
		fprintf (stderr, "Retrying block as iocb was full (retry=%d)\n", tries);
#endif
		tries ++;
		if (tries > TRIES_BEFORE_WARN)
		{
#ifdef DEBUG
		    fprintf (stderr, "Warning level on retries, informing logger (retry=%d)\n", tries);
#endif
			controller->log(threadContext, 1, "You should consider expanding AIOLimit if this message appears too many times");
		}

		if (tries > TRIES_BEFORE_ERROR)
		{
#ifdef DEBUG
		    fprintf (stderr, "Error level on retries, throwing exception (retry=%d)\n", tries);
#endif
			throw AIOException(NATIVE_ERROR_AIO_FULL, "Too many retries (500) waiting for a valid iocb block, please increase MAX_IO limit");
		}
		::usleep(WAIT_FOR_SPOT);
	}

	if (result<0)
	{
		std::stringstream str;
		str<< "Problem on submit block, errorCode=" << result;
		throw AIOException (NATIVE_ERROR_IO, str.str());
	}
}

long AsyncFile::getSize()
{
	struct stat statBuffer;

	if (fstat(fileHandle, &statBuffer) < 0)
	{
		return -1l;
	}
	return statBuffer.st_size;
}


void AsyncFile::stopPoller(THREAD_CONTEXT threadContext)
{
	pollerRunning = 0;


	struct iocb * iocb = new struct iocb();
	::io_prep_pwrite(iocb, fileHandle, 0, 0, 0);
	iocb->data = (void *) -1;

	int result = 0;

	while ((result = ::io_submit(aioContext, 1, &iocb)) == (-EAGAIN))
	{
		fprintf(stderr, "Couldn't send request to stop poller, trying again");
		controller->log(threadContext, 1, "Couldn't send request to stop poller, trying again");
		::usleep(WAIT_FOR_SPOT);
	}

	// Waiting the Poller to finish (by giving up the lock)
	LockClass lock(&pollerMutex);
}

