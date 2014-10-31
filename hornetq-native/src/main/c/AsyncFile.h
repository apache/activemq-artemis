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

#ifndef FILEOUTPUT_H_
#define FILEOUTPUT_H_

#include <string>
#include <libaio.h>
#include <stdlib.h>
#include <pthread.h>
#include "JAIODatatypes.h"
#include "AIOException.h"

class AIOController;

class CallbackAdapter;

/** Author: Clebert Suconic at Redhat dot com*/
class AsyncFile
{
private:
	io_context_t aioContext;
	struct io_event *events; 
	int fileHandle;
	std::string fileName;
	
	pthread_mutex_t fileMutex;
	pthread_mutex_t pollerMutex;
	
	AIOController * controller;
	
	bool pollerRunning;
	
	int maxIO;
	
public:
	AsyncFile(std::string & _fileName, AIOController * controller, int maxIO);
	virtual ~AsyncFile();
	
	void write(THREAD_CONTEXT threadContext, long position, size_t size, void *& buffer, CallbackAdapter *& adapter);
	
	/** Write directly to the file without using libaio queue */
	void writeInternal(THREAD_CONTEXT threadContext, long position, size_t size, void *& buffer);
	
	void read(THREAD_CONTEXT threadContext, long position, size_t size, void *& buffer, CallbackAdapter *& adapter);
	
	int getHandle()
	{
		return fileHandle;
	}

	long getSize();

	inline void * newBuffer(int size)
	{
		void * buffer = 0;
		if (::posix_memalign(&buffer, 512, size))
		{
			throw AIOException(NATIVE_ERROR_ALLOCATE_MEMORY, "Error on posix_memalign");
		}
		return buffer;
		
	}

	inline void destroyBuffer(void * buffer)
	{
		::free(buffer);
	}

	
	// Finishes the polling thread (if any) and return
	void stopPoller(THREAD_CONTEXT threadContext);
	void preAllocate(THREAD_CONTEXT threadContext, off_t position, int blocks, size_t size, int fillChar);
	
	void pollEvents(THREAD_CONTEXT threadContext);
	
};

#endif /*FILEOUTPUT_H_*/
