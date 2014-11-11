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



#ifndef AIOEXCEPTION_H_
#define AIOEXCEPTION_H_

#include <exception>
#include <string>


#define NATIVE_ERROR_INTERNAL 200
#define NATIVE_ERROR_INVALID_BUFFER 201
#define NATIVE_ERROR_NOT_ALIGNED 202
#define NATIVE_ERROR_CANT_INITIALIZE_AIO 203
#define NATIVE_ERROR_CANT_RELEASE_AIO 204
#define NATIVE_ERROR_CANT_OPEN_CLOSE_FILE 205
#define NATIVE_ERROR_CANT_ALLOCATE_QUEUE 206
#define NATIVE_ERROR_PREALLOCATE_FILE 208
#define NATIVE_ERROR_ALLOCATE_MEMORY 209
#define NATIVE_ERROR_IO 006
#define NATIVE_ERROR_AIO_FULL 211


class AIOException : public std::exception
{
private:
	int errorCode;
	std::string message;
public:
	AIOException(int _errorCode, std::string  _message) throw() : errorCode(_errorCode), message(_message)
	{
		errorCode = _errorCode;
		message = _message;
	}
	
	AIOException(int _errorCode, const char * _message) throw ()
	{
		message = std::string(_message);
		errorCode = _errorCode;
	}
	
	virtual ~AIOException() throw()
	{
		
	}
	
	int inline getErrorCode()
	{
		return errorCode;
	}
	
    const char* what() const throw()
    {
    	return message.data();
    }
	
};

#endif /*AIOEXCEPTION_H_*/
