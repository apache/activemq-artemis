# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


CMAKE_MINIMUM_REQUIRED(VERSION 2.6)

PROJECT(artemis-native)
SET(${PROJECT_NAME}_MAJOR_VERSION 1)
SET(${PROJECT_NAME}_MINOR_VERSION 0)
SET(${PROJECT_NAME}_PATCH_LEVEL 0)

FIND_PACKAGE(Java)
FIND_PACKAGE(JNI)
if (JNI_FOUND)
    message (STATUS "JNI_INCLUDE_DIRS=${JNI_INCLUDE_DIRS}")
    message (STATUS "JNI_LIBRARIES=${JNI_LIBRARIES}")
endif()

# You may want to adjust this next line for debugging. The -O3 is removed by default, since it would make debugging
# harder. Nonetheless, it can still be added by passing CMAKE_USER_C_FLAGS
# Also note that define the C99 as the minimum supported standard so the code can be compiled with older GCC versions
# (circa 4.4)
set(CMAKE_C_FLAGS_DEBUG "-Wall -std=c99 -z execstack -fdump-tree-all -Wall -pg -g ${CMAKE_USER_C_FLAGS}")
set(CMAKE_C_FLAGS "-O3 -std=c99 -Wall ${CMAKE_USER_C_FLAGS}")


set(ARTEMIS_LIB_NAME artemis-native-64)
if (CMAKE_SIZEOF_VOID_P EQUAL 4)
    set(ARTEMIS_LIB_NAME artemis-native-32)
endif()

set(ARTEMIS_CROSS_COMPILE OFF CACHE BOOL "Cross-compile the native library")

if (ARTEMIS_CROSS_COMPILE)
    if (CMAKE_SIZEOF_VOID_P EQUAL 4)
        message(FATAL_ERROR "Cannot cross-compile to 32-bit architecture in a 32-bit architecture")
    endif()

    message(STATUS "Using cross-compilation")
    set(ARTEMIS_CROSS_COMPILE_ROOT_PATH /usr/lib)
    set(ARTEMIS_LIB_NAME artemis-native-32)

    # The Cmake variable CMAKE_FIND_ROOT_PATH cannot be set via CLI, so we have to use a separate variable and then
    # set it to that value. We use ARTEMIS_CROSS_COMPILE_ROOT_PATH for that.
    set(CMAKE_FIND_ROOT_PATH ${ARTEMIS_CROSS_COMPILE_ROOT_PATH})
endif()

find_library(LIBAIO_LIB NAMES aio)
message(STATUS "Using the following libaio library for linking: ${LIBAIO_LIB}")

INCLUDE_DIRECTORIES(. ${JNI_INCLUDE_DIRS})

ADD_CUSTOM_COMMAND(
    OUTPUT org_apache_activemq_artemis_jlibaio_LibaioContext.h
    COMMAND javah -cp ../java/ org.apache.activemq.artemis.jlibaio.LibaioContext
    DEPENDS ../java/org/apache/activemq/artemis/jlibaio/LibaioContext.java
)

ADD_LIBRARY(artemis-native SHARED org_apache_activemq_artemis_jlibaio_LibaioContext.c org_apache_activemq_artemis_jlibaio_LibaioContext.h exception_helper.h)

target_link_libraries(artemis-native ${LIBAIO_LIB})

set_target_properties(artemis-native PROPERTIES
              LIBRARY_OUTPUT_DIRECTORY ../../../bin
              LIBRARY_OUTPUT_NAME ${ARTEMIS_LIB_NAME})
message(STATUS "Setting up library as ${ARTEMIS_LIB_NAME} based on current architecture")
