/*
 * Copyright 2005-2014 Red Hat, Inc.
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
package org.apache.activemq6.api.core;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Defines all {@link HornetQException} types and their codes.
 */
public enum HornetQExceptionType
{

   // Error codes -------------------------------------------------

   INTERNAL_ERROR(000)
   {
      @Override
      public HornetQException createException(String msg)
      {
         return new HornetQInternalErrorException(msg);
      }
   },
   UNSUPPORTED_PACKET(001)
   {
      @Override
      public HornetQException createException(String msg)
      {
         return new HornetQUnsupportedPacketException(msg);
      }
   },
   NOT_CONNECTED(002)
   {
      @Override
      public HornetQException createException(String msg)
      {
         return new HornetQNotConnectedException(msg);
      }
   },
   CONNECTION_TIMEDOUT(003)
   {
      @Override
      public HornetQException createException(String msg)
      {
         return new HornetQConnectionTimedOutException(msg);
      }
   },
   DISCONNECTED(004)
   {
      @Override
      public HornetQException createException(String msg)
      {
         return new HornetQDisconnectedException(msg);
      }
   },
   UNBLOCKED(005)
   {
      @Override
      public HornetQException createException(String msg)
      {
         return new HornetQUnBlockedException(msg);
      }
   },
   IO_ERROR(006)
   {
      @Override
      public HornetQException createException(String msg)
      {
         return new HornetQIOErrorException(msg);
      }
   },
   QUEUE_DOES_NOT_EXIST(100)
   {
      @Override
      public HornetQException createException(String msg)
      {
         return new HornetQNonExistentQueueException(msg);
      }
   },
   QUEUE_EXISTS(101)
   {
      @Override
      public HornetQException createException(String msg)
      {
         return new HornetQQueueExistsException(msg);
      }
   },
   OBJECT_CLOSED(102)
   {
      @Override
      public HornetQException createException(String msg)
      {
         return new HornetQObjectClosedException(msg);
      }
   },
   INVALID_FILTER_EXPRESSION(103)
   {
      @Override
      public HornetQException createException(String msg)
      {
         return new HornetQInvalidFilterExpressionException(msg);
      }
   },
   ILLEGAL_STATE(104)
   {
      @Override
      public HornetQException createException(String msg)
      {
         return new HornetQIllegalStateException(msg);
      }
   },
   SECURITY_EXCEPTION(105)
   {
      @Override
      public HornetQException createException(String msg)
      {
         return new HornetQSecurityException(msg);
      }
   },
   ADDRESS_EXISTS(107)
   {
      @Override
      public HornetQException createException(String msg)
      {
         return new HornetQAddressExistsException(msg);
      }
   },
   INCOMPATIBLE_CLIENT_SERVER_VERSIONS(108)
   {
      @Override
      public HornetQException createException(String msg)
      {
         return new HornetQIncompatibleClientServerException(msg);
      }
   },
   LARGE_MESSAGE_ERROR_BODY(110)
   {
      @Override
      public HornetQException createException(String msg)
      {
         return new HornetQLargeMessageException(msg);
      }
   },
   TRANSACTION_ROLLED_BACK(111)
   {
      @Override
      public HornetQException createException(String msg)
      {
         return new HornetQTransactionRolledBackException(msg);
      }
   },
   SESSION_CREATION_REJECTED(112)
   {
      @Override
      public HornetQException createException(String msg)
      {
         return new HornetQSessionCreationException(msg);
      }
   },
   DUPLICATE_ID_REJECTED(113)
   {
      @Override
      public HornetQException createException(String msg)
      {
         return new HornetQDuplicateIdException(msg);
      }
   },
   DUPLICATE_METADATA(114)
   {
      @Override
      public HornetQException createException(String msg)
      {
         return new HornetQDuplicateMetaDataException(msg);
      }
   },
   TRANSACTION_OUTCOME_UNKNOWN(115)
   {
      @Override
      public HornetQException createException(String msg)
      {
         return new HornetQTransactionOutcomeUnknownException(msg);
      }
   },
   ALREADY_REPLICATING(116)
   {
      @Override
      public HornetQException createException(String msg)
      {
         return new HornetQAlreadyReplicatingException(msg);
      }
   },
   INTERCEPTOR_REJECTED_PACKET(117)
   {
      @Override
      public HornetQException createException(String msg)
      {
         return new HornetQInterceptorRejectedPacketException(msg);
      }
   },
   INVALID_TRANSIENT_QUEUE_USE(118)
   {
      @Override
      public HornetQException createException(String msg)
      {
         return new HornetQInvalidTransientQueueUseException(msg);
      }
   },

   GENERIC_EXCEPTION(999),
   NATIVE_ERROR_INTERNAL(200),
   NATIVE_ERROR_INVALID_BUFFER(201),
   NATIVE_ERROR_NOT_ALIGNED(202),
   NATIVE_ERROR_CANT_INITIALIZE_AIO(203),
   NATIVE_ERROR_CANT_RELEASE_AIO(204),
   NATIVE_ERROR_CANT_OPEN_CLOSE_FILE(205),
   NATIVE_ERROR_CANT_ALLOCATE_QUEUE(206),
   NATIVE_ERROR_PREALLOCATE_FILE(208),
   NATIVE_ERROR_ALLOCATE_MEMORY(209),
   ADDRESS_FULL(210)
   {
      @Override
      public HornetQException createException(String msg)
      {
         return new HornetQAddressFullException(msg);
      }
   },
   LARGE_MESSAGE_INTERRUPTED(211)
   {
      @Override
      public HornetQException createException(String msg)
      {
         return new HornetQLargeMessageInterruptedException(msg);
      }
   },
   CLUSTER_SECURITY_EXCEPTION(212)
   {
      @Override
      public HornetQException createException(String msg)
      {
         return new HornetQClusterSecurityException(msg);
      }

   };

   private static final Map<Integer, HornetQExceptionType> TYPE_MAP;

   static
   {
      HashMap<Integer, HornetQExceptionType> map = new HashMap<Integer, HornetQExceptionType>();
      for (HornetQExceptionType type : EnumSet.allOf(HornetQExceptionType.class))
      {
         map.put(type.getCode(), type);
      }
      TYPE_MAP = Collections.unmodifiableMap(map);
   }

   private final int code;

   HornetQExceptionType(int code)
   {
      this.code = code;
   }

   public int getCode()
   {
      return code;
   }

   public HornetQException createException(String msg)
   {
      return new HornetQException(msg + ", code:" + this);
   }

   public static HornetQException createException(int code, String msg)
   {
      return getType(code).createException(msg);
   }

   public static HornetQExceptionType getType(int code)
   {
      HornetQExceptionType type = TYPE_MAP.get(code);
      if (type != null)
         return type;
      return HornetQExceptionType.GENERIC_EXCEPTION;
   }
}