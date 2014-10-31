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
/**
 *
 */
package org.hornetq.core.replication;

/**
 * {@link LargeServerMessage} methods used by the {@link ReplicationEndpoint}.
 * <p/>
 * In practice a subset of the methods necessary to have a {@link LargeServerMessage}
 *
 * @see LargeServerMessageInSync
 */
public interface ReplicatedLargeMessage
{
   /**
    * @see LargeServerMessage#setDurable(boolean)
    */
   void setDurable(boolean b);

   /**
    * @see LargeServerMessage#setMessageID(long)
    */
   void setMessageID(long id);

   /**
    * @see LargeServerMessage#releaseResources()
    */
   void releaseResources();

   /**
    * @see LargeServerMessage#deleteFile()
    */
   void deleteFile() throws Exception;

   /**
    * @see LargeServerMessage#addBytes(byte[])
    */
   void addBytes(byte[] body) throws Exception;

}
