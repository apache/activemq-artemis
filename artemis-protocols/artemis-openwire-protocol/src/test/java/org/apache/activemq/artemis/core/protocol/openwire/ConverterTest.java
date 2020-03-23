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
package org.apache.activemq.artemis.core.protocol.openwire;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.CommandTypes;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.util.ByteArrayOutputStream;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.MarshallingSupport;
import org.apache.activemq.wireformat.WireFormat;
import org.junit.Before;

public abstract class ConverterTest {

   protected WireFormat format;
   protected MessageId fooMessageId;
   protected ProducerId foorProducerId;
   protected FooDestination fooDestination;

   @Before
   public void before() {
      format = new OpenWireFormat();
      fooMessageId = new MessageId("ID:fvaleri-mac-54067-1584812479127-1:1:1:1:1");
      foorProducerId = new ProducerId("ID:fvaleri-mac-54067-1584812479127-1:1:1:1");
      fooDestination = new FooDestination();
   }

   protected ByteSequence encodeOpenWireContent(String content) throws IOException {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(baos);
      MarshallingSupport.writeUTF8(dos, content);
      dos.close();
      return baos.toByteSequence();
   }

   class FooDestination extends ActiveMQDestination {
      @Override
      public String getPhysicalName() {
         return "foo";
      }

      @Override
      public byte getDataStructureType() {
         return CommandTypes.ACTIVEMQ_QUEUE;
      }

      @Override
      protected String getQualifiedPrefix() {
         return "queue://";
      }

      @Override
      public byte getDestinationType() {
         return QUEUE_TYPE;
      }
   }

}
