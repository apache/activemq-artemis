/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.protocol.amqp.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Date;

import org.apache.activemq.artemis.protocol.amqp.broker.AMQPMessage;
import org.apache.activemq.artemis.protocol.amqp.util.NettyWritable;
import org.apache.commons.collections.map.HashedMap;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.Header;
import org.apache.qpid.proton.amqp.messaging.Properties;
import org.apache.qpid.proton.message.Message;
import org.apache.qpid.proton.message.impl.MessageImpl;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class AMQPMessageTest {

   @Test
   public void testVerySimple() {
      MessageImpl protonMessage = (MessageImpl) Message.Factory.create();
      protonMessage.setHeader( new Header());
      Properties properties = new Properties();
      properties.setTo("someNiceLocal");
      protonMessage.setProperties(properties);
      protonMessage.getHeader().setDeliveryCount(new UnsignedInteger(7));
      protonMessage.getHeader().setDurable(Boolean.TRUE);
      protonMessage.setApplicationProperties(new ApplicationProperties(new HashedMap()));

      AMQPMessage decoded = encodeAndDecodeMessage(protonMessage);

      assertEquals(7, decoded.getHeader().getDeliveryCount().intValue());
      assertEquals(true, decoded.getHeader().getDurable());
      assertEquals("someNiceLocal", decoded.getAddress());
   }

   @Test
   public void testGetAddressFromMessage() {
      final String ADDRESS = "myQueue";

      MessageImpl protonMessage = (MessageImpl) Message.Factory.create();
      protonMessage.setHeader(new Header());
      protonMessage.setAddress(ADDRESS);

      AMQPMessage decoded = encodeAndDecodeMessage(protonMessage);

      assertEquals(ADDRESS, decoded.getAddress());
   }

   @Test
   public void testGetAddressSimpleStringFromMessage() {
      final String ADDRESS = "myQueue";

      MessageImpl protonMessage = (MessageImpl) Message.Factory.create();
      protonMessage.setHeader(new Header());
      protonMessage.setAddress(ADDRESS);

      AMQPMessage decoded = encodeAndDecodeMessage(protonMessage);

      assertEquals(ADDRESS, decoded.getAddressSimpleString().toString());
   }

   @Test
   public void testGetAddressFromMessageWithNoValueSet() {
      MessageImpl protonMessage = (MessageImpl) Message.Factory.create();

      AMQPMessage decoded = encodeAndDecodeMessage(protonMessage);

      assertNull(decoded.getAddress());
      assertNull(decoded.getAddressSimpleString());
   }

   @Test
   public void testIsDurableFromMessage() {
      MessageImpl protonMessage = (MessageImpl) Message.Factory.create();
      protonMessage.setHeader(new Header());
      protonMessage.setDurable(true);

      AMQPMessage decoded = encodeAndDecodeMessage(protonMessage);

      assertTrue(decoded.isDurable());
   }

   @Test
   public void testIsDurableFromMessageWithNoValueSet() {
      MessageImpl protonMessage = (MessageImpl) Message.Factory.create();

      AMQPMessage decoded = encodeAndDecodeMessage(protonMessage);

      assertFalse(decoded.isDurable());
   }

   @Test
   public void testGetGroupIDFromMessage() {
      final String GROUP_ID = "group-1";

      MessageImpl protonMessage = (MessageImpl) Message.Factory.create();
      protonMessage.setHeader(new Header());
      protonMessage.setGroupId(GROUP_ID);

      AMQPMessage decoded = encodeAndDecodeMessage(protonMessage);

      assertEquals(GROUP_ID, decoded.getGroupID().toString());
   }

   @Test
   public void testGetGroupIDFromMessageWithNoGroupId() {
      MessageImpl protonMessage = (MessageImpl) Message.Factory.create();

      AMQPMessage decoded = encodeAndDecodeMessage(protonMessage);

      assertNull(decoded.getUserID());
   }

   @Test
   public void testGetUserIDFromMessage() {
      final String USER_NAME = "foo";

      MessageImpl protonMessage = (MessageImpl) Message.Factory.create();
      protonMessage.setHeader(new Header());
      protonMessage.setUserId(USER_NAME.getBytes(StandardCharsets.UTF_8));

      AMQPMessage decoded = encodeAndDecodeMessage(protonMessage);

      assertEquals(USER_NAME, decoded.getAMQPUserID());
   }

   @Test
   public void testGetUserIDFromMessageWithNoUserID() {
      MessageImpl protonMessage = (MessageImpl) Message.Factory.create();

      AMQPMessage decoded = encodeAndDecodeMessage(protonMessage);

      assertNull(decoded.getUserID());
   }

   @Test
   public void testGetPriorityFromMessage() {
      final short PRIORITY = 7;

      MessageImpl protonMessage = (MessageImpl) Message.Factory.create();
      protonMessage.setHeader(new Header());
      protonMessage.setPriority(PRIORITY);

      AMQPMessage decoded = encodeAndDecodeMessage(protonMessage);

      assertEquals(PRIORITY, decoded.getPriority());
   }

   @Test
   public void testGetPriorityFromMessageWithNoPrioritySet() {
      MessageImpl protonMessage = (MessageImpl) Message.Factory.create();

      AMQPMessage decoded = encodeAndDecodeMessage(protonMessage);

      assertEquals(AMQPMessage.DEFAULT_MESSAGE_PRIORITY, decoded.getPriority());
   }

   @Test
   public void testGetTimestampFromMessage() {
      Date timestamp = new Date(System.currentTimeMillis());

      MessageImpl protonMessage = (MessageImpl) Message.Factory.create();
      protonMessage.setHeader( new Header());
      Properties properties = new Properties();
      properties.setCreationTime(timestamp);

      protonMessage.setProperties(properties);

      AMQPMessage decoded = encodeAndDecodeMessage(protonMessage);

      assertEquals(timestamp.getTime(), decoded.getTimestamp());
   }

   @Test
   public void testGetTimestampFromMessageWithNoCreateTimeSet() {
      MessageImpl protonMessage = (MessageImpl) Message.Factory.create();
      protonMessage.setHeader( new Header());

      AMQPMessage decoded = encodeAndDecodeMessage(protonMessage);

      assertEquals(0L, decoded.getTimestamp());
   }

   private AMQPMessage encodeAndDecodeMessage(MessageImpl message) {
      ByteBuf nettyBuffer = Unpooled.buffer(1500);

      message.encode(new NettyWritable(nettyBuffer));
      byte[] bytes = new byte[nettyBuffer.writerIndex()];
      nettyBuffer.readBytes(bytes);

      return new AMQPMessage(0, bytes);
   }
}
