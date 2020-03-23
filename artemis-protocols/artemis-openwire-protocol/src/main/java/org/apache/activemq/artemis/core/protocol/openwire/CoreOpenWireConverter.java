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

import static org.apache.activemq.artemis.core.protocol.openwire.OpenWireMessageSupport.isNullOrEmpty;
import static org.apache.activemq.artemis.core.protocol.openwire.OpenWireMessageSupport.getBinaryObjectProperty;

import static org.apache.activemq.artemis.core.protocol.openwire.OpenWireMessageSupport.JMS_TYPE_PROPERTY;
import static org.apache.activemq.artemis.core.protocol.openwire.OpenWireMessageSupport.AMQ_MSG_COMPRESSED;
import static org.apache.activemq.artemis.core.protocol.openwire.OpenWireMessageSupport.AMQ_MSG_ARRIVAL;
import static org.apache.activemq.artemis.core.protocol.openwire.OpenWireMessageSupport.AMQ_MSG_BROKER_IN_TIME;
import static org.apache.activemq.artemis.core.protocol.openwire.OpenWireMessageSupport.AMQ_MSG_COMMAND_ID;
import static org.apache.activemq.artemis.core.protocol.openwire.OpenWireMessageSupport.JMS_CORRELATION_ID_PROPERTY;
import static org.apache.activemq.artemis.core.protocol.openwire.OpenWireMessageSupport.AMQ_MSG_DATASTRUCTURE;
import static org.apache.activemq.artemis.core.protocol.openwire.OpenWireMessageSupport.AMQ_MSG_MESSAGE_ID;
import static org.apache.activemq.artemis.core.protocol.openwire.OpenWireMessageSupport.AMQ_MSG_PRODUCER_ID;
import static org.apache.activemq.artemis.core.protocol.openwire.OpenWireMessageSupport.AMQ_MSG_USER_ID;
import static org.apache.activemq.artemis.core.protocol.openwire.OpenWireMessageSupport.AMQ_MSG_DROPPABLE;
import static org.apache.activemq.artemis.core.protocol.openwire.OpenWireMessageSupport.AMQ_MSG_ORIG_DESTINATION;
import static org.apache.activemq.artemis.core.protocol.openwire.OpenWireMessageSupport.AMQ_MSG_BROKER_PATH;
import static org.apache.activemq.artemis.core.protocol.openwire.OpenWireMessageSupport.AMQ_MSG_CLUSTER;
import static org.apache.activemq.artemis.core.protocol.openwire.OpenWireMessageSupport.AMQ_MSG_MARSHALL_PROP;
import static org.apache.activemq.artemis.core.protocol.openwire.OpenWireMessageSupport.AMQ_MSG_ORIG_TXID;
import static org.apache.activemq.artemis.core.protocol.openwire.OpenWireMessageSupport.AMQ_MSG_REPLY_TO;
import static org.apache.activemq.artemis.core.protocol.openwire.OpenWireMessageSupport.AMQ_MSG_DLQ_DELIVERY_FAILURE_CAUSE_PROPERTY;

import static org.apache.activemq.artemis.api.core.Message.BYTES_TYPE;
import static org.apache.activemq.artemis.api.core.Message.MAP_TYPE;
import static org.apache.activemq.artemis.api.core.Message.OBJECT_TYPE;
import static org.apache.activemq.artemis.api.core.Message.STREAM_TYPE;
import static org.apache.activemq.artemis.api.core.Message.TEXT_TYPE;
import static org.apache.activemq.artemis.api.core.Message.DEFAULT_TYPE;

import javax.jms.JMSException;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.management.ManagementHelper;
import org.apache.activemq.artemis.core.protocol.openwire.amq.AMQConsumer;
import org.apache.activemq.artemis.core.protocol.openwire.util.OpenWireUtil;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.reader.MessageUtil;
import org.apache.activemq.artemis.utils.DataConstants;
import org.apache.activemq.artemis.utils.UUIDGenerator;
import org.apache.activemq.artemis.utils.collections.TypedProperties;
import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMapMessage;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQObjectMessage;
import org.apache.activemq.command.ActiveMQStreamMessage;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.BrokerId;
import org.apache.activemq.command.DataStructure;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.ByteSequenceData;
import org.apache.activemq.util.MarshallingSupport;
import org.apache.activemq.wireformat.WireFormat;
import org.jboss.logging.Logger;

public class CoreOpenWireConverter {

   private static final Logger LOG = Logger.getLogger(CoreOpenWireConverter.class);

   public static MessageDispatch createMessageDispatch(MessageReference reference,
                                                       ICoreMessage coreMessage,
                                                       WireFormat format,
                                                       AMQConsumer consumer) throws Exception {
      ActiveMQMessage amqMessage = toAMQMessage(reference, coreMessage, format, consumer);

      //we can use core message id for sequenceId
      amqMessage.getMessageId().setBrokerSequenceId(coreMessage.getMessageID());
      MessageDispatch md = new MessageDispatch();
      md.setConsumerId(consumer.getId());
      md.setRedeliveryCounter(reference.getDeliveryCount() - 1);
      md.setDeliverySequenceId(amqMessage.getMessageId().getBrokerSequenceId());
      md.setMessage(amqMessage);
      ActiveMQDestination destination = amqMessage.getDestination();
      md.setDestination(destination);

      return md;
   }

   private static final class EagerActiveMQBytesMessage extends ActiveMQBytesMessage {
      EagerActiveMQBytesMessage(int size) {
         this.bytesOut = new org.apache.activemq.util.ByteArrayOutputStream(size);
         OutputStream os = bytesOut;
         this.dataOut = new DataOutputStream(os);
      }
   }

   private static ActiveMQMessage toAMQMessage(MessageReference reference,
                                               ICoreMessage coreMessage,
                                               WireFormat format,
                                               AMQConsumer consumer) throws IOException {
      if (LOG.isTraceEnabled()) {
         LOG.trace("Converting message from Core to OpenWire");
      }

      final ActiveMQMessage amqMessage;
      final byte coreType = coreMessage.getType();
      final Boolean compressProp = (Boolean) coreMessage.getObjectProperty(AMQ_MSG_COMPRESSED);
      final boolean isCompressed = compressProp == null ? false : compressProp.booleanValue();
      final byte[] bytes;
      final ActiveMQBuffer buffer = coreMessage.getDataBuffer();
      buffer.resetReaderIndex();

      switch (coreType) {
         case BYTES_TYPE:
            amqMessage = new EagerActiveMQBytesMessage(0);
            bytes = toAMQMessageBytesType(buffer, isCompressed);
            break;
         case MAP_TYPE:
            amqMessage = new ActiveMQMapMessage();
            bytes = toAMQMessageMapType(buffer, isCompressed);
            break;
         case OBJECT_TYPE:
            amqMessage = new ActiveMQObjectMessage();
            bytes = toAMQMessageObjectType(buffer, isCompressed);
            break;
         case STREAM_TYPE:
            amqMessage = new ActiveMQStreamMessage();
            bytes = toAMQMessageStreamType(buffer, isCompressed);
            break;
         case TEXT_TYPE:
            amqMessage = new ActiveMQTextMessage();
            bytes = toAMQMessageTextType(buffer, isCompressed);
            break;
         case DEFAULT_TYPE:
            amqMessage = new ActiveMQMessage();
            bytes = toAMQMessageDefaultType(buffer, isCompressed);
            break;
         default:
            throw new IllegalStateException("Unknown message type: " + coreMessage.getType());
      }

      if (bytes != null) {
         ByteSequence content = new ByteSequence(bytes);
         amqMessage.setContent(content);
      }

      final String type = coreMessage.getStringProperty(JMS_TYPE_PROPERTY);
      if (type != null) {
         amqMessage.setJMSType(type);
      }
      amqMessage.setPersistent(coreMessage.isDurable());
      amqMessage.setExpiration(coreMessage.getExpiration());
      amqMessage.setPriority(coreMessage.getPriority());
      amqMessage.setTimestamp(coreMessage.getTimestamp());

      Long brokerInTime = (Long) coreMessage.getObjectProperty(AMQ_MSG_BROKER_IN_TIME);
      if (brokerInTime == null) {
         brokerInTime = 0L;
      }
      amqMessage.setBrokerInTime(brokerInTime);

      amqMessage.setCompressed(isCompressed);

      //we need check null because messages may come from other clients
      //and those amq specific attribute may not be set.
      Long arrival = (Long) coreMessage.getObjectProperty(AMQ_MSG_ARRIVAL);
      if (arrival == null) {
         //messages from other sources (like core client) may not set this prop
         arrival = 0L;
      }
      amqMessage.setArrival(arrival);

      final String brokerPath = coreMessage.getStringProperty(AMQ_MSG_BROKER_PATH);
      if (!isNullOrEmpty(brokerPath)) {
         amqMessage.setBrokerPath(getBrokerIdsArray(brokerPath));
      }

      final String clusterPath = coreMessage.getStringProperty(AMQ_MSG_CLUSTER);
      if (!isNullOrEmpty(clusterPath)) {
         amqMessage.setCluster(getBrokerIdsArray(clusterPath));
      }

      Integer commandId = (Integer) coreMessage.getObjectProperty(AMQ_MSG_COMMAND_ID);
      if (commandId == null) {
         commandId = -1;
      }
      amqMessage.setCommandId(commandId);

      final String corrId = coreMessage.getStringProperty(JMS_CORRELATION_ID_PROPERTY);
      if (!isNullOrEmpty(corrId)) {
         amqMessage.setCorrelationId(corrId);
      }

      final Object dataStruct = getBinaryObjectProperty(format, coreMessage, AMQ_MSG_DATASTRUCTURE);
      if (dataStruct != null) {
         amqMessage.setDataStructure((DataStructure) dataStruct);
      }

      final ActiveMQDestination actualDestination = consumer.getOpenwireDestination();
      amqMessage.setDestination(OpenWireUtil.toAMQAddress(coreMessage, actualDestination));

      final Object value = coreMessage.getGroupID();
      if (value != null) {
         String groupId = value.toString();
         amqMessage.setGroupID(groupId);
      }

      amqMessage.setGroupSequence(coreMessage.getGroupSequence());

      final MessageId mid;
      Object midObject = getBinaryObjectProperty(format, coreMessage, AMQ_MSG_MESSAGE_ID);
      if (midObject != null) {
         mid = (MessageId) midObject;
      } else {
         final SimpleString connectionId = (SimpleString) coreMessage.getObjectProperty(MessageUtil.CONNECTION_ID_PROPERTY_NAME);
         if (connectionId != null) {
            mid = new MessageId("ID:" + connectionId.toString() + ":-1:-1:-1", coreMessage.getMessageID());
         } else {
            //JMSMessageID should be started with "ID:"
            String midd = "ID:" + UUIDGenerator.getInstance().generateStringUUID() + ":-1:-1:-1:-1";
            mid = new MessageId(midd);
         }
      }
      amqMessage.setMessageId(mid);

      final Object oridDest = getBinaryObjectProperty(format, coreMessage, AMQ_MSG_ORIG_DESTINATION);
      if (oridDest != null) {
         amqMessage.setOriginalDestination((ActiveMQDestination) oridDest);
      }

      final Object origTxId = getBinaryObjectProperty(format, coreMessage, AMQ_MSG_ORIG_TXID);
      if (origTxId != null) {
         amqMessage.setOriginalTransactionId((TransactionId) origTxId);
      }

      final Object prodId = getBinaryObjectProperty(format, coreMessage, AMQ_MSG_PRODUCER_ID);
      if (prodId != null) {
         amqMessage.setProducerId((ProducerId) prodId);
      }

      final byte[] marshallProp = (byte[]) coreMessage.getObjectProperty(AMQ_MSG_MARSHALL_PROP);
      if (marshallProp != null) {
         amqMessage.setMarshalledProperties(new ByteSequence(marshallProp));
      }

      amqMessage.setRedeliveryCounter(reference.getDeliveryCount() - 1);

      final Object replyTo = getBinaryObjectProperty(format, coreMessage, AMQ_MSG_REPLY_TO);
      if (replyTo != null) {
         amqMessage.setReplyTo((ActiveMQDestination) replyTo);
      }

      final String userId = coreMessage.getStringProperty(AMQ_MSG_USER_ID);
      if (!isNullOrEmpty(userId)) {
         amqMessage.setUserID(userId);
      }

      final Boolean isDroppable = (Boolean) coreMessage.getObjectProperty(AMQ_MSG_DROPPABLE);
      if (isDroppable != null) {
         amqMessage.setDroppable(isDroppable);
      }

      final SimpleString dlqCause = (SimpleString) coreMessage.getObjectProperty(AMQ_MSG_DLQ_DELIVERY_FAILURE_CAUSE_PROPERTY);
      if (dlqCause != null) {
         setAMQMsgDlqDeliveryFailureCause(amqMessage, dlqCause);
      }

      final SimpleString lastValueProperty = coreMessage.getLastValueProperty();
      if (lastValueProperty != null) {
         setAMQMsgHdrLastValueName(amqMessage, lastValueProperty);
      }

      final Set<SimpleString> props = coreMessage.getPropertyNames();
      if (props != null) {
         setAMQMsgObjectProperties(amqMessage, coreMessage, props, consumer);
      }

      return amqMessage;
   }

   private static byte[] toAMQMessageTextType(ActiveMQBuffer buffer,
                                              boolean isCompressed) throws IOException {
      byte[] bytes = null;
      SimpleString text = buffer.readNullableSimpleString();
      if (text != null) {
         ByteArrayOutputStream bytesOut = new ByteArrayOutputStream(text.length() + 4);
         OutputStream out = bytesOut;
         if (isCompressed) {
            out = new DeflaterOutputStream(out, true);
         }
         try (DataOutputStream dataOut = new DataOutputStream(out)) {
            MarshallingSupport.writeUTF8(dataOut, text.toString());
            dataOut.flush();
            bytes = bytesOut.toByteArray();
         }
      }
      return bytes;
   }

   private static byte[] toAMQMessageMapType(ActiveMQBuffer buffer,
                                             boolean isCompressed) throws IOException {
      byte[] bytes = null;
      //it could be a null map
      if (buffer.readableBytes() > 0) {
         TypedProperties mapData = new TypedProperties();
         mapData.decode(buffer.byteBuf());
         Map<String, Object> map = mapData.getMap();
         ByteArrayOutputStream out = new ByteArrayOutputStream(mapData.getEncodeSize());
         OutputStream os = out;
         if (isCompressed) {
            os = new DeflaterOutputStream(os, true);
         }
         try (DataOutputStream dataOut = new DataOutputStream(os)) {
            MarshallingSupport.marshalPrimitiveMap(map, dataOut);
            dataOut.flush();
         }
         bytes = out.toByteArray();
      }
      return bytes;
   }

   private static byte[] toAMQMessageObjectType(ActiveMQBuffer buffer,
                                                boolean isCompressed) throws IOException {
      byte[] bytes = null;
      if (buffer.readableBytes() > 0) {
         int len = buffer.readInt();
         bytes = new byte[len];
         buffer.readBytes(bytes);
         if (isCompressed) {
            ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
            try (DeflaterOutputStream out = new DeflaterOutputStream(bytesOut, true)) {
               out.write(bytes);
               out.flush();
            }
            bytes = bytesOut.toByteArray();
         }
      }
      return bytes;
   }

   private static byte[] toAMQMessageStreamType(ActiveMQBuffer buffer,
                                                boolean isCompressed) throws IOException {
      byte[] bytes;
      org.apache.activemq.util.ByteArrayOutputStream bytesOut = new org.apache.activemq.util.ByteArrayOutputStream();
      OutputStream out = bytesOut;
      if (isCompressed) {
         out = new DeflaterOutputStream(bytesOut, true);
      }
      try (DataOutputStream dataOut = new DataOutputStream(out)) {

         boolean stop = false;
         while (!stop && buffer.readable()) {
            byte primitiveType = buffer.readByte();
            switch (primitiveType) {
               case DataConstants.BOOLEAN:
                  MarshallingSupport.marshalBoolean(dataOut, buffer.readBoolean());
                  break;
               case DataConstants.BYTE:
                  MarshallingSupport.marshalByte(dataOut, buffer.readByte());
                  break;
               case DataConstants.BYTES:
                  int len = buffer.readInt();
                  byte[] bytesData = new byte[len];
                  buffer.readBytes(bytesData);
                  MarshallingSupport.marshalByteArray(dataOut, bytesData);
                  break;
               case DataConstants.CHAR:
                  char ch = (char) buffer.readShort();
                  MarshallingSupport.marshalChar(dataOut, ch);
                  break;
               case DataConstants.DOUBLE:
                  double doubleVal = Double.longBitsToDouble(buffer.readLong());
                  MarshallingSupport.marshalDouble(dataOut, doubleVal);
                  break;
               case DataConstants.FLOAT:
                  float floatVal = Float.intBitsToFloat(buffer.readInt());
                  MarshallingSupport.marshalFloat(dataOut, floatVal);
                  break;
               case DataConstants.INT:
                  MarshallingSupport.marshalInt(dataOut, buffer.readInt());
                  break;
               case DataConstants.LONG:
                  MarshallingSupport.marshalLong(dataOut, buffer.readLong());
                  break;
               case DataConstants.SHORT:
                  MarshallingSupport.marshalShort(dataOut, buffer.readShort());
                  break;
               case DataConstants.STRING:
                  String string = buffer.readNullableString();
                  if (string == null) {
                     MarshallingSupport.marshalNull(dataOut);
                  } else {
                     MarshallingSupport.marshalString(dataOut, string);
                  }
                  break;
               default:
                  //now we stop
                  stop = true;
                  break;
            }
            dataOut.flush();
         }
      }
      bytes = bytesOut.toByteArray();
      return bytes;
   }

   private static byte[] toAMQMessageBytesType(ActiveMQBuffer buffer,
                                               boolean isCompressed) throws IOException {
      int n = buffer.readableBytes();
      byte[] bytes = new byte[n];
      buffer.readBytes(bytes);
      if (isCompressed) {
         bytes = toAMQMessageCompressedBytesType(bytes);
      }
      return bytes;
   }

   private static byte[] toAMQMessageCompressedBytesType(byte[] bytes) throws IOException {
      int length = bytes.length;
      Deflater deflater = new Deflater();
      try (org.apache.activemq.util.ByteArrayOutputStream compressed = new org.apache.activemq.util.ByteArrayOutputStream()) {
         compressed.write(new byte[4]);
         deflater.setInput(bytes);
         deflater.finish();
         byte[] bytesBuf = new byte[1024];
         while (!deflater.finished()) {
            int count = deflater.deflate(bytesBuf);
            compressed.write(bytesBuf, 0, count);
         }
         compressed.flush();
         ByteSequence byteSeq = compressed.toByteSequence();
         ByteSequenceData.writeIntBig(byteSeq, length);
         return Arrays.copyOfRange(byteSeq.data, 0, byteSeq.length);
      } finally {
         deflater.end();
      }
   }

   private static byte[] toAMQMessageDefaultType(ActiveMQBuffer buffer,
                                                 boolean isCompressed) throws IOException {
      int n = buffer.readableBytes();
      byte[] bytes = new byte[n];
      buffer.readBytes(bytes);
      if (isCompressed) {
         try (ByteArrayOutputStream bytesOut = new ByteArrayOutputStream(); DeflaterOutputStream out = new DeflaterOutputStream(bytesOut, true)) {
            out.write(bytes);
            out.flush();
            bytes = bytesOut.toByteArray();
         }
      }
      return bytes;
   }

   private static BrokerId[] getBrokerIdsArray(String path) {
      String[] ids = path.split(",");
      BrokerId[] bids = new BrokerId[ids.length];
      for (int i = 0; i < bids.length; i++) {
         bids[i] = new BrokerId(ids[i]);
      }
      return bids;
   }

   private static void setAMQMsgDlqDeliveryFailureCause(ActiveMQMessage amqMsg,
                                                        SimpleString dlqCause) throws IOException {
      try {
         amqMsg.setStringProperty(ActiveMQMessage.DLQ_DELIVERY_FAILURE_CAUSE_PROPERTY, dlqCause.toString());
      } catch (JMSException e) {
         throw new IOException("Failure to set DLQ property " + dlqCause, e);
      }
   }

   private static void setAMQMsgHdrLastValueName(ActiveMQMessage amqMsg,
                                                 SimpleString lastValueProperty) throws IOException {
      try {
         amqMsg.setStringProperty(org.apache.activemq.artemis.api.core.Message.HDR_LAST_VALUE_NAME.toString(), lastValueProperty.toString());
      } catch (JMSException e) {
         throw new IOException("Failure to set LVQ property " + lastValueProperty, e);
      }
   }

   private static void setAMQMsgObjectProperties(ActiveMQMessage amqMsg,
                                                 ICoreMessage coreMessage,
                                                 Set<SimpleString> props,
                                                 AMQConsumer consumer) throws IOException {
      for (SimpleString s : props) {
         final String keyStr = s.toString();
         if (!coreMessage.containsProperty(ManagementHelper.HDR_NOTIFICATION_TYPE) && (keyStr.startsWith("_AMQ") || keyStr.startsWith("__HDR_"))) {
            continue;
         }
         final Object prop = coreMessage.getObjectProperty(s);
         try {
            if (prop instanceof SimpleString) {
               amqMsg.setObjectProperty(keyStr, prop.toString());
            } else {
               if (keyStr.equals(MessageUtil.JMSXDELIVERYCOUNT) && prop instanceof Long) {
                  Long l = (Long) prop;
                  amqMsg.setObjectProperty(keyStr, l.intValue());
               } else {
                  amqMsg.setObjectProperty(keyStr, prop);
               }
            }
         } catch (JMSException e) {
            throw new IOException("exception setting property " + s + " : " + prop, e);
         }
      }
   }

}
