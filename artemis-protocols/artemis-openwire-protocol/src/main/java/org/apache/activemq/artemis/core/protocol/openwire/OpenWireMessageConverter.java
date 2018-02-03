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

import javax.jms.JMSException;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;
import java.util.zip.InflaterOutputStream;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQPropertyConversionException;
import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.core.message.impl.CoreMessageObjectPools;
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
import org.apache.activemq.command.CommandTypes;
import org.apache.activemq.command.DataStructure;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.util.ByteArrayInputStream;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.ByteSequenceData;
import org.apache.activemq.util.MarshallingSupport;
import org.apache.activemq.wireformat.WireFormat;
import org.fusesource.hawtbuf.UTF8Buffer;

public final class OpenWireMessageConverter {

   private static final SimpleString JMS_TYPE_PROPERTY = new SimpleString("JMSType");
   private static final SimpleString JMS_CORRELATION_ID_PROPERTY = new SimpleString("JMSCorrelationID");
   private static final SimpleString AMQ_PREFIX = new SimpleString("__HDR_");
   public static final SimpleString AMQ_MSG_DLQ_DELIVERY_FAILURE_CAUSE_PROPERTY = new SimpleString(AMQ_PREFIX + "dlqDeliveryFailureCause");

   private static final SimpleString AMQ_MSG_ARRIVAL = new SimpleString(AMQ_PREFIX + "ARRIVAL");
   private static final SimpleString AMQ_MSG_BROKER_IN_TIME = new SimpleString(AMQ_PREFIX + "BROKER_IN_TIME");

   private static final SimpleString AMQ_MSG_BROKER_PATH = new SimpleString(AMQ_PREFIX + "BROKER_PATH");
   private static final SimpleString AMQ_MSG_CLUSTER = new SimpleString(AMQ_PREFIX + "CLUSTER");
   private static final SimpleString AMQ_MSG_COMMAND_ID = new SimpleString(AMQ_PREFIX + "COMMAND_ID");
   private static final SimpleString AMQ_MSG_DATASTRUCTURE = new SimpleString(AMQ_PREFIX + "DATASTRUCTURE");
   private static final SimpleString AMQ_MSG_GROUP_ID = org.apache.activemq.artemis.api.core.Message.HDR_GROUP_ID;
   private static final SimpleString AMQ_MSG_GROUP_SEQUENCE = new SimpleString(AMQ_PREFIX + "GROUP_SEQUENCE");
   private static final SimpleString AMQ_MSG_MESSAGE_ID = new SimpleString(AMQ_PREFIX + "MESSAGE_ID");
   private static final SimpleString AMQ_MSG_ORIG_DESTINATION =  new SimpleString(AMQ_PREFIX + "ORIG_DESTINATION");
   private static final SimpleString AMQ_MSG_ORIG_TXID = new SimpleString(AMQ_PREFIX + "ORIG_TXID");
   private static final SimpleString AMQ_MSG_PRODUCER_ID =  new SimpleString(AMQ_PREFIX + "PRODUCER_ID");
   private static final SimpleString AMQ_MSG_MARSHALL_PROP = new SimpleString(AMQ_PREFIX + "MARSHALL_PROP");
   private static final SimpleString AMQ_MSG_REPLY_TO = new SimpleString(AMQ_PREFIX + "REPLY_TO");

   private static final SimpleString AMQ_MSG_USER_ID = new SimpleString(AMQ_PREFIX + "USER_ID");

   private static final SimpleString AMQ_MSG_DROPPABLE =  new SimpleString(AMQ_PREFIX + "DROPPABLE");
   private static final SimpleString AMQ_MSG_COMPRESSED = new SimpleString(AMQ_PREFIX + "COMPRESSED");

   private OpenWireMessageConverter() {

   }

   public static org.apache.activemq.artemis.api.core.Message inbound(final Message messageSend,
                                                                      final WireFormat marshaller,
                                                                      final CoreMessageObjectPools coreMessageObjectPools) throws Exception {

      final CoreMessage coreMessage = new CoreMessage(-1, messageSend.getSize(), coreMessageObjectPools);

      final String type = messageSend.getType();
      if (type != null) {
         coreMessage.putStringProperty(JMS_TYPE_PROPERTY, new SimpleString(type));
      }
      coreMessage.setDurable(messageSend.isPersistent());
      coreMessage.setExpiration(messageSend.getExpiration());
      coreMessage.setPriority(messageSend.getPriority());
      coreMessage.setTimestamp(messageSend.getTimestamp());

      final byte coreType = toCoreType(messageSend.getDataStructureType());
      coreMessage.setType(coreType);

      final ActiveMQBuffer body = coreMessage.getBodyBuffer();

      final ByteSequence contents = messageSend.getContent();
      if (contents == null && coreType == org.apache.activemq.artemis.api.core.Message.TEXT_TYPE) {
         body.writeNullableString(null);
      } else if (contents != null) {
         final boolean messageCompressed = messageSend.isCompressed();
         if (messageCompressed) {
            coreMessage.putBooleanProperty(AMQ_MSG_COMPRESSED, messageCompressed);
         }

         switch (coreType) {
            case org.apache.activemq.artemis.api.core.Message.TEXT_TYPE:
               writeTextType(contents, messageCompressed, body);
               break;
            case org.apache.activemq.artemis.api.core.Message.MAP_TYPE:
               writeMapType(contents, messageCompressed, body);
               break;
            case org.apache.activemq.artemis.api.core.Message.OBJECT_TYPE:
               writeObjectType(contents, messageCompressed, body);
               break;
            case org.apache.activemq.artemis.api.core.Message.STREAM_TYPE:
               writeStreamType(contents, messageCompressed, body);
               break;
            case org.apache.activemq.artemis.api.core.Message.BYTES_TYPE:
               writeBytesType(contents, messageCompressed, body);
               break;
            default:
               writeDefaultType(contents, messageCompressed, body);
               break;
         }
      }
      //amq specific
      coreMessage.putLongProperty(AMQ_MSG_ARRIVAL, messageSend.getArrival());
      coreMessage.putLongProperty(AMQ_MSG_BROKER_IN_TIME, messageSend.getBrokerInTime());
      final BrokerId[] brokers = messageSend.getBrokerPath();
      if (brokers != null) {
         putMsgBrokerPath(brokers, coreMessage);
      }
      final BrokerId[] cluster = messageSend.getCluster();
      if (cluster != null) {
         putMsgCluster(cluster, coreMessage);
      }

      coreMessage.putIntProperty(AMQ_MSG_COMMAND_ID, messageSend.getCommandId());
      final String corrId = messageSend.getCorrelationId();
      if (corrId != null) {
         coreMessage.putStringProperty(JMS_CORRELATION_ID_PROPERTY, new SimpleString(corrId));
      }
      final DataStructure ds = messageSend.getDataStructure();
      if (ds != null) {
         putMsgDataStructure(ds, marshaller, coreMessage);
      }
      final String groupId = messageSend.getGroupID();
      if (groupId != null) {
         coreMessage.putStringProperty(AMQ_MSG_GROUP_ID, coreMessageObjectPools.getGroupIdStringSimpleStringPool().getOrCreate(groupId));
      }
      coreMessage.putIntProperty(AMQ_MSG_GROUP_SEQUENCE, messageSend.getGroupSequence());

      final MessageId messageId = messageSend.getMessageId();

      final ByteSequence midBytes = marshaller.marshal(messageId);
      midBytes.compact();
      coreMessage.putBytesProperty(AMQ_MSG_MESSAGE_ID, midBytes.data);

      final ProducerId producerId = messageSend.getProducerId();
      if (producerId != null) {
         final ByteSequence producerIdBytes = marshaller.marshal(producerId);
         producerIdBytes.compact();
         coreMessage.putBytesProperty(AMQ_MSG_PRODUCER_ID, producerIdBytes.data);
      }
      final ByteSequence propBytes = messageSend.getMarshalledProperties();
      if (propBytes != null) {
         putMsgMarshalledProperties(propBytes, messageSend, coreMessage);
      }

      final ActiveMQDestination replyTo = messageSend.getReplyTo();
      if (replyTo != null) {
         putMsgReplyTo(replyTo, marshaller, coreMessage);
      }

      final String userId = messageSend.getUserID();
      if (userId != null) {
         coreMessage.putStringProperty(AMQ_MSG_USER_ID, new SimpleString(userId));
      }
      coreMessage.putBooleanProperty(AMQ_MSG_DROPPABLE, messageSend.isDroppable());

      final ActiveMQDestination origDest = messageSend.getOriginalDestination();
      if (origDest != null) {
         putMsgOriginalDestination(origDest, marshaller, coreMessage);
      }

      return coreMessage;
   }

   private static void writeTextType(final ByteSequence contents,
                                     final boolean messageCompressed,
                                     final ActiveMQBuffer body) throws IOException {
      InputStream tis = new ByteArrayInputStream(contents);
      if (messageCompressed) {
         tis = new InflaterInputStream(tis);
      }
      DataInputStream tdataIn = new DataInputStream(tis);
      String text = MarshallingSupport.readUTF8(tdataIn);
      tdataIn.close();
      body.writeNullableSimpleString(new SimpleString(text));
   }

   private static void writeMapType(final ByteSequence contents,
                                    final boolean messageCompressed,
                                    final ActiveMQBuffer body) throws IOException {
      InputStream mis = new ByteArrayInputStream(contents);
      if (messageCompressed) {
         mis = new InflaterInputStream(mis);
      }
      DataInputStream mdataIn = new DataInputStream(mis);
      Map<String, Object> map = MarshallingSupport.unmarshalPrimitiveMap(mdataIn);
      mdataIn.close();
      TypedProperties props = new TypedProperties();
      loadMapIntoProperties(props, map);
      props.encode(body.byteBuf());
   }

   private static void writeObjectType(ByteSequence contents,
                                       final boolean messageCompressed,
                                       final ActiveMQBuffer body) throws IOException {
      if (messageCompressed) {
         contents = writeCompressedObjectType(contents);
      }
      body.writeInt(contents.length);
      body.writeBytes(contents.data, contents.offset, contents.length);
   }

   private static ByteSequence writeCompressedObjectType(final ByteSequence contents) throws IOException {
      try (InputStream ois = new InflaterInputStream(new ByteArrayInputStream(contents));
           org.apache.activemq.util.ByteArrayOutputStream decompressed = new org.apache.activemq.util.ByteArrayOutputStream()) {
         byte[] buf = new byte[1024];
         int n = ois.read(buf);
         while (n != -1) {
            decompressed.write(buf, 0, n);
            n = ois.read();
         }
         //read done
         return decompressed.toByteSequence();
      }
   }

   private static void writeStreamType(final ByteSequence contents,
                                       final boolean messageCompressed,
                                       final ActiveMQBuffer body) throws IOException {
      InputStream sis = new ByteArrayInputStream(contents);
      if (messageCompressed) {
         sis = new InflaterInputStream(sis);
      }
      DataInputStream sdis = new DataInputStream(sis);
      int stype = sdis.read();
      while (stype != -1) {
         switch (stype) {
            case MarshallingSupport.BOOLEAN_TYPE:
               body.writeByte(DataConstants.BOOLEAN);
               body.writeBoolean(sdis.readBoolean());
               break;
            case MarshallingSupport.BYTE_TYPE:
               body.writeByte(DataConstants.BYTE);
               body.writeByte(sdis.readByte());
               break;
            case MarshallingSupport.BYTE_ARRAY_TYPE:
               body.writeByte(DataConstants.BYTES);
               int slen = sdis.readInt();
               byte[] sbytes = new byte[slen];
               sdis.read(sbytes);
               body.writeInt(slen);
               body.writeBytes(sbytes);
               break;
            case MarshallingSupport.CHAR_TYPE:
               body.writeByte(DataConstants.CHAR);
               char schar = sdis.readChar();
               body.writeShort((short) schar);
               break;
            case MarshallingSupport.DOUBLE_TYPE:
               body.writeByte(DataConstants.DOUBLE);
               double sdouble = sdis.readDouble();
               body.writeLong(Double.doubleToLongBits(sdouble));
               break;
            case MarshallingSupport.FLOAT_TYPE:
               body.writeByte(DataConstants.FLOAT);
               float sfloat = sdis.readFloat();
               body.writeInt(Float.floatToIntBits(sfloat));
               break;
            case MarshallingSupport.INTEGER_TYPE:
               body.writeByte(DataConstants.INT);
               body.writeInt(sdis.readInt());
               break;
            case MarshallingSupport.LONG_TYPE:
               body.writeByte(DataConstants.LONG);
               body.writeLong(sdis.readLong());
               break;
            case MarshallingSupport.SHORT_TYPE:
               body.writeByte(DataConstants.SHORT);
               body.writeShort(sdis.readShort());
               break;
            case MarshallingSupport.STRING_TYPE:
               body.writeByte(DataConstants.STRING);
               String sstring = sdis.readUTF();
               body.writeNullableString(sstring);
               break;
            case MarshallingSupport.BIG_STRING_TYPE:
               body.writeByte(DataConstants.STRING);
               String sbigString = MarshallingSupport.readUTF8(sdis);
               body.writeNullableString(sbigString);
               break;
            case MarshallingSupport.NULL:
               body.writeByte(DataConstants.STRING);
               body.writeNullableString(null);
               break;
            default:
               //something we don't know, ignore
               break;
         }
         stype = sdis.read();
      }
      sdis.close();
   }

   private static void writeBytesType(ByteSequence contents,
                                      final boolean messageCompressed,
                                      final ActiveMQBuffer body) throws IOException {
      if (messageCompressed) {
         contents = writeCompressedBytesType(contents);
      }
      body.writeBytes(contents.data, contents.offset, contents.length);
   }

   private static ByteSequence writeCompressedBytesType(final ByteSequence contents) throws IOException {
      Inflater inflater = new Inflater();
      try (org.apache.activemq.util.ByteArrayOutputStream decompressed = new org.apache.activemq.util.ByteArrayOutputStream()) {
         int length = ByteSequenceData.readIntBig(contents);
         contents.offset = 0;
         byte[] data = Arrays.copyOfRange(contents.getData(), 4, contents.getLength());

         inflater.setInput(data);
         byte[] buffer = new byte[length];
         int count = inflater.inflate(buffer);
         decompressed.write(buffer, 0, count);
         return decompressed.toByteSequence();
      } catch (Exception e) {
         throw new IOException(e);
      } finally {
         inflater.end();
      }
   }

   private static void writeDefaultType(ByteSequence contents,
                                        final boolean messageCompressed,
                                        final ActiveMQBuffer body) throws IOException {
      if (messageCompressed) {
         contents = writeCompressedDefaultType(contents);
      }
      body.writeBytes(contents.data, contents.offset, contents.length);
   }

   private static ByteSequence writeCompressedDefaultType(final ByteSequence contents) throws IOException {
      try (org.apache.activemq.util.ByteArrayOutputStream decompressed = new org.apache.activemq.util.ByteArrayOutputStream();
           OutputStream os = new InflaterOutputStream(decompressed)) {
         os.write(contents.data, contents.offset, contents.getLength());
         return decompressed.toByteSequence();
      } catch (Exception e) {
         throw new IOException(e);
      }
   }

   private static void putMsgBrokerPath(final BrokerId[] brokers, final CoreMessage coreMessage) {
      final StringBuilder builder = new StringBuilder();
      for (int i = 0, size = brokers.length; i < size; i++) {
         builder.append(brokers[i].getValue());
         if (i != (size - 1)) {
            builder.append(','); //is this separator safe?
         }
      }
      coreMessage.putStringProperty(AMQ_MSG_BROKER_PATH, new SimpleString(builder.toString()));
   }

   private static void putMsgCluster(final BrokerId[] cluster, final CoreMessage coreMessage) {
      final StringBuilder builder = new StringBuilder();
      for (int i = 0, size = cluster.length; i < size; i++) {
         builder.append(cluster[i].getValue());
         if (i != (size - 1)) {
            builder.append(','); //is this separator safe?
         }
      }
      coreMessage.putStringProperty(AMQ_MSG_CLUSTER, new SimpleString(builder.toString()));
   }

   private static void putMsgDataStructure(final DataStructure ds,
                                           final WireFormat marshaller,
                                           final CoreMessage coreMessage) throws IOException {
      final ByteSequence dsBytes = marshaller.marshal(ds);
      dsBytes.compact();
      coreMessage.putBytesProperty(AMQ_MSG_DATASTRUCTURE, dsBytes.data);
   }

   private static void putMsgMarshalledProperties(final ByteSequence propBytes,
                                                  final Message messageSend,
                                                  final CoreMessage coreMessage) throws IOException {
      propBytes.compact();
      coreMessage.putBytesProperty(AMQ_MSG_MARSHALL_PROP, propBytes.data);
      //unmarshall properties to core so selector will work
      final Map<String, Object> props = messageSend.getProperties();
      if (!props.isEmpty()) {
         props.forEach((key, value) -> {
            try {
               coreMessage.putObjectProperty(key, value);
            } catch (ActiveMQPropertyConversionException e) {
               coreMessage.putStringProperty(key, value.toString());
            }
         });
      }
   }

   private static void putMsgReplyTo(final ActiveMQDestination replyTo,
                                     final WireFormat marshaller,
                                     final CoreMessage coreMessage) throws IOException {
      final ByteSequence replyToBytes = marshaller.marshal(replyTo);
      replyToBytes.compact();
      coreMessage.putBytesProperty(AMQ_MSG_REPLY_TO, replyToBytes.data);
   }

   private static void putMsgOriginalDestination(final ActiveMQDestination origDest,
                                                 final WireFormat marshaller,
                                                 final CoreMessage coreMessage) throws IOException {
      final ByteSequence origDestBytes = marshaller.marshal(origDest);
      origDestBytes.compact();
      coreMessage.putBytesProperty(AMQ_MSG_ORIG_DESTINATION, origDestBytes.data);
   }

   private static void loadMapIntoProperties(TypedProperties props, Map<String, Object> map) {
      for (Entry<String, Object> entry : map.entrySet()) {
         SimpleString key = new SimpleString(entry.getKey());
         Object value = entry.getValue();
         if (value instanceof UTF8Buffer) {
            value = ((UTF8Buffer) value).toString();
         }
         TypedProperties.setObjectProperty(key, value, props);
      }
   }

   public static byte toCoreType(byte amqType) {
      switch (amqType) {
         case CommandTypes.ACTIVEMQ_BLOB_MESSAGE:
            throw new IllegalStateException("We don't support BLOB type yet!");
         case CommandTypes.ACTIVEMQ_BYTES_MESSAGE:
            return org.apache.activemq.artemis.api.core.Message.BYTES_TYPE;
         case CommandTypes.ACTIVEMQ_MAP_MESSAGE:
            return org.apache.activemq.artemis.api.core.Message.MAP_TYPE;
         case CommandTypes.ACTIVEMQ_OBJECT_MESSAGE:
            return org.apache.activemq.artemis.api.core.Message.OBJECT_TYPE;
         case CommandTypes.ACTIVEMQ_STREAM_MESSAGE:
            return org.apache.activemq.artemis.api.core.Message.STREAM_TYPE;
         case CommandTypes.ACTIVEMQ_TEXT_MESSAGE:
            return org.apache.activemq.artemis.api.core.Message.TEXT_TYPE;
         case CommandTypes.ACTIVEMQ_MESSAGE:
            return org.apache.activemq.artemis.api.core.Message.DEFAULT_TYPE;
         default:
            throw new IllegalStateException("Unknown ActiveMQ Artemis message type: " + amqType);
      }
   }

   public static MessageDispatch createMessageDispatch(MessageReference reference,
                                                       ICoreMessage message,
                                                       WireFormat marshaller,
                                                       AMQConsumer consumer) throws IOException {
      ActiveMQMessage amqMessage = toAMQMessage(reference, message, marshaller, consumer);

      //we can use core message id for sequenceId
      amqMessage.getMessageId().setBrokerSequenceId(message.getMessageID());
      MessageDispatch md = new MessageDispatch();
      md.setConsumerId(consumer.getId());
      md.setRedeliveryCounter(reference.getDeliveryCount() - 1);
      md.setDeliverySequenceId(amqMessage.getMessageId().getBrokerSequenceId());
      md.setMessage(amqMessage);
      ActiveMQDestination destination = amqMessage.getDestination();
      md.setDestination(destination);

      return md;
   }

   private static ActiveMQMessage toAMQMessage(MessageReference reference,
                                               ICoreMessage coreMessage,
                                               WireFormat marshaller,
                                               AMQConsumer consumer) throws IOException {
      final ActiveMQMessage amqMsg;
      final byte coreType = coreMessage.getType();
      final Boolean compressProp = (Boolean) coreMessage.getObjectProperty(AMQ_MSG_COMPRESSED);
      final boolean isCompressed = compressProp == null ? false : compressProp.booleanValue();
      final byte[] bytes;
      final ActiveMQBuffer buffer = coreMessage.getReadOnlyBodyBuffer();
      buffer.resetReaderIndex();

      switch (coreType) {
         case org.apache.activemq.artemis.api.core.Message.BYTES_TYPE:
            amqMsg = new ActiveMQBytesMessage();
            bytes = toAMQMessageBytesType(buffer, isCompressed);
            break;
         case org.apache.activemq.artemis.api.core.Message.MAP_TYPE:
            amqMsg = new ActiveMQMapMessage();
            bytes = toAMQMessageMapType(buffer, isCompressed);
            break;
         case org.apache.activemq.artemis.api.core.Message.OBJECT_TYPE:
            amqMsg = new ActiveMQObjectMessage();
            bytes = toAMQMessageObjectType(buffer, isCompressed);
            break;
         case org.apache.activemq.artemis.api.core.Message.STREAM_TYPE:
            amqMsg = new ActiveMQStreamMessage();
            bytes = toAMQMessageStreamType(buffer, isCompressed);
            break;
         case org.apache.activemq.artemis.api.core.Message.TEXT_TYPE:
            amqMsg = new ActiveMQTextMessage();
            bytes = toAMQMessageTextType(buffer, isCompressed);
            break;
         case org.apache.activemq.artemis.api.core.Message.DEFAULT_TYPE:
            amqMsg = new ActiveMQMessage();
            bytes = toAMQMessageDefaultType(buffer, isCompressed);
            break;
         default:
            throw new IllegalStateException("Unknown message type: " + coreMessage.getType());
      }

      final String type = coreMessage.getStringProperty(JMS_TYPE_PROPERTY);
      if (type != null) {
         amqMsg.setJMSType(type);
      }
      amqMsg.setPersistent(coreMessage.isDurable());
      amqMsg.setExpiration(coreMessage.getExpiration());
      amqMsg.setPriority(coreMessage.getPriority());
      amqMsg.setTimestamp(coreMessage.getTimestamp());

      Long brokerInTime = (Long) coreMessage.getObjectProperty(AMQ_MSG_BROKER_IN_TIME);
      if (brokerInTime == null) {
         brokerInTime = 0L;
      }
      amqMsg.setBrokerInTime(brokerInTime);

      amqMsg.setCompressed(isCompressed);

      //we need check null because messages may come from other clients
      //and those amq specific attribute may not be set.
      Long arrival = (Long) coreMessage.getObjectProperty(AMQ_MSG_ARRIVAL);
      if (arrival == null) {
         //messages from other sources (like core client) may not set this prop
         arrival = 0L;
      }
      amqMsg.setArrival(arrival);

      final String brokerPath = (String) coreMessage.getObjectProperty(AMQ_MSG_BROKER_PATH);
      if (brokerPath != null && !brokerPath.isEmpty()) {
         setAMQMsgBrokerPath(amqMsg, brokerPath);
      }

      final String clusterPath = (String) coreMessage.getObjectProperty(AMQ_MSG_CLUSTER);
      if (clusterPath != null && !clusterPath.isEmpty()) {
         setAMQMsgClusterPath(amqMsg, clusterPath);
      }

      Integer commandId = (Integer) coreMessage.getObjectProperty(AMQ_MSG_COMMAND_ID);
      if (commandId == null) {
         commandId = -1;
      }
      amqMsg.setCommandId(commandId);

      final SimpleString corrId = (SimpleString) coreMessage.getObjectProperty(JMS_CORRELATION_ID_PROPERTY);
      if (corrId != null) {
         amqMsg.setCorrelationId(corrId.toString());
      }

      final byte[] dsBytes = (byte[]) coreMessage.getObjectProperty(AMQ_MSG_DATASTRUCTURE);
      if (dsBytes != null) {
         setAMQMsgDataStructure(amqMsg, marshaller, dsBytes);
      }
      final ActiveMQDestination actualDestination = consumer.getOpenwireDestination();
      amqMsg.setDestination(OpenWireUtil.toAMQAddress(coreMessage, actualDestination));

      final Object value = coreMessage.getGroupID();
      if (value != null) {
         String groupId = value.toString();
         amqMsg.setGroupID(groupId);
      }

      Integer groupSequence = (Integer) coreMessage.getObjectProperty(AMQ_MSG_GROUP_SEQUENCE);
      if (groupSequence == null) {
         groupSequence = -1;
      }
      amqMsg.setGroupSequence(groupSequence);

      final MessageId mid;
      final byte[] midBytes = (byte[]) coreMessage.getObjectProperty(AMQ_MSG_MESSAGE_ID);
      if (midBytes != null) {
         ByteSequence midSeq = new ByteSequence(midBytes);
         mid = (MessageId) marshaller.unmarshal(midSeq);
      } else {
         mid = new MessageId(UUIDGenerator.getInstance().generateStringUUID() + ":-1");
      }

      amqMsg.setMessageId(mid);

      final byte[] origDestBytes = (byte[]) coreMessage.getObjectProperty(AMQ_MSG_ORIG_DESTINATION);
      if (origDestBytes != null) {
         setAMQMsgOriginalDestination(amqMsg, marshaller, origDestBytes);
      }

      final byte[] origTxIdBytes = (byte[]) coreMessage.getObjectProperty(AMQ_MSG_ORIG_TXID);
      if (origTxIdBytes != null) {
         setAMQMsgOriginalTransactionId(amqMsg, marshaller, origTxIdBytes);
      }

      final byte[] producerIdBytes = (byte[]) coreMessage.getObjectProperty(AMQ_MSG_PRODUCER_ID);
      if (producerIdBytes != null) {
         ProducerId producerId = (ProducerId) marshaller.unmarshal(new ByteSequence(producerIdBytes));
         amqMsg.setProducerId(producerId);
      }

      final byte[] marshalledBytes = (byte[]) coreMessage.getObjectProperty(AMQ_MSG_MARSHALL_PROP);
      if (marshalledBytes != null) {
         amqMsg.setMarshalledProperties(new ByteSequence(marshalledBytes));
      }

      amqMsg.setRedeliveryCounter(reference.getDeliveryCount() - 1);

      final byte[] replyToBytes = (byte[]) coreMessage.getObjectProperty(AMQ_MSG_REPLY_TO);
      if (replyToBytes != null) {
         setAMQMsgReplyTo(amqMsg, marshaller, replyToBytes);
      }

      final String userId = (String) coreMessage.getObjectProperty(AMQ_MSG_USER_ID);
      if (userId != null) {
         amqMsg.setUserID(userId);
      }

      final Boolean isDroppable = (Boolean) coreMessage.getObjectProperty(AMQ_MSG_DROPPABLE);
      if (isDroppable != null) {
         amqMsg.setDroppable(isDroppable);
      }

      final SimpleString dlqCause = (SimpleString) coreMessage.getObjectProperty(AMQ_MSG_DLQ_DELIVERY_FAILURE_CAUSE_PROPERTY);
      if (dlqCause != null) {
         setAMQMsgDlqDeliveryFailureCause(amqMsg, dlqCause);
      }

      final SimpleString lastValueProperty = coreMessage.getLastValueProperty();
      if (lastValueProperty != null) {
         setAMQMsgHdrLastValueName(amqMsg, lastValueProperty);
      }

      final Set<SimpleString> props = coreMessage.getPropertyNames();
      if (props != null) {
         setAMQMsgObjectProperties(amqMsg, coreMessage, props, consumer);
      }

      if (bytes != null) {
         ByteSequence content = new ByteSequence(bytes);
         amqMsg.setContent(content);
      }
      return amqMsg;
   }

   private static byte[] toAMQMessageTextType(final ActiveMQBuffer buffer,
                                              final boolean isCompressed) throws IOException {
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

   private static byte[] toAMQMessageMapType(final ActiveMQBuffer buffer,
                                             final boolean isCompressed) throws IOException {
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

   private static byte[] toAMQMessageObjectType(final ActiveMQBuffer buffer,
                                                final boolean isCompressed) throws IOException {
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

   private static byte[] toAMQMessageStreamType(final ActiveMQBuffer buffer,
                                                final boolean isCompressed) throws IOException {
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
                  Float floatVal = Float.intBitsToFloat(buffer.readInt());
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

   private static byte[] toAMQMessageBytesType(final ActiveMQBuffer buffer,
                                               final boolean isCompressed) throws IOException {
      int n = buffer.readableBytes();
      byte[] bytes = new byte[n];
      buffer.readBytes(bytes);
      if (isCompressed) {
         bytes = toAMQMessageCompressedBytesType(bytes);
      }
      return bytes;
   }

   private static byte[] toAMQMessageCompressedBytesType(final byte[] bytes) throws IOException {
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

   private static byte[] toAMQMessageDefaultType(final ActiveMQBuffer buffer,
                                                 final boolean isCompressed) throws IOException {
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

   private static void setAMQMsgBrokerPath(final ActiveMQMessage amqMsg, final String brokerPath) {
      String[] brokers = brokerPath.split(",");
      BrokerId[] bids = new BrokerId[brokers.length];
      for (int i = 0; i < bids.length; i++) {
         bids[i] = new BrokerId(brokers[i]);
      }
      amqMsg.setBrokerPath(bids);
   }

   private static void setAMQMsgClusterPath(final ActiveMQMessage amqMsg, final String clusterPath) {
      String[] cluster = clusterPath.split(",");
      BrokerId[] bids = new BrokerId[cluster.length];
      for (int i = 0; i < bids.length; i++) {
         bids[i] = new BrokerId(cluster[i]);
      }
      amqMsg.setCluster(bids);
   }

   private static void setAMQMsgDataStructure(final ActiveMQMessage amqMsg,
                                              final WireFormat marshaller,
                                              final byte[] dsBytes) throws IOException {
      ByteSequence seq = new ByteSequence(dsBytes);
      DataStructure ds = (DataStructure) marshaller.unmarshal(seq);
      amqMsg.setDataStructure(ds);
   }

   private static void setAMQMsgOriginalDestination(final ActiveMQMessage amqMsg,
                                                    final WireFormat marshaller,
                                                    final byte[] origDestBytes) throws IOException {
      ActiveMQDestination origDest = (ActiveMQDestination) marshaller.unmarshal(new ByteSequence(origDestBytes));
      amqMsg.setOriginalDestination(origDest);
   }

   private static void setAMQMsgOriginalTransactionId(final ActiveMQMessage amqMsg,
                                                      final WireFormat marshaller,
                                                      final byte[] origTxIdBytes) throws IOException {
      TransactionId origTxId = (TransactionId) marshaller.unmarshal(new ByteSequence(origTxIdBytes));
      amqMsg.setOriginalTransactionId(origTxId);
   }

   private static void setAMQMsgReplyTo(final ActiveMQMessage amqMsg,
                                        final WireFormat marshaller,
                                        final byte[] replyToBytes) throws IOException {
      ActiveMQDestination replyTo = (ActiveMQDestination) marshaller.unmarshal(new ByteSequence(replyToBytes));
      amqMsg.setReplyTo(replyTo);
   }

   private static void setAMQMsgDlqDeliveryFailureCause(final ActiveMQMessage amqMsg,
                                                        final SimpleString dlqCause) throws IOException {
      try {
         amqMsg.setStringProperty(ActiveMQMessage.DLQ_DELIVERY_FAILURE_CAUSE_PROPERTY, dlqCause.toString());
      } catch (JMSException e) {
         throw new IOException("failure to set dlq property " + dlqCause, e);
      }
   }

   private static void setAMQMsgHdrLastValueName(final ActiveMQMessage amqMsg,
                                                 final SimpleString lastValueProperty) throws IOException {
      try {
         amqMsg.setStringProperty(org.apache.activemq.artemis.api.core.Message.HDR_LAST_VALUE_NAME.toString(), lastValueProperty.toString());
      } catch (JMSException e) {
         throw new IOException("failure to set lvq property " + lastValueProperty, e);
      }
   }

   private static void setAMQMsgObjectProperties(final ActiveMQMessage amqMsg,
                                                 final ICoreMessage coreMessage,
                                                 final Set<SimpleString> props,
                                                 final AMQConsumer consumer) throws IOException {
      for (SimpleString s : props) {
         final String keyStr = s.toString();
         if (!consumer.hasNotificationDestination() && (keyStr.startsWith("_AMQ") || keyStr.startsWith("__HDR_"))) {
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
