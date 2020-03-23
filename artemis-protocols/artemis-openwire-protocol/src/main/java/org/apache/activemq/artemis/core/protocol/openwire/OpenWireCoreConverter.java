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

import static org.apache.activemq.artemis.core.protocol.openwire.OpenWireMessageSupport.setBinaryObjectProperty;

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
import static org.apache.activemq.artemis.core.protocol.openwire.OpenWireMessageSupport.AMQ_MSG_REPLY_TO;

import static org.apache.activemq.artemis.api.core.Message.TEXT_TYPE;
import static org.apache.activemq.artemis.api.core.Message.MAP_TYPE;
import static org.apache.activemq.artemis.api.core.Message.OBJECT_TYPE;
import static org.apache.activemq.artemis.api.core.Message.STREAM_TYPE;
import static org.apache.activemq.artemis.api.core.Message.BYTES_TYPE;

import static org.apache.activemq.artemis.jms.client.ActiveMQDestination.TEMP_QUEUE_QUALIFED_PREFIX;
import static org.apache.activemq.artemis.jms.client.ActiveMQDestination.TEMP_TOPIC_QUALIFED_PREFIX;
import static org.apache.activemq.artemis.jms.client.ActiveMQDestination.QUEUE_QUALIFIED_PREFIX;
import static org.apache.activemq.artemis.jms.client.ActiveMQDestination.TOPIC_QUALIFIED_PREFIX;

import javax.jms.Queue;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.Topic;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;
import java.util.zip.InflaterOutputStream;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQPropertyConversionException;
import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.core.persistence.CoreMessageObjectPools;
import org.apache.activemq.artemis.reader.MessageUtil;
import org.apache.activemq.artemis.utils.DataConstants;
import org.apache.activemq.artemis.utils.UUIDGenerator;
import org.apache.activemq.artemis.utils.collections.TypedProperties;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.BrokerId;
import org.apache.activemq.command.CommandTypes;
import org.apache.activemq.command.DataStructure;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.util.ByteArrayInputStream;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.ByteSequenceData;
import org.apache.activemq.util.MarshallingSupport;
import org.apache.activemq.wireformat.WireFormat;
import org.fusesource.hawtbuf.UTF8Buffer;
import org.jboss.logging.Logger;

public class OpenWireCoreConverter {

   private static final Logger LOG = Logger.getLogger(OpenWireCoreConverter.class);

   public static ICoreMessage createInboundMessage(Message amqMessage,
                                                   WireFormat format,
                                                   CoreMessageObjectPools coreMessageObjectPools) throws Exception {
      if (LOG.isTraceEnabled()) {
         LOG.trace("Converting message from OpenWire to Core");
      }

      final ICoreMessage coreMessage = new CoreMessage(-1, amqMessage.getSize(), coreMessageObjectPools);

      final String type = amqMessage.getType();
      if (type != null) {
         coreMessage.putStringProperty(JMS_TYPE_PROPERTY, type);
      }

      coreMessage.setDurable(amqMessage.isPersistent());
      coreMessage.setExpiration(amqMessage.getExpiration());
      coreMessage.setPriority(amqMessage.getPriority());
      coreMessage.setTimestamp(amqMessage.getTimestamp());

      final byte coreType = toCoreType(amqMessage.getDataStructureType());
      coreMessage.setType(coreType);

      final ActiveMQBuffer body = coreMessage.getBodyBuffer();
      final ByteSequence contents = amqMessage.getContent();
      if (contents == null && coreType == TEXT_TYPE) {
         body.writeNullableString(null);
      } else if (contents != null) {
         final boolean messageCompressed = amqMessage.isCompressed();
         if (messageCompressed) {
            coreMessage.putBooleanProperty(AMQ_MSG_COMPRESSED, messageCompressed);
         }
         switch (coreType) {
            case TEXT_TYPE:
               writeTextType(contents, messageCompressed, body);
               break;
            case MAP_TYPE:
               writeMapType(contents, messageCompressed, body);
               break;
            case OBJECT_TYPE:
               writeObjectType(contents, messageCompressed, body);
               break;
            case STREAM_TYPE:
               writeStreamType(contents, messageCompressed, body);
               break;
            case BYTES_TYPE:
               writeBytesType(contents, messageCompressed, body);
               break;
            default:
               writeDefaultType(contents, messageCompressed, body);
               break;
         }
      }

      //amq specific
      coreMessage.putLongProperty(AMQ_MSG_ARRIVAL, amqMessage.getArrival());
      coreMessage.putLongProperty(AMQ_MSG_BROKER_IN_TIME, amqMessage.getBrokerInTime());

      final BrokerId[] brokers = amqMessage.getBrokerPath();
      if (brokers != null) {
         putBrokerIdsString(brokers, coreMessage, AMQ_MSG_BROKER_PATH);
      }

      final BrokerId[] cluster = amqMessage.getCluster();
      if (cluster != null) {
         putBrokerIdsString(cluster, coreMessage, AMQ_MSG_CLUSTER);
      }

      coreMessage.putIntProperty(AMQ_MSG_COMMAND_ID, amqMessage.getCommandId());

      final String corrId = amqMessage.getCorrelationId();
      if (corrId != null) {
         coreMessage.putStringProperty(JMS_CORRELATION_ID_PROPERTY, corrId);
      }

      final DataStructure ds = amqMessage.getDataStructure();
      if (ds != null) {
         setBinaryObjectProperty(format, coreMessage, AMQ_MSG_DATASTRUCTURE, ds);
      }

      final String groupId = amqMessage.getGroupID();
      if (groupId != null) {
         coreMessage.setGroupID(groupId);
      }

      coreMessage.setGroupSequence(amqMessage.getGroupSequence());

      final MessageId messageId = amqMessage.getMessageId();
      setBinaryObjectProperty(format, coreMessage, AMQ_MSG_MESSAGE_ID, messageId);

      coreMessage.setUserID(UUIDGenerator.getInstance().generateUUID());

      final ProducerId producerId = amqMessage.getProducerId();
      if (producerId != null) {
         setBinaryObjectProperty(format, coreMessage, AMQ_MSG_PRODUCER_ID, producerId);
      }

      final ByteSequence propBytes = amqMessage.getMarshalledProperties();
      if (propBytes != null) {
         putMsgMarshalledProperties(propBytes, amqMessage, coreMessage);
      }

      final ActiveMQDestination replyTo = amqMessage.getReplyTo();
      if (replyTo != null) {
         setBinaryObjectProperty(format, coreMessage, AMQ_MSG_REPLY_TO, replyTo);
         if (replyTo instanceof TemporaryQueue) {
            MessageUtil.setJMSReplyTo(coreMessage, TEMP_QUEUE_QUALIFED_PREFIX + (((TemporaryQueue) replyTo).getQueueName()));
         } else if (replyTo instanceof TemporaryTopic) {
            MessageUtil.setJMSReplyTo(coreMessage, TEMP_TOPIC_QUALIFED_PREFIX + (((TemporaryTopic) replyTo).getTopicName()));
         } else if (replyTo instanceof Queue) {
            MessageUtil.setJMSReplyTo(coreMessage, QUEUE_QUALIFIED_PREFIX + (((Queue) replyTo).getQueueName()));
         } else if (replyTo instanceof Topic) {
            MessageUtil.setJMSReplyTo(coreMessage, TOPIC_QUALIFIED_PREFIX + (((Topic) replyTo).getTopicName()));
         } else {
            // it should not happen
            MessageUtil.setJMSReplyTo(coreMessage, QUEUE_QUALIFIED_PREFIX + (((Queue) replyTo).getQueueName()));
         }
      }

      final String userId = amqMessage.getUserID();
      if (userId != null) {
         coreMessage.putStringProperty(AMQ_MSG_USER_ID, userId);
      }

      coreMessage.putBooleanProperty(AMQ_MSG_DROPPABLE, amqMessage.isDroppable());

      final ActiveMQDestination origDest = amqMessage.getOriginalDestination();
      if (origDest != null) {
         setBinaryObjectProperty(format, coreMessage, AMQ_MSG_ORIG_DESTINATION, origDest);
      }

      return coreMessage;
   }

   private static void writeTextType(ByteSequence contents,
                                     boolean messageCompressed,
                                     ActiveMQBuffer body) throws IOException {
      InputStream tis = new ByteArrayInputStream(contents);
      if (messageCompressed) {
         tis = new InflaterInputStream(tis);
      }
      DataInputStream tdataIn = new DataInputStream(tis);
      String text = MarshallingSupport.readUTF8(tdataIn);
      tdataIn.close();
      body.writeNullableSimpleString(new SimpleString(text));
   }

   private static void writeMapType(ByteSequence contents,
                                    boolean messageCompressed,
                                    ActiveMQBuffer body) throws IOException {
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
                                       boolean messageCompressed,
                                       ActiveMQBuffer body) throws IOException {
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
                                      boolean messageCompressed,
                                      ActiveMQBuffer body) throws IOException {
      if (messageCompressed) {
         contents = writeCompressedBytesType(contents);
      }
      body.writeBytes(contents.data, contents.offset, contents.length);
   }

   private static ByteSequence writeCompressedBytesType(ByteSequence contents) throws IOException {
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
                                        boolean messageCompressed,
                                        ActiveMQBuffer body) throws IOException {
      if (messageCompressed) {
         contents = writeCompressedDefaultType(contents);
      }
      body.writeBytes(contents.data, contents.offset, contents.length);
   }

   private static ByteSequence writeCompressedDefaultType(ByteSequence contents) throws IOException {
      try (org.apache.activemq.util.ByteArrayOutputStream decompressed = new org.apache.activemq.util.ByteArrayOutputStream();
           OutputStream os = new InflaterOutputStream(decompressed)) {
         os.write(contents.data, contents.offset, contents.getLength());
         return decompressed.toByteSequence();
      } catch (Exception e) {
         throw new IOException(e);
      }
   }

   private static void putBrokerIdsString(BrokerId[] cluster,
                                          ICoreMessage coreMessage,
                                          SimpleString propertyName) {
      final StringBuilder builder = new StringBuilder();
      for (int i = 0, size = cluster.length; i < size; i++) {
         builder.append(cluster[i].getValue());
         if (i != (size - 1)) {
            builder.append(',');
         }
      }
      coreMessage.putStringProperty(propertyName, builder.toString());
   }

   private static void putMsgMarshalledProperties(ByteSequence propBytes,
                                                  Message messageSend,
                                                  ICoreMessage coreMessage) throws IOException {
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

   private static byte toCoreType(byte amqType) {
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
            return TEXT_TYPE;
         case CommandTypes.ACTIVEMQ_MESSAGE:
            return org.apache.activemq.artemis.api.core.Message.DEFAULT_TYPE;
         default:
            throw new IllegalStateException("Unknown ActiveMQ Artemis message type: " + amqType);
      }
   }

}
