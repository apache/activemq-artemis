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
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.protocol.openwire.amq.AMQConsumer;
import org.apache.activemq.artemis.core.protocol.openwire.util.OpenWireUtil;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.ServerMessage;
import org.apache.activemq.artemis.core.server.impl.ServerMessageImpl;
import org.apache.activemq.artemis.spi.core.protocol.MessageConverter;
import org.apache.activemq.artemis.utils.DataConstants;
import org.apache.activemq.artemis.utils.TypedProperties;
import org.apache.activemq.artemis.utils.UUIDGenerator;
import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMapMessage;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQObjectMessage;
import org.apache.activemq.command.ActiveMQStreamMessage;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.BrokerId;
import org.apache.activemq.command.CommandTypes;
import org.apache.activemq.command.ConsumerId;
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

public class OpenWireMessageConverter implements MessageConverter {

   public static final String AMQ_PREFIX = "__HDR_";
   public static final String AMQ_MSG_DLQ_DELIVERY_FAILURE_CAUSE_PROPERTY = AMQ_PREFIX + "dlqDeliveryFailureCause";

   private static final String AMQ_MSG_ARRIVAL = AMQ_PREFIX + "ARRIVAL";
   private static final String AMQ_MSG_BROKER_IN_TIME = AMQ_PREFIX + "BROKER_IN_TIME";

   private static final String AMQ_MSG_BROKER_PATH = AMQ_PREFIX + "BROKER_PATH";
   private static final String AMQ_MSG_CLUSTER = AMQ_PREFIX + "CLUSTER";
   private static final String AMQ_MSG_COMMAND_ID = AMQ_PREFIX + "COMMAND_ID";
   private static final String AMQ_MSG_DATASTRUCTURE = AMQ_PREFIX + "DATASTRUCTURE";
   private static final String AMQ_MSG_GROUP_ID = AMQ_PREFIX + "GROUP_ID";
   private static final String AMQ_MSG_GROUP_SEQUENCE = AMQ_PREFIX + "GROUP_SEQUENCE";
   private static final String AMQ_MSG_MESSAGE_ID = AMQ_PREFIX + "MESSAGE_ID";
   private static final String AMQ_MSG_ORIG_DESTINATION = AMQ_PREFIX + "ORIG_DESTINATION";
   private static final String AMQ_MSG_ORIG_TXID = AMQ_PREFIX + "ORIG_TXID";
   private static final String AMQ_MSG_PRODUCER_ID = AMQ_PREFIX + "PRODUCER_ID";
   private static final String AMQ_MSG_MARSHALL_PROP = AMQ_PREFIX + "MARSHALL_PROP";
   private static final String AMQ_MSG_REPLY_TO = AMQ_PREFIX + "REPLY_TO";

   private static final String AMQ_MSG_USER_ID = AMQ_PREFIX + "USER_ID";

   private static final String AMQ_MSG_DROPPABLE = AMQ_PREFIX + "DROPPABLE";
   private static final String AMQ_MSG_COMPRESSED = AMQ_PREFIX + "COMPRESSED";

   private final WireFormat marshaller;

   public OpenWireMessageConverter(WireFormat marshaller) {
      this.marshaller = marshaller;
   }

   @Override
   public Object outbound(ServerMessage message, int deliveryCount) {
      // TODO: implement this
      return null;
   }

   @Override
   public ServerMessage inbound(Object message) throws Exception {

      Message messageSend = (Message) message;
      ServerMessageImpl coreMessage = new ServerMessageImpl(-1, messageSend.getSize());

      String type = messageSend.getType();
      if (type != null) {
         coreMessage.putStringProperty(new SimpleString("JMSType"), new SimpleString(type));
      }
      coreMessage.setDurable(messageSend.isPersistent());
      coreMessage.setExpiration(messageSend.getExpiration());
      coreMessage.setPriority(messageSend.getPriority());
      coreMessage.setTimestamp(messageSend.getTimestamp());

      byte coreType = toCoreType(messageSend.getDataStructureType());
      coreMessage.setType(coreType);

      ActiveMQBuffer body = coreMessage.getBodyBuffer();

      ByteSequence contents = messageSend.getContent();
      if (contents == null && coreType == org.apache.activemq.artemis.api.core.Message.TEXT_TYPE) {
         body.writeNullableString(null);
      } else if (contents != null) {
         boolean messageCompressed = messageSend.isCompressed();
         if (messageCompressed) {
            coreMessage.putBooleanProperty(AMQ_MSG_COMPRESSED, messageCompressed);
         }

         switch (coreType) {
            case org.apache.activemq.artemis.api.core.Message.TEXT_TYPE:
               InputStream tis = new ByteArrayInputStream(contents);
               if (messageCompressed) {
                  tis = new InflaterInputStream(tis);
               }
               DataInputStream tdataIn = new DataInputStream(tis);
               String text = MarshallingSupport.readUTF8(tdataIn);
               tdataIn.close();
               body.writeNullableSimpleString(new SimpleString(text));
               break;
            case org.apache.activemq.artemis.api.core.Message.MAP_TYPE:
               InputStream mis = new ByteArrayInputStream(contents);
               if (messageCompressed) {
                  mis = new InflaterInputStream(mis);
               }
               DataInputStream mdataIn = new DataInputStream(mis);
               Map<String, Object> map = MarshallingSupport.unmarshalPrimitiveMap(mdataIn);
               mdataIn.close();
               TypedProperties props = new TypedProperties();
               loadMapIntoProperties(props, map);
               props.encode(body);
               break;
            case org.apache.activemq.artemis.api.core.Message.OBJECT_TYPE:
               if (messageCompressed) {
                  try (InputStream ois = new InflaterInputStream(new ByteArrayInputStream(contents));
                       org.apache.activemq.util.ByteArrayOutputStream decompressed = new org.apache.activemq.util.ByteArrayOutputStream()) {
                     byte[] buf = new byte[1024];
                     int n = ois.read(buf);
                     while (n != -1) {
                        decompressed.write(buf, 0, n);
                        n = ois.read();
                     }
                     //read done
                     contents = decompressed.toByteSequence();
                  }
               }
               body.writeInt(contents.length);
               body.writeBytes(contents.data, contents.offset, contents.length);
               break;
            case org.apache.activemq.artemis.api.core.Message.STREAM_TYPE:
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
               break;
            case org.apache.activemq.artemis.api.core.Message.BYTES_TYPE:
               if (messageCompressed) {
                  Inflater inflater = new Inflater();
                  try (org.apache.activemq.util.ByteArrayOutputStream decompressed = new org.apache.activemq.util.ByteArrayOutputStream()) {
                     int length = ByteSequenceData.readIntBig(contents);
                     contents.offset = 0;
                     byte[] data = Arrays.copyOfRange(contents.getData(), 4, contents.getLength());

                     inflater.setInput(data);
                     byte[] buffer = new byte[length];
                     int count = inflater.inflate(buffer);
                     decompressed.write(buffer, 0, count);
                     contents = decompressed.toByteSequence();
                  } catch (Exception e) {
                     throw new IOException(e);
                  } finally {
                     inflater.end();
                  }
               }
               body.writeBytes(contents.data, contents.offset, contents.length);
               break;
            default:
               if (messageCompressed) {
                  try (org.apache.activemq.util.ByteArrayOutputStream decompressed = new org.apache.activemq.util.ByteArrayOutputStream();
                       OutputStream os = new InflaterOutputStream(decompressed)) {
                     os.write(contents.data, contents.offset, contents.getLength());
                     contents = decompressed.toByteSequence();
                  } catch (Exception e) {
                     throw new IOException(e);
                  }
               }
               body.writeBytes(contents.data, contents.offset, contents.length);
               break;
         }
      }
      //amq specific
      coreMessage.putLongProperty(AMQ_MSG_ARRIVAL, messageSend.getArrival());
      coreMessage.putLongProperty(AMQ_MSG_BROKER_IN_TIME, messageSend.getBrokerInTime());
      BrokerId[] brokers = messageSend.getBrokerPath();
      if (brokers != null) {
         StringBuilder builder = new StringBuilder();
         for (int i = 0; i < brokers.length; i++) {
            builder.append(brokers[i].getValue());
            if (i != (brokers.length - 1)) {
               builder.append(","); //is this separator safe?
            }
         }
         coreMessage.putStringProperty(AMQ_MSG_BROKER_PATH, builder.toString());
      }
      BrokerId[] cluster = messageSend.getCluster();
      if (cluster != null) {
         StringBuilder builder = new StringBuilder();
         for (int i = 0; i < cluster.length; i++) {
            builder.append(cluster[i].getValue());
            if (i != (cluster.length - 1)) {
               builder.append(","); //is this separator safe?
            }
         }
         coreMessage.putStringProperty(AMQ_MSG_CLUSTER, builder.toString());
      }

      coreMessage.putIntProperty(AMQ_MSG_COMMAND_ID, messageSend.getCommandId());
      String corrId = messageSend.getCorrelationId();
      if (corrId != null) {
         coreMessage.putStringProperty("JMSCorrelationID", corrId);
      }
      DataStructure ds = messageSend.getDataStructure();
      if (ds != null) {
         ByteSequence dsBytes = marshaller.marshal(ds);
         dsBytes.compact();
         coreMessage.putBytesProperty(AMQ_MSG_DATASTRUCTURE, dsBytes.data);
      }
      String groupId = messageSend.getGroupID();
      if (groupId != null) {
         coreMessage.putStringProperty(AMQ_MSG_GROUP_ID, groupId);
      }
      coreMessage.putIntProperty(AMQ_MSG_GROUP_SEQUENCE, messageSend.getGroupSequence());

      MessageId messageId = messageSend.getMessageId();

      ByteSequence midBytes = marshaller.marshal(messageId);
      midBytes.compact();
      coreMessage.putBytesProperty(AMQ_MSG_MESSAGE_ID, midBytes.data);

      ProducerId producerId = messageSend.getProducerId();
      if (producerId != null) {
         ByteSequence producerIdBytes = marshaller.marshal(producerId);
         producerIdBytes.compact();
         coreMessage.putBytesProperty(AMQ_MSG_PRODUCER_ID, producerIdBytes.data);
      }
      ByteSequence propBytes = messageSend.getMarshalledProperties();
      if (propBytes != null) {
         propBytes.compact();
         coreMessage.putBytesProperty(AMQ_MSG_MARSHALL_PROP, propBytes.data);
         //unmarshall properties to core so selector will work
         Map<String, Object> props = messageSend.getProperties();
         //Map<String, Object> props = MarshallingSupport.unmarshalPrimitiveMap(new DataInputStream(new ByteArrayInputStream(propBytes)));
         for (Entry<String, Object> ent : props.entrySet()) {
            Object value = ent.getValue();
            try {
               coreMessage.putObjectProperty(ent.getKey(), value);
            } catch (ActiveMQPropertyConversionException e) {
               coreMessage.putStringProperty(ent.getKey(), value.toString());
            }
         }
      }

      ActiveMQDestination replyTo = messageSend.getReplyTo();
      if (replyTo != null) {
         ByteSequence replyToBytes = marshaller.marshal(replyTo);
         replyToBytes.compact();
         coreMessage.putBytesProperty(AMQ_MSG_REPLY_TO, replyToBytes.data);
      }

      ConsumerId consumerId = messageSend.getTargetConsumerId();

      String userId = messageSend.getUserID();
      if (userId != null) {
         coreMessage.putStringProperty(AMQ_MSG_USER_ID, userId);
      }
      coreMessage.putBooleanProperty(AMQ_MSG_DROPPABLE, messageSend.isDroppable());

      ActiveMQDestination origDest = messageSend.getOriginalDestination();
      if (origDest != null) {
         ByteSequence origDestBytes = marshaller.marshal(origDest);
         origDestBytes.compact();
         coreMessage.putBytesProperty(AMQ_MSG_ORIG_DESTINATION, origDestBytes.data);
      }

      return coreMessage;
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
                                                       ServerMessage message,
                                                       AMQConsumer consumer) throws IOException, JMSException {
      ActiveMQMessage amqMessage = toAMQMessage(reference, message, consumer.getMarshaller(), consumer.getOpenwireDestination());

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
                                               ServerMessage coreMessage,
                                               WireFormat marshaller,
                                               ActiveMQDestination actualDestination) throws IOException {
      ActiveMQMessage amqMsg = null;
      byte coreType = coreMessage.getType();
      switch (coreType) {
         case org.apache.activemq.artemis.api.core.Message.BYTES_TYPE:
            amqMsg = new ActiveMQBytesMessage();
            break;
         case org.apache.activemq.artemis.api.core.Message.MAP_TYPE:
            amqMsg = new ActiveMQMapMessage();
            break;
         case org.apache.activemq.artemis.api.core.Message.OBJECT_TYPE:
            amqMsg = new ActiveMQObjectMessage();
            break;
         case org.apache.activemq.artemis.api.core.Message.STREAM_TYPE:
            amqMsg = new ActiveMQStreamMessage();
            break;
         case org.apache.activemq.artemis.api.core.Message.TEXT_TYPE:
            amqMsg = new ActiveMQTextMessage();
            break;
         case org.apache.activemq.artemis.api.core.Message.DEFAULT_TYPE:
            amqMsg = new ActiveMQMessage();
            break;
         default:
            throw new IllegalStateException("Unknown message type: " + coreMessage.getType());
      }

      String type = coreMessage.getStringProperty(new SimpleString("JMSType"));
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

      ActiveMQBuffer buffer = coreMessage.getBodyBufferDuplicate();
      Boolean compressProp = (Boolean) coreMessage.getObjectProperty(AMQ_MSG_COMPRESSED);
      boolean isCompressed = compressProp == null ? false : compressProp.booleanValue();
      amqMsg.setCompressed(isCompressed);

      if (buffer != null) {
         buffer.resetReaderIndex();
         byte[] bytes = null;
         synchronized (buffer) {
            if (coreType == org.apache.activemq.artemis.api.core.Message.TEXT_TYPE) {
               SimpleString text = buffer.readNullableSimpleString();
               if (text != null) {
                  ByteArrayOutputStream bytesOut = new ByteArrayOutputStream(text.length() + 4);
                  OutputStream out = bytesOut;
                  if (isCompressed) {
                     out = new DeflaterOutputStream(out);
                  }
                  try (DataOutputStream dataOut = new DataOutputStream(out)) {
                     MarshallingSupport.writeUTF8(dataOut, text.toString());
                     bytes = bytesOut.toByteArray();
                  }
               }
            } else if (coreType == org.apache.activemq.artemis.api.core.Message.MAP_TYPE) {
               TypedProperties mapData = new TypedProperties();
               //it could be a null map
               if (buffer.readableBytes() > 0) {
                  mapData.decode(buffer);
                  Map<String, Object> map = mapData.getMap();
                  ByteArrayOutputStream out = new ByteArrayOutputStream(mapData.getEncodeSize());
                  OutputStream os = out;
                  if (isCompressed) {
                     os = new DeflaterOutputStream(os);
                  }
                  try (DataOutputStream dataOut = new DataOutputStream(os)) {
                     MarshallingSupport.marshalPrimitiveMap(map, dataOut);
                  }
                  bytes = out.toByteArray();
               }

            } else if (coreType == org.apache.activemq.artemis.api.core.Message.OBJECT_TYPE) {
               int len = buffer.readInt();
               bytes = new byte[len];
               buffer.readBytes(bytes);
               if (isCompressed) {
                  ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
                  try (DeflaterOutputStream out = new DeflaterOutputStream(bytesOut)) {
                     out.write(bytes);
                  }
                  bytes = bytesOut.toByteArray();
               }
            } else if (coreType == org.apache.activemq.artemis.api.core.Message.STREAM_TYPE) {
               org.apache.activemq.util.ByteArrayOutputStream bytesOut = new org.apache.activemq.util.ByteArrayOutputStream();
               OutputStream out = bytesOut;
               if (isCompressed) {
                  out = new DeflaterOutputStream(bytesOut);
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
                  }
               }
               bytes = bytesOut.toByteArray();
            } else if (coreType == org.apache.activemq.artemis.api.core.Message.BYTES_TYPE) {
               int n = buffer.readableBytes();
               bytes = new byte[n];
               buffer.readBytes(bytes);
               if (isCompressed) {
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
                     ByteSequence byteSeq = compressed.toByteSequence();
                     ByteSequenceData.writeIntBig(byteSeq, length);
                     bytes = Arrays.copyOfRange(byteSeq.data, 0, byteSeq.length);
                  } finally {
                     deflater.end();
                  }
               }
            } else {
               int n = buffer.readableBytes();
               bytes = new byte[n];
               buffer.readBytes(bytes);
               if (isCompressed) {
                  try (ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
                       DeflaterOutputStream out = new DeflaterOutputStream(bytesOut)) {
                     out.write(bytes);
                     bytes = bytesOut.toByteArray();
                  }
               }
            }

            buffer.resetReaderIndex();// this is important for topics as the buffer
            // may be read multiple times
         }

         if (bytes != null) {
            ByteSequence content = new ByteSequence(bytes);
            amqMsg.setContent(content);
         }
      }

      //we need check null because messages may come from other clients
      //and those amq specific attribute may not be set.
      Long arrival = (Long) coreMessage.getObjectProperty(AMQ_MSG_ARRIVAL);
      if (arrival == null) {
         //messages from other sources (like core client) may not set this prop
         arrival = 0L;
      }
      amqMsg.setArrival(arrival);

      String brokerPath = (String) coreMessage.getObjectProperty(AMQ_MSG_BROKER_PATH);
      if (brokerPath != null && brokerPath.isEmpty()) {
         String[] brokers = brokerPath.split(",");
         BrokerId[] bids = new BrokerId[brokers.length];
         for (int i = 0; i < bids.length; i++) {
            bids[i] = new BrokerId(brokers[i]);
         }
         amqMsg.setBrokerPath(bids);
      }

      String clusterPath = (String) coreMessage.getObjectProperty(AMQ_MSG_CLUSTER);
      if (clusterPath != null && clusterPath.isEmpty()) {
         String[] cluster = clusterPath.split(",");
         BrokerId[] bids = new BrokerId[cluster.length];
         for (int i = 0; i < bids.length; i++) {
            bids[i] = new BrokerId(cluster[i]);
         }
         amqMsg.setCluster(bids);
      }

      Integer commandId = (Integer) coreMessage.getObjectProperty(AMQ_MSG_COMMAND_ID);
      if (commandId == null) {
         commandId = -1;
      }
      amqMsg.setCommandId(commandId);

      SimpleString corrId = (SimpleString) coreMessage.getObjectProperty("JMSCorrelationID");
      if (corrId != null) {
         amqMsg.setCorrelationId(corrId.toString());
      }

      byte[] dsBytes = (byte[]) coreMessage.getObjectProperty(AMQ_MSG_DATASTRUCTURE);
      if (dsBytes != null) {
         ByteSequence seq = new ByteSequence(dsBytes);
         DataStructure ds = (DataStructure) marshaller.unmarshal(seq);
         amqMsg.setDataStructure(ds);
      }

      amqMsg.setDestination(OpenWireUtil.toAMQAddress(coreMessage, actualDestination));

      Object value = coreMessage.getObjectProperty(AMQ_MSG_GROUP_ID);
      if (value != null) {
         String groupId = value.toString();
         amqMsg.setGroupID(groupId);
      }

      Integer groupSequence = (Integer) coreMessage.getObjectProperty(AMQ_MSG_GROUP_SEQUENCE);
      if (groupSequence == null) {
         groupSequence = -1;
      }
      amqMsg.setGroupSequence(groupSequence);

      MessageId mid = null;
      byte[] midBytes = (byte[]) coreMessage.getObjectProperty(AMQ_MSG_MESSAGE_ID);
      if (midBytes != null) {
         ByteSequence midSeq = new ByteSequence(midBytes);
         mid = (MessageId) marshaller.unmarshal(midSeq);
      } else {
         mid = new MessageId(UUIDGenerator.getInstance().generateStringUUID() + ":-1");
      }

      amqMsg.setMessageId(mid);

      byte[] origDestBytes = (byte[]) coreMessage.getObjectProperty(AMQ_MSG_ORIG_DESTINATION);
      if (origDestBytes != null) {
         ActiveMQDestination origDest = (ActiveMQDestination) marshaller.unmarshal(new ByteSequence(origDestBytes));
         amqMsg.setOriginalDestination(origDest);
      }

      byte[] origTxIdBytes = (byte[]) coreMessage.getObjectProperty(AMQ_MSG_ORIG_TXID);
      if (origTxIdBytes != null) {
         TransactionId origTxId = (TransactionId) marshaller.unmarshal(new ByteSequence(origTxIdBytes));
         amqMsg.setOriginalTransactionId(origTxId);
      }

      byte[] producerIdBytes = (byte[]) coreMessage.getObjectProperty(AMQ_MSG_PRODUCER_ID);
      if (producerIdBytes != null) {
         ProducerId producerId = (ProducerId) marshaller.unmarshal(new ByteSequence(producerIdBytes));
         amqMsg.setProducerId(producerId);
      }

      byte[] marshalledBytes = (byte[]) coreMessage.getObjectProperty(AMQ_MSG_MARSHALL_PROP);
      if (marshalledBytes != null) {
         amqMsg.setMarshalledProperties(new ByteSequence(marshalledBytes));
      }

      amqMsg.setRedeliveryCounter(reference.getDeliveryCount() - 1);

      byte[] replyToBytes = (byte[]) coreMessage.getObjectProperty(AMQ_MSG_REPLY_TO);
      if (replyToBytes != null) {
         ActiveMQDestination replyTo = (ActiveMQDestination) marshaller.unmarshal(new ByteSequence(replyToBytes));
         amqMsg.setReplyTo(replyTo);
      }

      String userId = (String) coreMessage.getObjectProperty(AMQ_MSG_USER_ID);
      if (userId != null) {
         amqMsg.setUserID(userId);
      }

      Boolean isDroppable = (Boolean) coreMessage.getObjectProperty(AMQ_MSG_DROPPABLE);
      if (isDroppable != null) {
         amqMsg.setDroppable(isDroppable);
      }

      SimpleString dlqCause = (SimpleString) coreMessage.getObjectProperty(AMQ_MSG_DLQ_DELIVERY_FAILURE_CAUSE_PROPERTY);
      if (dlqCause != null) {
         try {
            amqMsg.setStringProperty(ActiveMQMessage.DLQ_DELIVERY_FAILURE_CAUSE_PROPERTY, dlqCause.toString());
         } catch (JMSException e) {
            throw new IOException("failure to set dlq property " + dlqCause, e);
         }
      }

      Set<SimpleString> props = coreMessage.getPropertyNames();
      if (props != null) {
         for (SimpleString s : props) {
            String keyStr = s.toString();
            if (keyStr.startsWith("_AMQ") || keyStr.startsWith("__HDR_")) {
               continue;
            }
            Object prop = coreMessage.getObjectProperty(s);
            try {
               if (prop instanceof SimpleString) {
                  amqMsg.setObjectProperty(s.toString(), prop.toString());
               } else {
                  amqMsg.setObjectProperty(s.toString(), prop);
               }
            } catch (JMSException e) {
               throw new IOException("exception setting property " + s + " : " + prop, e);
            }
         }
      }
      try {
         amqMsg.onSend();
         amqMsg.setCompressed(isCompressed);
      } catch (JMSException e) {
         throw new IOException("Failed to covert to Openwire message", e);
      }
      return amqMsg;
   }

}
