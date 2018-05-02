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
package org.apache.activemq.artemis.cli.commands.tools.xml;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.utils.Base64;
import org.apache.activemq.artemis.utils.UUIDGenerator;
import org.jboss.logging.Logger;

/** This is an Utility class that will import the outputs in XML format. */
public class XMLMessageImporter {

   private static final Logger logger = Logger.getLogger(XMLMessageImporter.class);

   private XMLStreamReader reader;

   private ClientSession session;

   Map<String, String> oldPrefixTranslation = new HashMap<>();

   public XMLMessageImporter(XMLStreamReader xmlStreamReader, ClientSession session) {
      this.reader = xmlStreamReader;
      this.session = session;
   }

   public void setOldPrefixTranslation(Map<String, String> oldPrefixTranslation) {
      this.oldPrefixTranslation = oldPrefixTranslation;
   }

   public XMLStreamReader getRawXMLReader() {
      return reader;
   }

   public MessageInfo readMessage(boolean decodeUTF8) throws Exception {
      if (!reader.hasNext()) return null;

      Byte type = 0;
      Byte priority = 0;
      Long expiration = 0L;
      Long timestamp = 0L;
      Long id = 0L;
      org.apache.activemq.artemis.utils.UUID userId = null;
      ArrayList<String> queues = new ArrayList<>();

      // get message's attributes
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String attributeName = reader.getAttributeLocalName(i);
         switch (attributeName) {
            case XmlDataConstants.MESSAGE_TYPE:
               type = getMessageType(reader.getAttributeValue(i));
               break;
            case XmlDataConstants.MESSAGE_PRIORITY:
               priority = Byte.parseByte(reader.getAttributeValue(i));
               break;
            case XmlDataConstants.MESSAGE_EXPIRATION:
               expiration = Long.parseLong(reader.getAttributeValue(i));
               break;
            case XmlDataConstants.MESSAGE_TIMESTAMP:
               timestamp = Long.parseLong(reader.getAttributeValue(i));
               break;
            case XmlDataConstants.MESSAGE_USER_ID:
               userId = UUIDGenerator.getInstance().generateUUID();
               break;
            case XmlDataConstants.MESSAGE_ID:
               id = Long.parseLong(reader.getAttributeValue(i));
               break;
         }
      }

      Message message = session.createMessage(type, true, expiration, timestamp, priority);

      message.setUserID(userId);

      boolean endLoop = false;

      File largeMessageTemporaryFile = null;
      // loop through the XML and gather up all the message's data (i.e. body, properties, queues, etc.)
      while (reader.hasNext()) {
         int eventType = reader.getEventType();
         switch (eventType) {
            case XMLStreamConstants.START_ELEMENT:
               if (XmlDataConstants.MESSAGE_BODY.equals(reader.getLocalName())) {
                  largeMessageTemporaryFile = processMessageBody(message.toCore(), decodeUTF8);
               } else if (XmlDataConstants.PROPERTIES_CHILD.equals(reader.getLocalName())) {
                  processMessageProperties(message);
               } else if (XmlDataConstants.QUEUES_CHILD.equals(reader.getLocalName())) {
                  processMessageQueues(queues);
               }
               break;
            case XMLStreamConstants.END_ELEMENT:
               if (XmlDataConstants.MESSAGES_CHILD.equals(reader.getLocalName())) {
                  endLoop = true;
               }
               break;
         }
         if (endLoop) {
            break;
         }
         reader.next();
      }
      return new MessageInfo(id, queues, message, largeMessageTemporaryFile);
   }

   private Byte getMessageType(String value) {
      Byte type = Message.DEFAULT_TYPE;
      switch (value) {
         case XmlDataConstants.DEFAULT_TYPE_PRETTY:
            type = Message.DEFAULT_TYPE;
            break;
         case XmlDataConstants.BYTES_TYPE_PRETTY:
            type = Message.BYTES_TYPE;
            break;
         case XmlDataConstants.MAP_TYPE_PRETTY:
            type = Message.MAP_TYPE;
            break;
         case XmlDataConstants.OBJECT_TYPE_PRETTY:
            type = Message.OBJECT_TYPE;
            break;
         case XmlDataConstants.STREAM_TYPE_PRETTY:
            type = Message.STREAM_TYPE;
            break;
         case XmlDataConstants.TEXT_TYPE_PRETTY:
            type = Message.TEXT_TYPE;
            break;
      }
      return type;
   }

   private void processMessageQueues(ArrayList<String> queues) {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         if (XmlDataConstants.QUEUE_NAME.equals(reader.getAttributeLocalName(i))) {
            String queueName = reader.getAttributeValue(i);
            String translation = checkPrefix(queueName);
            queues.add(translation);
         }
      }
   }

   private String checkPrefix(String queueName) {
      String newQueueName = oldPrefixTranslation.get(queueName);
      if (newQueueName == null) {
         newQueueName = queueName;
      }
      return newQueueName;
   }

   private void processMessageProperties(Message message) {
      String key = "";
      String value = "";
      String propertyType = "";

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String attributeName = reader.getAttributeLocalName(i);
         switch (attributeName) {
            case XmlDataConstants.PROPERTY_NAME:
               key = reader.getAttributeValue(i);
               break;
            case XmlDataConstants.PROPERTY_VALUE:
               value = reader.getAttributeValue(i);
               break;
            case XmlDataConstants.PROPERTY_TYPE:
               propertyType = reader.getAttributeValue(i);
               break;
         }
      }

      if (value.equals(XmlDataConstants.NULL)) {
         value = null;
      }

      switch (propertyType) {
         case XmlDataConstants.PROPERTY_TYPE_SHORT:
            message.putShortProperty(key, Short.parseShort(value));
            break;
         case XmlDataConstants.PROPERTY_TYPE_BOOLEAN:
            message.putBooleanProperty(key, Boolean.parseBoolean(value));
            break;
         case XmlDataConstants.PROPERTY_TYPE_BYTE:
            message.putByteProperty(key, Byte.parseByte(value));
            break;
         case XmlDataConstants.PROPERTY_TYPE_BYTES:
            message.putBytesProperty(key, value == null ? null : decode(value));
            break;
         case XmlDataConstants.PROPERTY_TYPE_DOUBLE:
            message.putDoubleProperty(key, Double.parseDouble(value));
            break;
         case XmlDataConstants.PROPERTY_TYPE_FLOAT:
            message.putFloatProperty(key, Float.parseFloat(value));
            break;
         case XmlDataConstants.PROPERTY_TYPE_INTEGER:
            message.putIntProperty(key, Integer.parseInt(value));
            break;
         case XmlDataConstants.PROPERTY_TYPE_LONG:
            message.putLongProperty(key, Long.parseLong(value));
            break;
         case XmlDataConstants.PROPERTY_TYPE_SIMPLE_STRING:
            message.putStringProperty(new SimpleString(key), value == null ? null : SimpleString.toSimpleString(value));
            break;
         case XmlDataConstants.PROPERTY_TYPE_STRING:
            message.putStringProperty(key, value);
            break;
      }
   }

   private File processMessageBody(final ICoreMessage message, boolean decodeTextMessage) throws XMLStreamException, IOException {
      File tempFileName = null;
      boolean isLarge = false;

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String attributeName = reader.getAttributeLocalName(i);
         if (XmlDataConstants.MESSAGE_IS_LARGE.equals(attributeName)) {
            isLarge = Boolean.parseBoolean(reader.getAttributeValue(i));
         }
      }
      reader.next();
      if (logger.isDebugEnabled()) {
         logger.debug("XMLStreamReader impl: " + reader);
      }
      if (isLarge) {
         tempFileName = File.createTempFile("largeMessage", ".tmp");
         if (logger.isDebugEnabled()) {
            logger.debug("Creating temp file " + tempFileName + " for large message.");
         }
         try (OutputStream out = new BufferedOutputStream(new FileOutputStream(tempFileName))) {
            getMessageBodyBytes(bytes -> out.write(bytes), (message.toCore().getType() == Message.TEXT_TYPE) && decodeTextMessage);
         }
         FileInputStream fileInputStream = new FileInputStream(tempFileName);
         BufferedInputStream bufferedInput = new BufferedInputStream(fileInputStream);
         ((ClientMessage) message).setBodyInputStream(bufferedInput);
      } else {
         getMessageBodyBytes(bytes -> message.getBodyBuffer().writeBytes(bytes), (message.toCore().getType() == Message.TEXT_TYPE) && decodeTextMessage);
      }

      return tempFileName;
   }

   /**
    * Message bodies are written to XML as one or more Base64 encoded CDATA elements. Some parser implementations won't
    * read an entire CDATA element at once (e.g. Woodstox) so it's possible that multiple CDATA/CHARACTERS events need
    * to be combined to reconstruct the Base64 encoded string.  You can't decode bits and pieces of each CDATA.  Each
    * CDATA has to be decoded in its entirety.
    *
    * @param processor used to deal with the decoded CDATA elements
    * @param textMessage If this a text message we decode UTF8 and encode as a simple string
    */
   private void getMessageBodyBytes(MessageBodyBytesProcessor processor, boolean decodeTextMessage) throws IOException, XMLStreamException {
      int currentEventType;
      StringBuilder cdata = new StringBuilder();
      while (reader.hasNext()) {
         currentEventType = reader.getEventType();
         if (currentEventType == XMLStreamConstants.END_ELEMENT) {
            break;
         } else if (currentEventType == XMLStreamConstants.CHARACTERS && reader.isWhiteSpace() && cdata.length() > 0) {
            /* when we hit a whitespace CHARACTERS event we know that the entire CDATA is complete so decode, pass back to
             * the processor, and reset the cdata for the next event(s)
             */
            if (decodeTextMessage) {
               SimpleString text = new SimpleString(cdata.toString());
               ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer(SimpleString.sizeofNullableString(text));
               SimpleString.writeNullableSimpleString(byteBuf, text);
               byte[] bytes = new byte[SimpleString.sizeofNullableString(text)];
               byteBuf.readBytes(bytes);
               processor.processBodyBytes(bytes);
            } else {
               processor.processBodyBytes(decode(cdata.toString()));
               cdata.setLength(0);
            }
         } else {
            cdata.append(new String(reader.getTextCharacters(), reader.getTextStart(), reader.getTextLength()).trim());
         }
         reader.next();
      }
   }

   private static byte[] decode(String data) {
      return Base64.decode(data, Base64.DONT_BREAK_LINES | Base64.URL_SAFE);
   }

   private interface MessageBodyBytesProcessor {
      void processBodyBytes(byte[] bytes) throws IOException;
   }

   public class MessageInfo {
      public long id;
      public List<String> queues;
      public Message message;
      public File tempFile;

      MessageInfo(long id, List<String> queues, Message message, File tempFile) {
         this.message = message;
         this.queues = queues;
         this.id = id;
         this.tempFile = tempFile;
      }
   }
}
