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
package org.apache.activemq6.jms.client;

import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.ObjectMessage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.apache.activemq6.api.core.HornetQException;
import org.apache.activemq6.api.core.Message;
import org.apache.activemq6.api.core.client.ClientMessage;
import org.apache.activemq6.api.core.client.ClientSession;

/**
 * HornetQ implementation of a JMS ObjectMessage.
 * <br>
 * Don't used ObjectMessage if you want good performance!
 * <p>
 * Serialization is slooooow!
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 */
public class HornetQObjectMessage extends HornetQMessage implements ObjectMessage
{
   // Constants -----------------------------------------------------

   public static final byte TYPE = Message.OBJECT_TYPE;

   // Attributes ----------------------------------------------------

   // keep a snapshot of the Serializable Object as a byte[] to provide Object isolation
   private byte[] data;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   protected HornetQObjectMessage(final ClientSession session)
   {
      super(HornetQObjectMessage.TYPE, session);
   }

   protected HornetQObjectMessage(final ClientMessage message, final ClientSession session)
   {
      super(message, session);
   }

   /**
    * A copy constructor for foreign JMS ObjectMessages.
    */
   public HornetQObjectMessage(final ObjectMessage foreign, final ClientSession session) throws JMSException
   {
      super(foreign, HornetQObjectMessage.TYPE, session);

      setObject(foreign.getObject());
   }

   // Public --------------------------------------------------------

   @Override
   public byte getType()
   {
      return HornetQObjectMessage.TYPE;
   }

   @Override
   public void doBeforeSend() throws Exception
   {
      message.getBodyBuffer().clear();
      if (data != null)
      {
         message.getBodyBuffer().writeInt(data.length);
         message.getBodyBuffer().writeBytes(data);
      }

      super.doBeforeSend();
   }

   @Override
   public void doBeforeReceive() throws HornetQException
   {
      super.doBeforeReceive();
      try
      {
         int len = message.getBodyBuffer().readInt();
         data = new byte[len];
         message.getBodyBuffer().readBytes(data);
      }
      catch (Exception e)
      {
         data = null;
      }

   }

   // ObjectMessage implementation ----------------------------------

   public void setObject(final Serializable object) throws JMSException
   {
      checkWrite();

      if (object != null)
      {
         try
         {
            ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);

            ObjectOutputStream oos = new ObjectOutputStream(baos);

            oos.writeObject(object);

            oos.flush();

            data = baos.toByteArray();
         }
         catch (Exception e)
         {
            JMSException je = new JMSException("Failed to serialize object");
            je.setLinkedException(e);
            je.initCause(e);
            throw je;
         }
      }
   }

   // lazy deserialize the Object the first time the client requests it
   public Serializable getObject() throws JMSException
   {
      if (data == null || data.length == 0)
      {
         return null;
      }

      try
      {
         ByteArrayInputStream bais = new ByteArrayInputStream(data);
         ObjectInputStream ois = new org.apache.activemq6.utils.ObjectInputStreamWithClassLoader(bais);
         Serializable object = (Serializable)ois.readObject();
         return object;
      }
      catch (Exception e)
      {
         JMSException je = new JMSException(e.getMessage());
         je.setStackTrace(e.getStackTrace());
         throw je;
      }
   }

   @Override
   public void clearBody() throws JMSException
   {
      super.clearBody();

      data = null;
   }

   @Override
   protected <T> T getBodyInternal(Class<T> c) throws MessageFormatException
   {
      try
      {
         return (T)getObject();
      }
      catch (JMSException e)
      {
         throw new MessageFormatException("Deserialization error on HornetQObjectMessage");
      }
   }

   @Override
   public boolean isBodyAssignableTo(@SuppressWarnings("rawtypes")
                                     Class c)
   {
      if (data == null) // we have no body
         return true;
      try
      {
         return Serializable.class == c || Object.class == c || c.isInstance(getObject());
      }
      catch (JMSException e)
      {
         return false;
      }
   }
}
