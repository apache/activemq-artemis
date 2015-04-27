/**
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
package org.apache.activemq.tests.integration.stomp.util;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public abstract class AbstractClientStompFrame implements ClientStompFrame
{
   protected static final String HEADER_RECEIPT = "receipt";

   protected static final Set<String> validCommands = new HashSet<String>();
   protected String command;
   protected List<Header> headers = new ArrayList<Header>();
   protected Set<String> headerKeys = new HashSet<String>();
   protected String body;
   protected String EOL = "\n";
   static
   {
      validCommands.add("CONNECT");
      validCommands.add("CONNECTED");
      validCommands.add("SEND");
      validCommands.add("RECEIPT");
      validCommands.add("SUBSCRIBE");
      validCommands.add("UNSUBSCRIBE");
      validCommands.add("MESSAGE");
      validCommands.add("BEGIN");
      validCommands.add("COMMIT");
      validCommands.add("ABORT");
      validCommands.add("ACK");
      validCommands.add("DISCONNECT");
      validCommands.add("ERROR");
   }

   public AbstractClientStompFrame(String command)
   {
      this(command, true);
   }

   public AbstractClientStompFrame(String command, boolean validate)
   {
      if (validate && (!validate(command))) throw new IllegalArgumentException("Invalid command " + command);
      this.command = command;
   }

   public boolean validate(String command)
   {
      return validCommands.contains(command);
   }

   public String toString()
   {
      StringBuffer sb = new StringBuffer("Frame: <" + command + ">" + "\n");
      Iterator<Header> iter = headers.iterator();
      while (iter.hasNext())
      {
         Header h = iter.next();
         sb.append(h.key + ":" + h.val + "\n");
      }
      sb.append("\n");
      sb.append("<body>" + body + "<body>");
      return sb.toString();
   }

   @Override
   public ByteBuffer toByteBuffer()
   {
      if (isPing())
      {
         ByteBuffer buffer = ByteBuffer.allocateDirect(1);
         buffer.put((byte)0x0A);
         buffer.rewind();
         return buffer;
      }
      StringBuffer sb = new StringBuffer();
      sb.append(command + EOL);
      int n = headers.size();
      for (int i = 0; i < n; i++)
      {
         sb.append(headers.get(i).key + ":" + headers.get(i).val + EOL);
      }
      sb.append(EOL);
      if (body != null)
      {
         sb.append(body);
      }
      sb.append((char)0);

      String data = sb.toString();

      byte[] byteValue = data.getBytes(StandardCharsets.UTF_8);

      ByteBuffer buffer = ByteBuffer.allocateDirect(byteValue.length);
      buffer.put(byteValue);

      buffer.rewind();
      return buffer;
   }

   @Override
   public ByteBuffer toByteBufferWithExtra(String str)
   {
      StringBuffer sb = new StringBuffer();
      sb.append(command + EOL);
      int n = headers.size();
      for (int i = 0; i < n; i++)
      {
         sb.append(headers.get(i).key + ":" + headers.get(i).val + EOL);
      }
      sb.append(EOL);
      if (body != null)
      {
         sb.append(body);
      }
      sb.append((char)0);
      sb.append(str);

      String data = sb.toString();

      byte[] byteValue = data.getBytes(StandardCharsets.UTF_8);

      ByteBuffer buffer = ByteBuffer.allocateDirect(byteValue.length);
      buffer.put(byteValue);

      buffer.rewind();
      return buffer;
   }

   @Override
   public boolean needsReply()
   {
      if ("CONNECT".equals(command) || headerKeys.contains(HEADER_RECEIPT))
      {
         return true;
      }
      return false;
   }

   @Override
   public void setCommand(String command)
   {
      this.command = command;
   }

   @Override
   public void addHeader(String key, String val)
   {
      headers.add(new Header(key, val));
      headerKeys.add(key);
   }

   @Override
   public void setBody(String body)
   {
      this.body = body;
   }

   @Override
   public String getBody()
   {
      return body;
   }

   private class Header
   {
      public String key;
      public String val;

      public Header(String key, String val)
      {
         this.key = key;
         this.val = val;
      }
   }

   @Override
   public String getCommand()
   {
      return command;
   }

   @Override
   public String getHeader(String header)
   {
      if (headerKeys.contains(header))
      {
         Iterator<Header> iter = headers.iterator();
         while (iter.hasNext())
         {
            Header h = iter.next();
            if (h.key.equals(header))
            {
               return h.val;
            }
         }
      }
      return null;
   }

}
