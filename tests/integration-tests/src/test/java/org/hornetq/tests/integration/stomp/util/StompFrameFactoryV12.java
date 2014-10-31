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
package org.hornetq.tests.integration.stomp.util;

import java.util.StringTokenizer;

public class StompFrameFactoryV12 implements StompFrameFactory
{

   @Override
   public ClientStompFrame createFrame(String data)
   {
      //split the string at "\n\n"
      String[] dataFields = data.split("\n\n");

      StringTokenizer tokenizer = new StringTokenizer(dataFields[0], "\n");

      String command = tokenizer.nextToken();
      ClientStompFrame frame = new ClientStompFrameV12(command);

      while (tokenizer.hasMoreTokens())
      {
         String header = tokenizer.nextToken();
         String[] fields = splitHeader(header);
         frame.addHeader(fields[0], fields[1]);
      }

      //body (without null byte)
      if (dataFields.length == 2)
      {
         frame.setBody(dataFields[1]);
      }
      return frame;
   }

   public void printByteHeader(String headers)
   {
      StringBuffer buffer = new StringBuffer();

      for (int i = 0; i < headers.length(); i++)
      {
         char c = headers.charAt(i);
         buffer.append((byte) c + " ");
      }
      System.out.println("header in byte : " + buffer.toString());
   }

   private String[] splitHeader(String header)
   {
      StringBuffer sbKey = new StringBuffer();
      StringBuffer sbVal = new StringBuffer();
      boolean isEsc = false;
      boolean isKey = true;

      for (int i = 0; i < header.length(); i++)
      {
         char b = header.charAt(i);

         switch (b)
         {
            //escaping
            case '\\':
            {
               if (isEsc)
               {
                  //this is a backslash
                  if (isKey)
                  {
                     sbKey.append(b);
                  }
                  else
                  {
                     sbVal.append(b);
                  }
                  isEsc = false;
               }
               else
               {
                  //begin escaping
                  isEsc = true;
               }
               break;
            }
            case ':':
            {
               if (isEsc)
               {
                  if (isKey)
                  {
                     sbKey.append(b);
                  }
                  else
                  {
                     sbVal.append(b);
                  }
                  isEsc = false;
               }
               else
               {
                  isKey = false;
               }
               break;
            }
            case 'n':
            {
               if (isEsc)
               {
                  if (isKey)
                  {
                     sbKey.append('\n');
                  }
                  else
                  {
                     sbVal.append('\n');
                  }
                  isEsc = false;
               }
               else
               {
                  if (isKey)
                  {
                     sbKey.append(b);
                  }
                  else
                  {
                     sbVal.append(b);
                  }
               }
               break;
            }
            case 'r':
            {
               if (isEsc)
               {
                  if (isKey)
                  {
                     sbKey.append('\r');
                  }
                  else
                  {
                     sbVal.append('\r');
                  }
                  isEsc = false;
               }
               else
               {
                  if (isKey)
                  {
                     sbKey.append(b);
                  }
                  else
                  {
                     sbVal.append(b);
                  }
               }
               break;
            }
            default:
            {
               if (isKey)
               {
                  sbKey.append(b);
               }
               else
               {
                  sbVal.append(b);
               }
            }
         }
      }
      String[] result = new String[2];
      result[0] = sbKey.toString();
      result[1] = sbVal.toString();

      return result;
   }

   @Override
   public ClientStompFrame newFrame(String command)
   {
      return new ClientStompFrameV12(command);
   }

   @Override
   public ClientStompFrame newAnyFrame(String command)
   {
      return new ClientStompFrameV12(command, true, false);
   }

}
