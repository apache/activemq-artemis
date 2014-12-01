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
package org.apache.activemq.api.jms;

/**
 * A JMSFactoryType
 *
 * @author howard
 *
 *
 */
// XXX no javadocs
public enum JMSFactoryType
{
   CF, QUEUE_CF, TOPIC_CF, XA_CF, QUEUE_XA_CF, TOPIC_XA_CF;

   public int intValue()
   {
      int val = 0;
      switch (this)
      {
         case CF:
            val = 0;
            break;
         case QUEUE_CF:
            val = 1;
            break;
         case TOPIC_CF:
            val = 2;
            break;
         case XA_CF:
            val = 3;
            break;
         case QUEUE_XA_CF:
            val = 4;
            break;
         case TOPIC_XA_CF:
            val = 5;
            break;
      }
      return val;
   }

   public static JMSFactoryType valueOf(int val)
   {
      JMSFactoryType type;
      switch (val)
      {
         case 0:
            type = CF;
            break;
         case 1:
            type = QUEUE_CF;
            break;
         case 2:
            type = TOPIC_CF;
            break;
         case 3:
            type = XA_CF;
            break;
         case 4:
            type = QUEUE_XA_CF;
            break;
         case 5:
            type = TOPIC_XA_CF;
            break;
         default:
            type = XA_CF;
            break;
      }
      return type;
   }
}
