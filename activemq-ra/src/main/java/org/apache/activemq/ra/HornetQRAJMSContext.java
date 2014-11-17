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
package org.apache.activemq.ra;

import org.apache.activemq.jms.client.HornetQConnectionForContext;
import org.apache.activemq.jms.client.HornetQJMSContext;
import org.apache.activemq.jms.client.ThreadAwareContext;

import javax.jms.ExceptionListener;
import javax.jms.JMSContext;

public class HornetQRAJMSContext extends HornetQJMSContext
{
   public HornetQRAJMSContext(HornetQConnectionForContext connection, int ackMode, ThreadAwareContext threadAwareContext)
   {
      super(connection, ackMode, threadAwareContext);
   }

   public HornetQRAJMSContext(HornetQConnectionForContext connection, ThreadAwareContext threadAwareContext)
   {
      super(connection, threadAwareContext);
   }

   @Override
   public JMSContext createContext(int sessionMode)
   {
      throw HornetQRABundle.BUNDLE.illegalJEEMethod();
   }

   @Override
   public void stop()
   {
      throw HornetQRABundle.BUNDLE.illegalJEEMethod();
   }

   @Override
   public void setClientID(String clientID)
   {
      throw HornetQRABundle.BUNDLE.illegalJEEMethod();
   }

   @Override
   public void setExceptionListener(ExceptionListener listener)
   {
      throw HornetQRABundle.BUNDLE.illegalJEEMethod();
   }
}
