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
package org.apache.activemq.jms.tests.stress;

import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.XASession;
import javax.transaction.xa.XAResource;

import org.apache.activemq.core.transaction.impl.XidImpl;
import org.apache.activemq.jms.tests.JmsTestLogger;
import org.apache.activemq.utils.UUIDGenerator;

/**
 *
 * A Sender that sends messages to a destination in an XA transaction
 *
 * Sends messages to a destination in a jms transaction.
 * Sends <commitSize> messages then prepares, commits, then
 * sends <rollbackSize> messages then prepares, rollsback until
 * a total of <numMessages> messages have been sent (commitSize)
 * <nuMessages> must be a multiple of <commitSize>
 *
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class Transactional2PCSender extends Sender
{
   private static final JmsTestLogger log = JmsTestLogger.LOGGER;

   protected int commitSize;

   protected int rollbackSize;

   protected XAResource xaResource;

   public Transactional2PCSender(final String prodName,
                                 final XASession sess,
                                 final MessageProducer prod,
                                 final int numMessages,
                                 final int commitSize,
                                 final int rollbackSize)
   {
      super(prodName, sess, prod, numMessages);

      this.commitSize = commitSize;
      this.rollbackSize = rollbackSize;
      xaResource = sess.getXAResource();
   }

   @Override
   public void run()
   {

      int iterations = numMessages / commitSize;

      try
      {
         for (int outerCount = 0; outerCount < iterations; outerCount++)
         {
            XidImpl xid = null;
            if (commitSize > 0)
            {
               xid = new XidImpl("bq1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());
               xaResource.start(xid, XAResource.TMNOFLAGS);
            }
            for (int innerCount = 0; innerCount < commitSize; innerCount++)
            {
               Message m = sess.createMessage();
               m.setStringProperty("PROD_NAME", prodName);
               m.setIntProperty("MSG_NUMBER", outerCount * commitSize + innerCount);
               prod.send(m);
            }
            if (commitSize > 0)
            {
               xaResource.end(xid, XAResource.TMSUCCESS);
               xaResource.prepare(xid);
               xaResource.commit(xid, false);
            }
            if (rollbackSize > 0)
            {
               xid = new XidImpl("bq1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());
               xaResource.start(xid, XAResource.TMNOFLAGS);
            }
            for (int innerCount = 0; innerCount < rollbackSize; innerCount++)
            {
               Message m = sess.createMessage();
               m.setStringProperty("PROD_NAME", prodName);
               m.setIntProperty("MSG_NUMBER", (outerCount + 1) * commitSize + innerCount);
               prod.send(m);
            }
            if (rollbackSize > 0)
            {
               xaResource.end(xid, XAResource.TMSUCCESS);
               xaResource.prepare(xid);
               xaResource.rollback(xid);
            }
         }
      }
      catch (Exception e)
      {
         Transactional2PCSender.log.error("Failed to send message", e);
         setFailed(true);
      }
   }
}
