/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.store.kahadb;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.concurrent.atomic.AtomicLong;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Ignore;
import org.junit.Test;

public class NoSpaceIOTest {
    private static final Logger LOG = LoggerFactory.getLogger(NoSpaceIOTest.class);

    // need an app to input to console in intellij idea
    public static void main(String[] args) throws Exception {
       new NoSpaceIOTest().testRunOutOfSpace();
    }

    // handy way to validate some out of space related errors with a usb key
    // allow it to run out of space, delete toDelete and see it recover
    @Ignore("needs small volume, like usb key")
    @Test
    public void testRunOutOfSpace() throws Exception {
        BrokerService broker = new BrokerService();
        File dataDir = new File("/Volumes/NO NAME/");
        File useUpSpace = new File(dataDir, "bigFile");
        if (!useUpSpace.exists()) {
            LOG.info("using up some space...");
            RandomAccessFile filler = new RandomAccessFile(useUpSpace, "rw");
            filler.setLength(1024*1024*1212); // use ~1.xG of 2G (usb) volume
            filler.close();
            File toDelete = new File(dataDir, "toDelete");
            filler = new RandomAccessFile(toDelete, "rw");
            filler.setLength(1024*1024*32*10); // 10 data files
            filler.close();
        }
        broker.setDataDirectoryFile(dataDir);
        broker.start();
        AtomicLong consumed = new AtomicLong(0);
        consume(consumed);
        LOG.info("consumed: " + consumed);

        broker.getPersistenceAdapter().checkpoint(true);

        AtomicLong sent = new AtomicLong(0);
        try {
            produce(sent, 200);
        } catch (Exception expected) {
            LOG.info("got ex, sent: " + sent);
        }
        LOG.info("sent: " + sent);
        System.out.println("Remove toDelete file and press any key to continue");
        int read = System.in.read();
        System.err.println("read:" + read);

        LOG.info("Trying to send again: " + sent);
        try {
            produce(sent, 200);
        } catch (Exception expected) {
            LOG.info("got ex, sent: " + sent);
        }
        LOG.info("sent: " + sent);
    }

    private void consume(AtomicLong consumed) throws JMSException {
        Connection c = new ActiveMQConnectionFactory("vm://localhost").createConnection();
        try {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = s.createConsumer(new ActiveMQQueue("t"));
            while (consumer.receive(2000) != null) {
                consumed.incrementAndGet();
            }
        } finally {
            c.close();
        }
    }

    private void produce(AtomicLong sent, long toSend) throws JMSException {
        Connection c = new ActiveMQConnectionFactory("vm://localhost").createConnection();
        try {
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = s.createProducer(new ActiveMQQueue("t"));
            TextMessage m = s.createTextMessage();
            m.setText(String.valueOf(new char[1024*1024]));
            for (int i=0; i<toSend; i++) {
                producer.send(m);
                sent.incrementAndGet();
            }
        } finally {
            c.close();
        }
    }
}
