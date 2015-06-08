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
package org.apache.activemq.bugs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TopicSubscriber;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.BrokerView;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(value = Parameterized.class)
public class AMQ2870Test extends org.apache.activemq.TestSupport  {

    static final Logger LOG = LoggerFactory.getLogger(AMQ2870Test.class);
    BrokerService broker = null;
    ActiveMQTopic topic;

    ActiveMQConnection consumerConnection = null, producerConnection = null;
    Session producerSession;
    MessageProducer producer;
    final int minPercentUsageForStore = 10;
    String data;

    private final PersistenceAdapterChoice persistenceAdapterChoice;

    @Parameterized.Parameters
    public static Collection<PersistenceAdapterChoice[]> getTestParameters() {
        String osName = System.getProperty("os.name");
        LOG.info("Running on [" + osName + "]");
        PersistenceAdapterChoice[] kahaDb = {PersistenceAdapterChoice.KahaDB};
        PersistenceAdapterChoice[] levelDb = {PersistenceAdapterChoice.LevelDB};
        List<PersistenceAdapterChoice[]> choices = new ArrayList<PersistenceAdapterChoice[]>();
        choices.add(kahaDb);
        if (!osName.equalsIgnoreCase("AIX") && !osName.equalsIgnoreCase("SunOS")) {
            choices.add(levelDb);
        }

        return choices;
    }

    public AMQ2870Test(PersistenceAdapterChoice choice) {
        this.persistenceAdapterChoice = choice;
    }

    @Test(timeout = 300000)
    public void testSize() throws Exception {
        openConsumer();

        assertEquals(0, broker.getAdminView().getStorePercentUsage());

        for (int i = 0; i < 5000; i++) {
            sendMessage(false);
        }

        final BrokerView brokerView = broker.getAdminView();

        // wait for reclaim
        assertTrue("in range with consumer",
                Wait.waitFor(new Wait.Condition() {
                    @Override
                    public boolean isSatisified() throws Exception {
                        // usage percent updated only on send check for isFull so once
                        // sends complete it is no longer updated till next send via a call to isFull
                        // this is optimal as it is only used to block producers
                        broker.getSystemUsage().getStoreUsage().isFull();
                        LOG.info("store percent usage: "+brokerView.getStorePercentUsage());
                        return broker.getAdminView().getStorePercentUsage() < minPercentUsageForStore;
                    }
                }));

        closeConsumer();

        assertTrue("in range with closed consumer",
                Wait.waitFor(new Wait.Condition() {
                    @Override
                    public boolean isSatisified() throws Exception {
                        broker.getSystemUsage().getStoreUsage().isFull();
                        LOG.info("store precent usage: "+brokerView.getStorePercentUsage());
                        return broker.getAdminView().getStorePercentUsage() < minPercentUsageForStore;
                    }
                }));

        for (int i = 0; i < 5000; i++) {
            sendMessage(false);
        }

        // What if i drop the subscription?
        broker.getAdminView().destroyDurableSubscriber("cliID", "subName");

        assertTrue("in range after send with consumer",
                Wait.waitFor(new Wait.Condition() {
                    @Override
                    public boolean isSatisified() throws Exception {
                        broker.getSystemUsage().getStoreUsage().isFull();
                        LOG.info("store precent usage: "+brokerView.getStorePercentUsage());
                        return broker.getAdminView().getStorePercentUsage() < minPercentUsageForStore;
                    }
                }));
    }

    private void openConsumer() throws Exception {
        consumerConnection = (ActiveMQConnection) createConnection();
        consumerConnection.setClientID("cliID");
        consumerConnection.start();
        Session session = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TopicSubscriber subscriber = session.createDurableSubscriber(topic, "subName", "filter=true", false);

        subscriber.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                // received++;
            }
        });
    }

    private void closeConsumer() throws JMSException {
        if (consumerConnection != null)
            consumerConnection.close();
        consumerConnection = null;
    }

    private void sendMessage(boolean filter) throws Exception {
        if (producerConnection == null) {
            producerConnection = (ActiveMQConnection) createConnection();
            producerConnection.start();
            producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            producer = producerSession.createProducer(topic);
        }

        Message message = producerSession.createMessage();
        message.setBooleanProperty("filter", filter);
        message.setStringProperty("data", data);
        producer.send(message);
    }

    private void startBroker(boolean deleteMessages) throws Exception {
        broker = new BrokerService();
        broker.setAdvisorySupport(false);
        broker.setBrokerName("testStoreSize");

        if (deleteMessages) {
            broker.setDeleteAllMessagesOnStartup(true);
        }
        LOG.info("Starting broker with persistenceAdapterChoice " + persistenceAdapterChoice.toString());
        setPersistenceAdapter(broker, persistenceAdapterChoice);
        configurePersistenceAdapter(broker.getPersistenceAdapter());
        broker.getSystemUsage().getStoreUsage().setLimit(100 * 1000 * 1000);
        broker.start();
    }

    private void configurePersistenceAdapter(PersistenceAdapter persistenceAdapter) {
        Properties properties = new Properties();
        String maxFileLengthVal = String.valueOf(2 * 1024 * 1024);
        properties.put("journalMaxFileLength", maxFileLengthVal);
        properties.put("maxFileLength", maxFileLengthVal);
        properties.put("cleanupInterval", "2000");
        properties.put("checkpointInterval", "2000");

        // leveldb
        properties.put("logSize", maxFileLengthVal);

        IntrospectionSupport.setProperties(persistenceAdapter, properties);
    }

    private void stopBroker() throws Exception {
        if (broker != null)
            broker.stop();
        broker = null;
    }

    @Override
    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory("vm://testStoreSize?jms.watchTopicAdvisories=false&waitForStart=5000&create=false");
    }

    @Override
    @Before
    public void setUp() throws Exception {
        StringBuilder sb = new StringBuilder(5000);
        for (int i = 0; i < 5000; i++) {
            sb.append('a');
        }
        data = sb.toString();

        startBroker(true);
        topic = (ActiveMQTopic) createDestination();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        stopBroker();
    }
}
