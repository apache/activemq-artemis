/*
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
package org.apache.activemq.artemis.junit;

import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.jms.server.config.JMSConfiguration;
import org.apache.activemq.artemis.jms.server.embedded.EmbeddedJMS;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.BytesMessage;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Deprecated in favor of EmbeddedActiveMQResource. Since Artemis 2.0 all JMS specific broker management classes,
 * interfaces, and methods have been deprecated in favor of their more general counter-parts.
 * <p>
 * A JUnit Rule that embeds an ActiveMQ Artemis JMS server into a test.
 * <p>
 * This JUnit Rule is designed to simplify using embedded servers in unit tests.  Adding the rule to a test will startup
 * an embedded JMS server, which can then be used by client applications.
 *
 * <pre><code>
 * public class SimpleTest {
 *     &#64;Rule
 *     public EmbeddedJMSResource server = new EmbeddedJMSResource();
 *
 *     &#64;Test
 *     public void testSomething() throws Exception {
 *         // Use the embedded server here
 *     }
 * }
 * </code></pre>
 */
@Deprecated
public class EmbeddedJMSResource extends ExternalResource {

    Logger log = LoggerFactory.getLogger(this.getClass());

    private EmbeddedJMSDelegate embeddedJMSDelegate;

    /**
     * Create a default EmbeddedJMSResource
     */
    public EmbeddedJMSResource() {
        this.embeddedJMSDelegate = new EmbeddedJMSDelegate();
    }

    /**
     * Create a default EmbeddedJMSResource
     */
    public EmbeddedJMSResource(boolean useNetty) {
        this.embeddedJMSDelegate = new EmbeddedJMSDelegate(useNetty);
    }

    /**
     * The acceptor used
     */
    public EmbeddedJMSResource addAcceptor(String name, String uri) throws Exception {
        this.embeddedJMSDelegate = new EmbeddedJMSDelegate(name, uri);
        return this;
    }

    public static void setMessageProperties(Message message, Map<String, Object> properties) {
        EmbeddedJMSDelegate.setMessageProperties(message, properties);
    }

    public void start() {
        embeddedJMSDelegate.start();
    }

    public void stop() {
        embeddedJMSDelegate.stop();
    }

    public EmbeddedJMS getJmsServer() {
        return embeddedJMSDelegate.getJmsServer();
    }

    public String getServerName() {
        return embeddedJMSDelegate.getServerName();
    }

    public String getVmURL() {
        return embeddedJMSDelegate.getVmURL();
    }

    public Queue getDestinationQueue(String destinationName) {
        return embeddedJMSDelegate.getDestinationQueue(destinationName);
    }

    public List<Queue> getTopicQueues(String topicName) {
        return embeddedJMSDelegate.getTopicQueues(topicName);
    }

    public long getMessageCount(String destinationName) {
        return embeddedJMSDelegate.getMessageCount(destinationName);
    }

    public BytesMessage createBytesMessage() {
        return embeddedJMSDelegate.createBytesMessage();
    }

    public TextMessage createTextMessage() {
        return embeddedJMSDelegate.createTextMessage();
    }

    public MapMessage createMapMessage() {
        return embeddedJMSDelegate.createMapMessage();
    }

    public ObjectMessage createObjectMessage() {
        return embeddedJMSDelegate.createObjectMessage();
    }

    public StreamMessage createStreamMessage() {
        return embeddedJMSDelegate.createStreamMessage();
    }

    public BytesMessage createMessage(byte[] body) {
        return embeddedJMSDelegate.createMessage(body);
    }

    public TextMessage createMessage(String body) {
        return embeddedJMSDelegate.createMessage(body);
    }

    public MapMessage createMessage(Map<String, Object> body) {
        return embeddedJMSDelegate.createMessage(body);
    }

    public ObjectMessage createMessage(Serializable body) {
        return embeddedJMSDelegate.createMessage(body);
    }

    public BytesMessage createMessage(byte[] body, Map<String, Object> properties) {
        return embeddedJMSDelegate.createMessage(body, properties);
    }

    public TextMessage createMessage(String body, Map<String, Object> properties) {
        return embeddedJMSDelegate.createMessage(body, properties);
    }

    public MapMessage createMessage(Map<String, Object> body, Map<String, Object> properties) {
        return embeddedJMSDelegate.createMessage(body, properties);
    }

    public ObjectMessage createMessage(Serializable body, Map<String, Object> properties) {
        return embeddedJMSDelegate.createMessage(body, properties);
    }

    public void pushMessage(String destinationName, Message message) {
        embeddedJMSDelegate.pushMessage(destinationName, message);
    }

    public BytesMessage pushMessage(String destinationName, byte[] body) {
        return embeddedJMSDelegate.pushMessage(destinationName, body);
    }

    public TextMessage pushMessage(String destinationName, String body) {
        return embeddedJMSDelegate.pushMessage(destinationName, body);
    }

    public MapMessage pushMessage(String destinationName, Map<String, Object> body) {
        return embeddedJMSDelegate.pushMessage(destinationName, body);
    }

    public ObjectMessage pushMessage(String destinationName, Serializable body) {
        return embeddedJMSDelegate.pushMessage(destinationName, body);
    }

    public BytesMessage pushMessageWithProperties(String destinationName, byte[] body, Map<String, Object> properties) {
        return embeddedJMSDelegate.pushMessageWithProperties(destinationName, body, properties);
    }

    public TextMessage pushMessageWithProperties(String destinationName, String body, Map<String, Object> properties) {
        return embeddedJMSDelegate.pushMessageWithProperties(destinationName, body, properties);
    }

    public MapMessage pushMessageWithProperties(String destinationName, Map<String, Object> body, Map<String, Object> properties) {
        return embeddedJMSDelegate.pushMessageWithProperties(destinationName, body, properties);
    }

    public ObjectMessage pushMessageWithProperties(String destinationName, Serializable body, Map<String, Object> properties) {
        return embeddedJMSDelegate.pushMessageWithProperties(destinationName, body, properties);
    }

    public Message peekMessage(String destinationName) {
        return embeddedJMSDelegate.peekMessage(destinationName);
    }

    public BytesMessage peekBytesMessage(String destinationName) {
        return embeddedJMSDelegate.peekBytesMessage(destinationName);
    }

    public TextMessage peekTextMessage(String destinationName) {
        return embeddedJMSDelegate.peekTextMessage(destinationName);
    }

    public MapMessage peekMapMessage(String destinationName) {
        return embeddedJMSDelegate.peekMapMessage(destinationName);
    }

    public ObjectMessage peekObjectMessage(String destinationName) {
        return embeddedJMSDelegate.peekObjectMessage(destinationName);
    }

    public StreamMessage peekStreamMessage(String destinationName) {
        return embeddedJMSDelegate.peekStreamMessage(destinationName);
    }

    /**
     * Create a default EmbeddedJMSResource with the specified server id
     */
    public EmbeddedJMSResource(int serverId) {
        this.embeddedJMSDelegate = new EmbeddedJMSDelegate(serverId);
    }

    /**
     * Create an EmbeddedJMSResource with the specified configurations
     *
     * @param configuration    ActiveMQServer configuration
     * @param jmsConfiguration JMSServerManager configuration
     */
    public EmbeddedJMSResource(Configuration configuration, JMSConfiguration jmsConfiguration) {
        this.embeddedJMSDelegate = new EmbeddedJMSDelegate(configuration, jmsConfiguration);
    }

    /**
     * Create an EmbeddedJMSResource with the specified configuration file
     *
     * @param filename configuration file name
     */
    public EmbeddedJMSResource(String filename) {
        this.embeddedJMSDelegate = new EmbeddedJMSDelegate(filename);
    }

    /**
     * Create an EmbeddedJMSResource with the specified configuration file
     *
     * @param serverConfigurationFileName ActiveMQServer configuration file name
     * @param jmsConfigurationFileName    JMSServerManager configuration file name
     */
    public EmbeddedJMSResource(String serverConfigurationFileName, String jmsConfigurationFileName) {
        this.embeddedJMSDelegate = new EmbeddedJMSDelegate(serverConfigurationFileName, jmsConfigurationFileName);
    }

    /**
     * Start the embedded ActiveMQ Broker
     * <p>
     * Invoked by JUnit to setup the resource
     */
    @Override
    protected void before() throws Throwable {
        log.info("Starting {}: {}", this.getClass().getSimpleName(), embeddedJMSDelegate.getServerName());

        embeddedJMSDelegate.start();

        super.before();
    }

    /**
     * Stop the embedded ActiveMQ Broker
     * <p>
     * Invoked by JUnit to tear down the resource
     */
    @Override
    protected void after() {
        log.info("Stopping {}: {}", this.getClass().getSimpleName(), embeddedJMSDelegate.getServerName());

        super.after();

        embeddedJMSDelegate.stop();
    }

}
