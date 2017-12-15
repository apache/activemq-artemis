<!--
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#
-->
# CDI Example

This is a simple example that demonstrates how to use CDI to integrate with ActiveMQ Artemis on the client side.  It is designed mainly for connecting to a remote broker rather than embedding within your application.

## Configuring the connection

While the integration provides an out of the box solution for configuration with some sensible defaults, the values should be configurable.  This example leverages [Apache DeltaSpike](https://deltaspike.apache.org) to configure the connectivity.  It overrides the username, password and URL for the broker.  The configuration hard codes the connector class.  This configuration class is a standard CDI bean.

```
@ApplicationScoped
public class CDIClientConfig implements ArtemisClientConfiguration {
   @Inject
   @ConfigProperty(name = "username")
   private String username;

   @Inject
   @ConfigProperty(name = "password")
   private String password;

   @Inject
   @ConfigProperty(name = "url")
   private String url;

   @Override
   public String getUsername() {
      return username;
   }

   @Override
   public String getPassword() {
      return password;
   }

   @Override
   public String getUrl() {
      return url;
   }

   @Override
   public String getConnectorFactory() {
      return "org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnectorFactory";
   }
}
```

## Setup and Tear Down

For the example, we leverage DeltaSpike's Container Control to start and stop the container.  It is implemented within the main method, and is done in a way to make this work for both Weld and OpenWebBeans.

## Sending and Receiving Messages

The key to how the CDI integration works is the built in beans.  It provides two out of the box - a `ConnectionFactory` and a `JMSContext`.  Most operations should be performed against the JMS 2.0 simplified API using `JMSContext`.  The example does this via an observer method, but can be done anywhere.

```
@ApplicationScoped
public class CDIMessagingIntegrator {
   @Inject
   private JMSContext context;
   public void init(@Observes @Initialized(ApplicationScoped.class) Object obj) {
      String body = "This is a test";
      Queue queue = context.createQueue("test");
      context.createProducer().send(queue, body);
      String receivedBody = context.createConsumer(queue).receiveBody(String.class, 5000);
      System.out.println("Received a message "+receivedBody);
   }
}
```