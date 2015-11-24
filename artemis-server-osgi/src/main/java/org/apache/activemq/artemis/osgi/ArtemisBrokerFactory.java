/*
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
package org.apache.activemq.artemis.osgi;

import java.util.Dictionary;

import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.jboss.logging.Logger;
import org.osgi.framework.BundleContext;

public class ArtemisBrokerFactory extends BaseManagedServiceFactory<OsgiBroker, OsgiBroker> {

    public static final String PID = "org.apache.activemq.artemis";

    private static final Logger LOGGER = Logger.getLogger(ArtemisBrokerFactory.class);

    public ArtemisBrokerFactory(BundleContext context) {
        super(context, ArtemisBrokerFactory.class.getName());
    }

    @Override
    protected OsgiBroker doCreate(Dictionary<String, ?> properties) throws Exception {
        String config = getMandatory(properties, "config");
        String name = getMandatory(properties, "name");
        String domain = getMandatory(properties, "domain");

        ActiveMQJAASSecurityManager security = new ActiveMQJAASSecurityManager(domain);
        String serverInstanceDir = null;
        String karafDataDir = System.getProperty("karaf.data");
        if (karafDataDir != null) {
            serverInstanceDir = karafDataDir + "/artemis/" + name;
        }
        OsgiBroker server = new OsgiBroker(getContext(), name, serverInstanceDir, config, security);
        server.start();
        return server;
    }

    private String getMandatory(Dictionary<String, ?> properties, String key) {
        String value = (String) properties.get(key);
        if (value == null) {
            throw new IllegalStateException("Property " + key + " must be set");
        }
        return value;
    }

    @Override
    protected void doDestroy(OsgiBroker broker) throws Exception {
        broker.stop();
    }

    @Override
    protected OsgiBroker register(OsgiBroker broker, Dictionary<String, ?> properties) {
        broker.register(getContext(), properties);
        return broker;
    }

    @Override
    protected void unregister(OsgiBroker broker) {
        broker.unregister();
    }

    @Override
    protected void warn(String message, Throwable t) {
        LOGGER.warn(message, t);
    }

    @Override
    protected void info(String message, Throwable t) {
        LOGGER.info(message, t);
    }
}