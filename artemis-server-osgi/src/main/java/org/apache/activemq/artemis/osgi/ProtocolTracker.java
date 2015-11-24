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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.activemq.artemis.api.core.Interceptor;
import org.apache.activemq.artemis.spi.core.protocol.ProtocolManagerFactory;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.util.tracker.ServiceTrackerCustomizer;

/**
 * Tracks the available ProtocolManagerFactory services as well as the required protocols.
 * When a new service appears the factory is added to the server.
 * When all needed protocols are present the server is started.
 * When required a service disappears the server is stopped. 
 */
@SuppressWarnings("rawtypes")
public class ProtocolTracker implements ServiceTrackerCustomizer<ProtocolManagerFactory<Interceptor>, ProtocolManagerFactory<Interceptor>> {
    private static Logger LOG = Logger.getLogger(ProtocolTracker.class.getName());
    private String name;
    private BundleContext context;
    private Map<String, Boolean> protocols;
    private ProtocolTrackerCallBack callback;

    public ProtocolTracker(String name, BundleContext context, String[] requiredProtocols, ProtocolTrackerCallBack callback) {
        this.name = name;
        this.context = context;
        this.callback = callback;
        this.protocols = new HashMap<String, Boolean>();
        for (String requiredProtocol : requiredProtocols) {
            this.protocols.put(requiredProtocol, false);
        }
        LOG.info("Broker config " + name + " found. Tracking protocols " + Arrays.asList(requiredProtocols));
    }

    @Override
    public ProtocolManagerFactory addingService(ServiceReference<ProtocolManagerFactory<Interceptor>> reference) {
        ProtocolManagerFactory<Interceptor> pmf = context.getService(reference);
        callback.addFactory(pmf);
        for (String protocol : pmf.getProtocols()) {
            protocolAdded(protocol);
        }

        return pmf;
    }


    @Override
    public void modifiedService(ServiceReference<ProtocolManagerFactory<Interceptor>> reference, ProtocolManagerFactory<Interceptor> pmf) {
        // Not supported
    }

    @Override
    public void removedService(ServiceReference<ProtocolManagerFactory<Interceptor>> reference, ProtocolManagerFactory<Interceptor> pmf) {
        for (String protocol : pmf.getProtocols()) {
            protocolRemoved(protocol);
        }
        callback.removeFactory(pmf);
    }
 
    private void protocolAdded(String protocol) {
        Boolean present = this.protocols.get(protocol);
        if (present != null && !present) {
            this.protocols.put(protocol, true);
            List<String> missing = getMissing();
            LOG.info("Required protocol " + protocol + " was added for broker " + name + ". " + 
                (missing.isEmpty() ? "Starting broker." : "Still waiting for " + missing));
            if (missing.isEmpty()) {
                try {
                    callback.start();
                } catch (Exception e) {
                    LOG.log(Level.WARNING, "Error starting broker " + name, e);
                }
            }
        }
    }
    

    private void protocolRemoved(String protocol) {
        Boolean present = this.protocols.get(protocol);
        if (present != null && present) {
            List<String> missing = getMissing();
            LOG.info("Required protocol " + protocol + " was removed for broker " + name + ". " 
                     + (missing.isEmpty() ? "Stopping broker. " : ""));
            if (missing.isEmpty()) {
                try {
                    callback.stop();
                } catch (Exception e) {
                    LOG.log(Level.WARNING, "Error stopping broker " + name, e);
                }
            }
            this.protocols.put(protocol, false);
        }
    }

    private List<String> getMissing() {
        List<String> missing = new ArrayList<String>();
        for (String protocol : protocols.keySet()) {
            Boolean present = protocols.get(protocol);
            if (!present) {
                missing.add(protocol);
            }
        }
        return missing;
    }

}
