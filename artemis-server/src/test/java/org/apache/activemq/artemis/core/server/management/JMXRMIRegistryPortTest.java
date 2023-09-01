package org.apache.activemq.artemis.core.server.management;

import org.apache.activemq.artemis.core.config.JMXConnectorConfiguration;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;

public class JMXRMIRegistryPortTest {

    @Test
    public void explicitLocalhostRegistry() throws IOException {
        RmiRegistryFactory registryFactory = new RmiRegistryFactory();
        registryFactory.setHost("localhost");
        registryFactory.setPort(1099);
        registryFactory.init();
        try (ServerSocket testSocket = registryFactory.createTestSocket()) {
            Assert.assertEquals(InetAddress.getByName("localhost"),
                    testSocket.getInetAddress());
        }
        registryFactory.destroy();
    }

    @Test
    public void unlimitedHostRegistry() throws IOException {
        RmiRegistryFactory registryFactory = new RmiRegistryFactory();
        registryFactory.setHost(null);
        registryFactory.setPort(1099);
        registryFactory.init();
        try (ServerSocket testSocket = registryFactory.createTestSocket()) {
            Assert.assertEquals(InetAddress.getByAddress(new byte[] { 0, 0, 0, 0 }),
                    testSocket.getInetAddress());
        }
        registryFactory.destroy();
    }

    @Test
    public void defaultRegistry() throws IOException {
        RmiRegistryFactory registryFactory = new RmiRegistryFactory();
        JMXConnectorConfiguration configuration = new JMXConnectorConfiguration();
        registryFactory.setHost(configuration.getConnectorHost());
        registryFactory.setPort(configuration.getConnectorPort());
        registryFactory.init();
        try (ServerSocket testSocket = registryFactory.createTestSocket()) {
            Assert.assertEquals(InetAddress.getByName("localhost"),
                    testSocket.getInetAddress());
        }
        registryFactory.destroy();
    }
}
