package org.apache.activemq.artemis.tests.integration.server;

import org.apache.activemq.artemis.core.io.IOCriticalErrorListener;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * A simple test-case used for documentation purposes.
 */
public class IOCriticalErrorListenerTest extends ActiveMQTestBase{
    protected ActiveMQServer server;

    @Test
    public void simpleTest() throws Exception {
        ServerSocket s = new ServerSocket();
        try {
            s.bind(new InetSocketAddress("127.0.0.1", 61616));
            server = createServer(false, createDefaultNettyConfig());
            final CountDownLatch latch = new CountDownLatch(1);
            server.registerIOCriticalErrorListener((code, message, file) -> latch.countDown());
            server.start();
            assertTrue(latch.await(3000, TimeUnit.MILLISECONDS));
        } finally {
            s.close();
        }
    }
}
