package org.apache.activemq.artemis.osgi;

import static org.easymock.EasyMock.expect;

import org.apache.activemq.artemis.api.core.Interceptor;
import org.apache.activemq.artemis.spi.core.protocol.ProtocolManagerFactory;
import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.junit.Test;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;

@SuppressWarnings({"rawtypes", "unchecked"})
public class ProtocolTrackerTest {

    @Test
    public void testLifecycle() throws Exception {
        IMocksControl c = EasyMock.createControl();
        BundleContext context = c.createMock(BundleContext.class);
        String[] requiredProtocols = {"a", "b"};
        ProtocolTrackerCallBack callback = c.createMock(ProtocolTrackerCallBack.class);

        RefFact protA = new RefFact(c, context, new String[]{"a"});
        RefFact protB = new RefFact(c, context, new String[]{"b"});

        callback.addFactory(protA.factory);
        EasyMock.expectLastCall();

        callback.addFactory(protB.factory);
        EasyMock.expectLastCall();
        callback.start();
        EasyMock.expectLastCall();

        callback.removeFactory(protA.factory);
        callback.stop();
        EasyMock.expectLastCall();

        c.replay();
        ProtocolTracker tracker = new ProtocolTracker("test", context, requiredProtocols, callback);
        tracker.addingService(protA.ref);
        tracker.addingService(protB.ref);
        tracker.removedService(protA.ref, protA.factory);
        c.verify();
    }

    class RefFact {
        ServiceReference<ProtocolManagerFactory<Interceptor>> ref;
        ProtocolManagerFactory factory;
        
        public RefFact(IMocksControl c, BundleContext context, String[] protocols) {
            ref = c.createMock(ServiceReference.class);
            factory = c.createMock(ProtocolManagerFactory.class);
            expect(factory.getProtocols()).andReturn(protocols).atLeastOnce();
            expect(context.getService(ref)).andReturn(factory).atLeastOnce();
        }
    }

}
