package org.apache.activemq.artemis.osgi;

import org.apache.activemq.artemis.api.core.Interceptor;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.spi.core.protocol.ProtocolManagerFactory;

public interface ProtocolTrackerCallBack extends ActiveMQComponent {
    void addFactory(ProtocolManagerFactory<Interceptor> pmf);
    void removeFactory(ProtocolManagerFactory<Interceptor> pmf);
}
