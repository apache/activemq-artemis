package org.apache.activemq.artemis.osgi;

import java.util.Hashtable;

import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.cm.ManagedServiceFactory;

public class Activator implements BundleActivator {

    ArtemisBrokerFactory factory;
    ServiceRegistration<ManagedServiceFactory> registration;

    @Override
    public void start(BundleContext context) throws Exception {
        factory = new ArtemisBrokerFactory(context);
        Hashtable<String, String> props = new Hashtable<>();
        props.put(Constants.SERVICE_PID, ArtemisBrokerFactory.PID);
        registration = context.registerService(ManagedServiceFactory.class, factory, props);
    }

    @Override
    public void stop(BundleContext context) throws Exception {
        registration.unregister();
        factory.destroy();
    }

}