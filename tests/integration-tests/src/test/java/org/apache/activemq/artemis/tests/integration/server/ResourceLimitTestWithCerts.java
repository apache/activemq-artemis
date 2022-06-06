package org.apache.activemq.artemis.tests.integration.server;

import org.apache.activemq.artemis.api.core.*;
import org.apache.activemq.artemis.api.core.client.*;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.config.impl.SecurityConfiguration;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.core.settings.impl.ResourceLimitSettings;
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.apache.activemq.artemis.spi.core.security.jaas.InVMLoginModule;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Before;
import org.junit.Test;

import javax.security.auth.login.LoginException;
import java.io.File;
import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.*;

public class ResourceLimitTestWithCerts extends ActiveMQTestBase{

    private ActiveMQServer server;

    static final Map<String,String> users = new TreeMap<> ();
    static final Map<String, Set<String>> roles = new TreeMap<> ();
    static final String password = "secret";
    static final String thrust = "cluster.store";
    static final String cluster = "cluster.keystore";
    static final String userName = "client";
    static final String user = userName + ".keystore";


    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        deleteKeystoreFiles ();
        boolean res = prepareKeystoreFiles ();

        //setup catalog of accounts
        addUser (userName, "UID="+userName+", DC=test, DC=artemis");
        addUser ("ACTIVEMQ.CLUSTER.ADMIN.USER","UID=cluster, DC=test, DC=artemis");
        addRole (userName, "amq");
        addRole ("ACTIVEMQ.CLUSTER.ADMIN.USER", "amq");
        //setup listener URL params
        Map<String, Object> params = new HashMap<>();
        params.put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
        params.put(TransportConstants.KEYSTORE_PATH_PROP_NAME, cluster);
        params.put(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME, password);
        params.put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, thrust);
        params.put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, password);
        params.put(TransportConstants.NEED_CLIENT_AUTH_PROP_NAME, true);
        TransportConfiguration netty = new TransportConfiguration (NETTY_ACCEPTOR_FACTORY, params);
        //set limits
        SecurityConfiguration password = getSecurity ();
        ResourceLimitSettings limit = new ResourceLimitSettings ();
        limit.setMaxConnections (1);
        limit.setMaxQueues (1);
        limit.setMatch (new SimpleString (userName));
        //TLS manager
        ActiveMQJAASSecurityManager securityManager = new ActiveMQJAASSecurityManager (
                InVMLoginModule.class.getName (),
                getLoginModule ());

        securityManager.setCertificateConfiguration (password);
        securityManager.setConfiguration (password);
        //put all to the configuration
        Configuration configuration = createBasicConfig ()
                .addAcceptorConfiguration (netty)
                .setSecurityEnabled (true)
                .addResourceLimitSettings (limit)
                .putSecurityRoles ("#", getLibertyRoles ());

        //create and start server
        server = addServer (ActiveMQServers.newActiveMQServer (configuration,null,
                securityManager, false));

        server.start();
        runAfter (() -> deleteKeystoreFiles ());
    }

    @Test
    public void testSessionLimitForUser() throws Exception {
        TransportConfiguration tc = new TransportConfiguration (NETTY_CONNECTOR_FACTORY);
        tc.getParams ().put (TransportConstants.SSL_ENABLED_PROP_NAME, true);
        tc.getParams ().put (TransportConstants.TRUSTSTORE_PATH_PROP_NAME, thrust);
        tc.getParams ().put (TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, password);
        tc.getParams ().put (TransportConstants.KEYSTORE_PATH_PROP_NAME, user);
        tc.getParams ().put (TransportConstants.KEYSTORE_PASSWORD_PROP_NAME, password);
        ServerLocator locator = addServerLocator (ActiveMQClient.createServerLocatorWithoutHA (tc));
        ClientSessionFactory cf = createSessionFactory (locator);

        ClientSession clientSession = cf.createSession ();

        try {
            ClientSessionFactory extraClientSessionFactory = locator.createSessionFactory ();
            ClientSession extraClientSession = extraClientSessionFactory.createSession ();
            fail("creating a session factory here should fail");
        } catch (Exception e) {
            assertTrue(e instanceof ActiveMQSessionCreationException);
        }

        clientSession.close ();

        clientSession = cf.createSession ();

        try {
            ClientSessionFactory extraClientSessionFactory = locator.createSessionFactory ();
            ClientSession extraClientSession = extraClientSessionFactory.createSession ();
            fail("creating a session factory here should fail");
        } catch (Exception e) {
            assertTrue(e instanceof ActiveMQSessionCreationException);
        }
        clientSession.close ();
        cf.close ();
    }

    @Test
    public void testQueueLimitForUser() throws Exception {
        TransportConfiguration tc = new TransportConfiguration (NETTY_CONNECTOR_FACTORY);
        tc.getParams ().put (TransportConstants.SSL_ENABLED_PROP_NAME, true);
        tc.getParams ().put (TransportConstants.TRUSTSTORE_PATH_PROP_NAME, thrust);
        tc.getParams ().put (TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, password);
        tc.getParams ().put (TransportConstants.KEYSTORE_PATH_PROP_NAME, user);
        tc.getParams ().put (TransportConstants.KEYSTORE_PASSWORD_PROP_NAME, password);
        ServerLocator locator = addServerLocator (ActiveMQClient.createServerLocatorWithoutHA (tc));
        ClientSessionFactory cf = createSessionFactory (locator);

        ClientSession clientSession = cf.createSession ();
        clientSession.createQueue (new QueueConfiguration ("queue")
                .setAddress ("address")
                .setRoutingType (RoutingType.ANYCAST)
                .setDurable (false));

        try {
            clientSession.createQueue(new QueueConfiguration("anotherQueue").setAddress("address").setRoutingType(RoutingType.ANYCAST).setDurable(false));
        } catch (Exception e) {
            assertTrue(e instanceof ActiveMQSecurityException);
        }

        clientSession.deleteQueue("queue");

        clientSession.createQueue(new QueueConfiguration("queue").setAddress("address").setRoutingType(RoutingType.ANYCAST).setDurable(false));

        try {
            clientSession.createQueue(new QueueConfiguration("anotherQueue").setAddress("address").setRoutingType(RoutingType.ANYCAST).setDurable(false));
        } catch (Exception e) {
            assertTrue(e instanceof ActiveMQSecurityException);
        }

        try {
            clientSession.createSharedQueue(new QueueConfiguration("anotherQueue").setAddress("address").setDurable(false));
        } catch (Exception e) {
            assertTrue(e instanceof ActiveMQSecurityException);
        }
    }
    static boolean runKeytool (String cmd){
        ArrayList<String> exec = new ArrayList<>();
        exec.add (System.getProperty ("java.home")
                + System.getProperty ("file.separator") + "bin" + System.getProperty ("file.separator")
                + "keytool"
                + (System.getProperty ("file.separator").equals("/")?"":".exe"));

        Collections.addAll (exec, cmd.split(" "));
        try {
            ProcessBuilder process = new ProcessBuilder (exec)
                    .directory (new File (System.getProperty ("user.dir")))
                    .inheritIO ();
            int result =  process.start ().waitFor ();
            return result == 0;
        } catch (IOException | InterruptedException e) {
        }
        return false;
    }

    static boolean prepareKeystoreFiles (){

        return runKeytool ("-genkeypair -validity 7200 -dname uid=cluster,dc=test,dc=artemis -keysize 2048 -alias localhost -ext KeyUsage=keyEncipherment -ext ExtendedKeyUsage=serverAuth,clientAuth -ext san=dns:localhost -keyalg RSA -keystore "+cluster+" -storetype pkcs12 -storepass " + password)
                && runKeytool ("-genkeypair -validity 3600 -dname uid="+userName+",dc=test,dc=artemis -keysize 2048 -alias "+userName+" -ext ExtendedKeyUsage=clientAuth -keyalg RSA -keystore "+user+" -storetype pkcs12 -storepass " + password)
                && runKeytool ("-exportcert -alias localhost -keystore "+cluster+" -storetype pkcs12 -storepass "+password+" -file cluster.cer")
                && runKeytool ("-exportcert -alias "+userName+" -keystore "+user+" -storetype pkcs12 -storepass "+password+" -file "+userName+".cer")
                && runKeytool ("-importcert -noprompt -alias cluster -keystore "+thrust+" -storepass "+password+" -storetype PKCS12 -file cluster.cer")
                && runKeytool ("-importcert -noprompt -alias "+userName+" -keystore "+thrust+" -storepass "+password+" -storetype PKCS12 -file "+userName+".cer")
                ;
    }
    static void deleteKeystoreFiles (){
        File file;
        file = new File (System.getProperty ("user.dir") + System.getProperty ("file.separator") + user);

        if (file.exists())
            file.delete();

        file = new File (System.getProperty ("user.dir") + System.getProperty ("file.separator") + userName+".cer");

        if (file.exists())
            file.delete();

        file = new File (System.getProperty ("user.dir") + System.getProperty ("file.separator") + cluster);

        if (file.exists())
            file.delete();
        file = new File (System.getProperty ("user.dir") + System.getProperty ("file.separator") + thrust);

        if (file.exists())
            file.delete();
        file = new File (System.getProperty ("user.dir") + System.getProperty ("file.separator") + "cluster.cer");

        if (file.exists())
            file.delete();
    }

    static ConfigurationImpl createConfig(String acceptor, String client) throws Exception {
        ConfigurationImpl config = new ConfigurationImpl()
                .setPersistenceEnabled (false)
                .setSecurityEnabled (true)
                .addAcceptorConfiguration("VM", "vm://0");

        if (acceptor != null || acceptor.isEmpty() == false)
            config.addAcceptorConfiguration("SSL", acceptor);

        ResourceLimitSettings resourceLimitSettings = new ResourceLimitSettings ();
        resourceLimitSettings.setMatch (SimpleString.toSimpleString (client));
        resourceLimitSettings.setMaxConnections (1);
        resourceLimitSettings.setMaxQueues (1);
        return config;
    }

    static HashSet<Role> getLibertyRoles (){
        HashSet<Role> roles = new HashSet<>();

        for (String role: ResourceLimitTestWithCerts.roles.keySet ())
            roles.add (new Role (role,
                    true,
                    true,
                    true,
                    true,
                    true,
                    true,
                    true,
                    true,
                    true,
                    true));

        return roles;
    }
    static String getLoginModule (){
        return TestLogin.class.getName ();
    }
    static SecurityConfiguration getSecurity (){
        SecurityConfiguration password = new SecurityConfiguration();

        for (String user: users.values()){
            password.addUser (user, ResourceLimitTestWithCerts.password);
        }
        for (Map.Entry<String,Set<String>> entry: roles.entrySet ()){
            for (String role: entry.getValue ()){
                password.addRole (entry.getKey(),role);
            }
        }
        return password;
    }

    static void addUser (String name, String dn){
        users.put (dn,name);
        addRole (name,name);
    }
    static void addRole (String user, String role){
        Set<String> roles = ResourceLimitTestWithCerts.roles.get (user);

        if (roles == null){
            roles = new HashSet<>();
            ResourceLimitTestWithCerts.roles.put (user,roles);
        }
        roles.add (role);
    }

    public static class TestLogin  extends org.apache.activemq.artemis.spi.core.security.jaas.CertificateLoginModule {

        public TestLogin (){

        }
        @Override
        protected String getUserNameForCertificates (X509Certificate[] x509Certificates) throws LoginException {
            if (x509Certificates == null) {
                throw new LoginException ("Client certificates not found. Cannot authenticate.");
            } else {
                String dn = this.getDistinguishedName (x509Certificates);
                return users.get (dn);
            }
        }

        @Override
        protected Set<String> getUserRoles(String s) throws LoginException {
            return roles.get (s);
        }
    }
}
