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

package org.apache.activemq.artemis.cli.commands;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.activemq.artemis.cli.CLIException;
import org.apache.activemq.artemis.cli.commands.util.HashUtil;
import org.apache.activemq.artemis.cli.commands.util.SyncCalculation;
import org.apache.activemq.artemis.core.server.JournalType;
import org.apache.activemq.artemis.core.server.cluster.impl.MessageLoadBalancingType;
import org.apache.activemq.artemis.jlibaio.LibaioContext;
import org.apache.activemq.artemis.jlibaio.LibaioFile;
import org.apache.activemq.artemis.utils.FileUtil;

/**
 * CLI action that creates a broker instance directory.
 */
@Command(name = "create", description = "creates a new broker instance")
public class Create extends InputAbstract {

   private static final Integer DEFAULT_PORT = 61616;

   private static final Integer AMQP_PORT = 5672;

   private static final Integer STOMP_PORT = 61613;

   private static final Integer HQ_PORT = 5445;

   public static final String HTTP_HOST = "localhost";

   public static final Integer HTTP_PORT = 8161;

   private static final Integer MQTT_PORT = 1883;

   /*  **********************************************************************************
    *  Note for developers: These are tested at StreamClassPathTest on the unit test.
    *  This is to make sure maven or something else is not hiding these resources.
    *  ********************************************************************************** */

   public static final String BIN_ARTEMIS_CMD = "bin/artemis.cmd";
   public static final String BIN_ARTEMIS_SERVICE_EXE = "bin/artemis-service.exe";
   public static final String BIN_ARTEMIS_SERVICE_XML = "bin/artemis-service.xml";
   public static final String ETC_ARTEMIS_PROFILE_CMD = "etc/artemis.profile.cmd";
   public static final String BIN_ARTEMIS = "bin/artemis";
   public static final String BIN_ARTEMIS_SERVICE = "bin/artemis-service";
   public static final String ETC_ARTEMIS_PROFILE = "etc/artemis.profile";
   public static final String ETC_LOGGING_PROPERTIES = "etc/logging.properties";
   public static final String ETC_BOOTSTRAP_XML = "etc/bootstrap.xml";
   public static final String ETC_BROKER_XML = "etc/broker.xml";

   public static final String ETC_ARTEMIS_ROLES_PROPERTIES = "etc/artemis-roles.properties";
   public static final String ETC_ARTEMIS_USERS_PROPERTIES = "etc/artemis-users.properties";
   private static final String ETC_LOGIN_CONFIG = "etc/login.config";
   private static final String ETC_LOGIN_CONFIG_WITH_GUEST = "etc/login-with-guest.config";
   private static final String ETC_LOGIN_CONFIG_WITHOUT_GUEST = "etc/login-without-guest.config";
   public static final String ETC_REPLICATED_SETTINGS_TXT = "etc/replicated-settings.txt";
   public static final String ETC_SHARED_STORE_SETTINGS_TXT = "etc/shared-store-settings.txt";
   public static final String ETC_CLUSTER_SECURITY_SETTINGS_TXT = "etc/cluster-security-settings.txt";
   public static final String ETC_CLUSTER_SETTINGS_TXT = "etc/cluster-settings.txt";
   public static final String ETC_CONNECTOR_SETTINGS_TXT = "etc/connector-settings.txt";
   public static final String ETC_BOOTSTRAP_WEB_SETTINGS_TXT = "etc/bootstrap-web-settings.txt";
   public static final String ETC_JOURNAL_BUFFER_SETTINGS = "etc/journal-buffer-settings.txt";
   public static final String ETC_AMQP_ACCEPTOR_TXT = "etc/amqp-acceptor.txt";
   public static final String ETC_HORNETQ_ACCEPTOR_TXT = "etc/hornetq-acceptor.txt";
   public static final String ETC_MQTT_ACCEPTOR_TXT = "etc/mqtt-acceptor.txt";
   public static final String ETC_STOMP_ACCEPTOR_TXT = "etc/stomp-acceptor.txt";
   public static final String ETC_PING_TXT = "etc/ping-settings.txt";
   public static final String ETC_COMMENTED_PING_TXT = "etc/commented-ping-settings.txt";

   public static final String ETC_GLOBAL_MAX_SPECIFIED_TXT = "etc/global-max-specified.txt";
   public static final String ETC_GLOBAL_MAX_DEFAULT_TXT = "etc/global-max-default.txt";

   @Arguments(description = "The instance directory to hold the broker's configuration and data.  Path must be writable.", required = true)
   private File directory;

   @Option(name = "--host", description = "The host name of the broker (Default: 0.0.0.0 or input if clustered)")
   private String host;

   @Option(name = "--http-host", description = "The host name to use for embedded web server (Default: localhost)")
   private String httpHost = HTTP_HOST;

   @Option(name = "--ping", description = "A comma separated string to be passed on to the broker config as network-check-list. The broker will shutdown when all these addresses are unreachable.")
   private String ping;

   @Option(name = "--default-port", description = "The port number to use for the main 'artemis' acceptor (Default: 61616)")
   private int defaultPort = DEFAULT_PORT;

   @Option(name = "--http-port", description = "The port number to use for embedded web server (Default: 8161)")
   private int httpPort = HTTP_PORT;

   @Option(name = "--ssl-key", description = "The key store path for embedded web server")
   private String sslKey;

   @Option(name = "--ssl-key-password", description = "The key store password")
   private String sslKeyPassword;

   @Option(name = "--use-client-auth", description = "If the embedded server requires client authentication")
   private boolean useClientAuth;

   @Option(name = "--ssl-trust", description = "The trust store path in case of client authentication")
   private String sslTrust;

   @Option(name = "--ssl-trust-password", description = "The trust store password")
   private String sslTrustPassword;

   @Option(name = "--name", description = "The name of the broker (Default: same as host)")
   private String name;

   @Option(name = "--port-offset", description = "Off sets the ports of every acceptor")
   private int portOffset;

   @Option(name = "--force", description = "Overwrite configuration at destination directory")
   private boolean force;

   @Option(name = "--home", description = "Directory where ActiveMQ Artemis is installed")
   private File home;

   @Option(name = "--data", description = "Directory where ActiveMQ Data is used. Paths are relative to artemis.instance")
   private String data = "./data";

   @Option(name = "--clustered", description = "Enable clustering")
   private boolean clustered = false;

   @Option(name = "--max-hops", description = "Number of hops on the cluster configuration")
   private int maxHops = 0;

   @Option(name = "--message-load-balancing", description = "Load balancing policy on cluster. [ON_DEMAND (default) | STRICT | OFF]")
   private MessageLoadBalancingType messageLoadBalancing = MessageLoadBalancingType.ON_DEMAND;

   @Option(name = "--replicated", description = "Enable broker replication")
   private boolean replicated = false;

   @Option(name = "--shared-store", description = "Enable broker shared store")
   private boolean sharedStore = false;

   @Option(name = "--slave", description = "Valid for shared store or replication: this is a slave server?")
   private boolean slave;

   @Option(name = "--failover-on-shutdown", description = "Valid for shared store: will shutdown trigger a failover? (Default: false)")
   private boolean failoverOnShutodwn;

   @Option(name = "--cluster-user", description = "The cluster user to use for clustering. (Default: input)")
   private String clusterUser = null;

   @Option(name = "--cluster-password", description = "The cluster password to use for clustering. (Default: input)")
   private String clusterPassword = null;

   @Option(name = "--encoding", description = "The encoding that text files should use")
   private String encoding = "UTF-8";

   @Option(name = "--java-options", description = "Extra java options to be passed to the profile")
   private String javaOptions = "";

   @Option(name = "--allow-anonymous", description = "Enables anonymous configuration on security, opposite of --require-login (Default: input)")
   private Boolean allowAnonymous = null;

   @Option(name = "--require-login", description = "This will configure security to require user / password, opposite of --allow-anonymous")
   private Boolean requireLogin = null;

   @Option(name = "--paging", description = "Page messages to disk when address becomes full, opposite of --blocking (Default: true)")
   private Boolean paging;

   @Option(name = "--blocking", description = "Block producers when address becomes full, opposite of --paging (Default: false)")
   private Boolean blocking;

   @Option(name = "--no-autotune", description = "Disable auto tuning on the journal.")
   private boolean noAutoTune;

   @Option(name = "--no-autocreate", description = "Disable Auto create addresses.")
   private Boolean noAutoCreate;

   @Option(name = "--autocreate", description = "Auto create addresses. (default: true)")
   private Boolean autoCreate;

   @Option(name = "--user", description = "The username (Default: input)")
   private String user;

   @Option(name = "--password", description = "The user's password (Default: input)")
   private String password;

   @Option(name = "--role", description = "The name for the role created (Default: amq)")
   private String role = "amq";

   @Option(name = "--no-web", description = "Remove the web-server definition from bootstrap.xml")
   private boolean noWeb;

   @Option(name = "--queues", description = "Comma separated list of queues.")
   private String queues;

   @Option(name = "--addresses", description = "Comma separated list of addresses ")
   private String addresses;

   @Option(name = "--aio", description = "Sets the journal as asyncio.")
   private boolean aio;

   @Option(name = "--nio", description = "Sets the journal as nio.")
   private boolean nio;

   @Option(name = "--mapped", description = "Sets the journal as mapped.")
   private boolean mapped;

   // this is used by the setupJournalType method
   private JournalType journalType;

   @Option(name = "--disable-persistence", description = "Disable message persistence to the journal")
   private boolean disablePersistence;

   @Option(name = "--no-amqp-acceptor", description = "Disable the AMQP specific acceptor.")
   private boolean noAmqpAcceptor;

   @Option(name = "--no-mqtt-acceptor", description = "Disable the MQTT specific acceptor.")
   private boolean noMqttAcceptor;

   @Option(name = "--no-stomp-acceptor", description = "Disable the STOMP specific acceptor.")
   private boolean noStompAcceptor;

   @Option(name = "--no-hornetq-acceptor", description = "Disable the HornetQ specific acceptor.")
   private boolean noHornetQAcceptor;

   @Option(name = "--no-fsync", description = "Disable usage of fdatasync (channel.force(false) from java nio) on the journal")
   private boolean noJournalSync;

   @Option(name = "--global-max-size", description = "Maximum amount of memory which message data may consume (Default: Undefined, half of the system's memory)")
   private String globalMaxSize;

   private boolean IS_WINDOWS;
   private boolean IS_CYGWIN;

   private boolean isAutoCreate() {
      if (autoCreate == null) {
         if (noAutoCreate != null) {
            autoCreate = !noAutoCreate.booleanValue();
         }
      }

      if (autoCreate == null) {
         autoCreate = true;
      }

      return autoCreate;
   }

   public File getInstance() {
      return directory;
   }

   public void setInstance(File directory) {
      this.directory = directory;
   }

   public String getHost() {
      if (host == null) {
         host = "0.0.0.0";
      }
      return host;
   }

   private String getHostForClustered() {
      if (getHost().equals("0.0.0.0")) {
         host = input("--host", "Host " + host + " is not valid for clustering, please provide a valid IP or hostname", "localhost");
      }

      return host;
   }

   public void setHost(String host) {
      this.host = host;
   }

   public boolean isForce() {
      return force;
   }

   public void setForce(boolean force) {
      this.force = force;
   }

   public File getHome() {
      if (home == null) {
         home = new File(getBrokerHome());
      }
      return home;
   }

   public void setHome(File home) {
      this.home = home;
   }

   public boolean isReplicated() {
      return replicated;
   }

   public void setReplicated(boolean replicated) {
      this.replicated = replicated;
   }

   public boolean isSharedStore() {
      return sharedStore;
   }

   public String getEncoding() {
      return encoding;
   }

   public void setEncoding(String encoding) {
      this.encoding = encoding;
   }

   public String getData() {
      return data;
   }

   public void setData(String data) {
      this.data = data;
   }

   private String getClusterUser() {
      if (clusterUser == null) {
         clusterUser = input("--cluster-user", "Please provide the username:", "cluster-admin");
      }
      return clusterUser;
   }

   private String getClusterPassword() {
      if (clusterPassword == null) {
         clusterPassword = inputPassword("--cluster-password", "Please enter the password:", "password-admin");
      }
      return clusterPassword;
   }

   private String getSslKeyPassword() {
      if (sslKeyPassword == null) {
         sslKeyPassword = inputPassword("--ssl-key-password", "Please enter the keystore password:", "password");
      }
      return sslKeyPassword;
   }

   private String getSslTrust() {
      if (sslTrust == null) {
         sslTrust = input("--ssl-trust", "Please enter the trust store path:", "/etc/truststore.jks");
      }
      return sslTrust;
   }

   private String getSslTrustPassword() {
      if (sslTrustPassword == null) {
         sslTrustPassword = inputPassword("--ssl-key-password", "Please enter the keystore password:", "password");
      }
      return sslTrustPassword;
   }

   private boolean isAllowAnonymous() {
      if (allowAnonymous == null) {
         allowAnonymous = inputBoolean("--allow-anonymous | --require-login", "Allow anonymous access?", true);
      }
      return allowAnonymous;
   }

   public boolean isPaging() {
      return paging;
   }

   public String getPassword() {

      if (password == null) {
         password = inputPassword("--password", "Please provide the default password:", "admin");
      }

      password = HashUtil.tryHash(context, password);

      return password;
   }

   public void setPassword(String password) {
      this.password = password;
   }

   public String getUser() {
      if (user == null) {
         user = input("--user", "Please provide the default username:", "admin");
      }
      return user;
   }

   public void setUser(String user) {
      this.user = user;
   }

   public String getRole() {
      return role;
   }

   public void setRole(String role) {
      this.role = role;
   }

   private boolean isSlave() {
      return slave;
   }

   private boolean isFailoverOnShutodwn() {
      return failoverOnShutodwn;
   }

   private boolean isDisablePersistence() {
      return disablePersistence;
   }

   @Override
   public Object execute(ActionContext context) throws Exception {
      this.checkDirectory();
      super.execute(context);

      return run(context);
   }

   /**
    * This method is made public for the testsuite
    */
   public InputStream openStream(String source) {
      return this.getClass().getResourceAsStream(source);
   }

   /**
    * Checks that the directory provided either exists and is writable or doesn't exist but can be created.
    */
   private void checkDirectory() {
      if (!directory.exists()) {
         boolean created = directory.mkdirs();
         if (!created) {
            throw new RuntimeException(String.format("Unable to create the path '%s'.", directory));
         }
      } else if (!directory.canWrite()) {
         throw new RuntimeException(String.format("The path '%s' is not writable.", directory));
      }
   }

   public Object run(ActionContext context) throws Exception {

      IS_WINDOWS = System.getProperty("os.name").toLowerCase().trim().startsWith("win");
      IS_CYGWIN = IS_WINDOWS && "cygwin".equals(System.getenv("OSTYPE"));

      setupJournalType();

      // requireLogin should set allowAnonymous=false, to avoid user's questions
      if (requireLogin != null && requireLogin.booleanValue()) {
         allowAnonymous = Boolean.FALSE;
      }

      context.out.println(String.format("Creating ActiveMQ Artemis instance at: %s", directory.getCanonicalPath()));

      HashMap<String, String> filters = new HashMap<>();

      filters.put("${master-slave}", isSlave() ? "slave" : "master");

      filters.put("${failover-on-shutdown}", isFailoverOnShutodwn() ? "true" : "false");

      filters.put("${persistence-enabled}", isDisablePersistence() ? "false" : "true");

      if (ping != null && !ping.isEmpty()) {
         filters.put("${ping}", ping);
         filters.put("${ping-config.settings}", applyFilters(readTextFile(ETC_PING_TXT), filters));
      } else {
         filters.put("${ping-config.settings}", readTextFile(ETC_COMMENTED_PING_TXT));
      }

      if (replicated) {
         clustered = true;
         filters.put("${replicated.settings}", applyFilters(readTextFile(ETC_REPLICATED_SETTINGS_TXT), filters));
      } else {
         filters.put("${replicated.settings}", "");
      }

      if (sharedStore) {
         clustered = true;
         filters.put("${shared-store.settings}", applyFilters(readTextFile(ETC_SHARED_STORE_SETTINGS_TXT), filters));
      } else {
         filters.put("${shared-store.settings}", "");
      }

      filters.put("${journal.settings}", journalType.name());

      if (sslKey != null) {
         filters.put("${web.protocol}", "https");
         getSslKeyPassword();
         String extraWebAttr = " keyStorePath=\"" + sslKey + "\" keyStorePassword=\"" + sslKeyPassword + "\"";
         if (useClientAuth) {
            getSslTrust();
            getSslTrustPassword();
            extraWebAttr += " clientAuth=\"true\" trustStorePath=\"" + sslTrust + "\" trustStorePassword=\"" + sslTrustPassword + "\"";
         }
         filters.put("${extra.web.attributes}", extraWebAttr);
      } else {
         filters.put("${web.protocol}", "http");
         filters.put("${extra.web.attributes}", "");
      }
      filters.put("${fsync}", String.valueOf(!noJournalSync));
      filters.put("${user}", System.getProperty("user.name", ""));
      filters.put("${default.port}", String.valueOf(defaultPort + portOffset));
      filters.put("${amqp.port}", String.valueOf(AMQP_PORT + portOffset));
      filters.put("${stomp.port}", String.valueOf(STOMP_PORT + portOffset));
      filters.put("${hq.port}", String.valueOf(HQ_PORT + portOffset));
      filters.put("${mqtt.port}", String.valueOf(MQTT_PORT + portOffset));
      filters.put("${http.host}", httpHost);
      filters.put("${http.port}", String.valueOf(httpPort + portOffset));
      filters.put("${data.dir}", data);
      filters.put("${max-hops}", String.valueOf(maxHops));

      filters.put("${message-load-balancing}", messageLoadBalancing.toString());
      filters.put("${user}", getUser());
      filters.put("${password}", getPassword());
      filters.put("${role}", role);


      if (globalMaxSize == null || globalMaxSize.trim().equals("")) {
         filters.put("${global-max-section}", readTextFile(ETC_GLOBAL_MAX_DEFAULT_TXT));
      } else {
         filters.put("${global-max-size}", globalMaxSize);
         filters.put("${global-max-section}", applyFilters(readTextFile(ETC_GLOBAL_MAX_SPECIFIED_TXT), filters));
      }


      if (clustered) {
         filters.put("${host}", getHostForClustered());
         if (name == null) {
            name = getHostForClustered();
         }
         String connectorSettings = readTextFile(ETC_CONNECTOR_SETTINGS_TXT);
         connectorSettings = applyFilters(connectorSettings, filters);

         filters.put("${name}", name);
         filters.put("${connector-config.settings}", connectorSettings);
         filters.put("${cluster-security.settings}", readTextFile(ETC_CLUSTER_SECURITY_SETTINGS_TXT));
         filters.put("${cluster.settings}", applyFilters(readTextFile(ETC_CLUSTER_SETTINGS_TXT), filters));
         filters.put("${cluster-user}", getClusterUser());
         filters.put("${cluster-password}", getClusterPassword());
      } else {
         if (name == null) {
            name = getHost();
         }
         filters.put("${name}", name);
         filters.put("${host}", getHost());
         filters.put("${connector-config.settings}", "");
         filters.put("${cluster-security.settings}", "");
         filters.put("${cluster.settings}", "");
         filters.put("${cluster-user}", "");
         filters.put("${cluster-password}", "");
      }

      applyAddressesAndQueues(filters);

      if (home != null) {
         filters.put("${home}", path(home, false));
      }
      filters.put("${artemis.home}", path(getHome().toString(), false));
      filters.put("${artemis.instance}", path(directory, false));
      filters.put("${artemis.instance.name}", directory.getName());
      filters.put("${java.home}", path(System.getProperty("java.home"), false));

      new File(directory, "bin").mkdirs();
      new File(directory, "etc").mkdirs();
      new File(directory, "log").mkdirs();
      new File(directory, "tmp").mkdirs();
      File dataFolder = new File(directory, "data");
      dataFolder.mkdirs();

      filters.put("${logmanager}", getLogManager());

      if (javaOptions == null || javaOptions.length() == 0) {
         javaOptions = "";
      }

      filters.put("${java-opts}", javaOptions);

      if (isAllowAnonymous()) {
         write(ETC_LOGIN_CONFIG_WITH_GUEST, filters, false);
         new File(directory, ETC_LOGIN_CONFIG_WITH_GUEST).renameTo(new File(directory, ETC_LOGIN_CONFIG));
      } else {
         write(ETC_LOGIN_CONFIG_WITHOUT_GUEST, filters, false);
         new File(directory, ETC_LOGIN_CONFIG_WITHOUT_GUEST).renameTo(new File(directory, ETC_LOGIN_CONFIG));
      }

      write(ETC_ARTEMIS_ROLES_PROPERTIES, filters, false);

      if (IS_WINDOWS) {
         write(BIN_ARTEMIS_CMD, null, false);
         write(BIN_ARTEMIS_SERVICE_EXE);
         write(BIN_ARTEMIS_SERVICE_XML, filters, false);
         write(ETC_ARTEMIS_PROFILE_CMD, filters, false);
      }

      if (!IS_WINDOWS || IS_CYGWIN) {
         write(BIN_ARTEMIS, filters, true);
         makeExec(BIN_ARTEMIS);
         write(BIN_ARTEMIS_SERVICE, filters, true);
         makeExec(BIN_ARTEMIS_SERVICE);
         write(ETC_ARTEMIS_PROFILE, filters, true);
      }

      write(ETC_LOGGING_PROPERTIES, null, false);

      if (noWeb) {
         filters.put("${bootstrap-web-settings}", "");
      } else {
         filters.put("${bootstrap-web-settings}", applyFilters(readTextFile(ETC_BOOTSTRAP_WEB_SETTINGS_TXT), filters));
      }

      if (noAmqpAcceptor) {
         filters.put("${amqp-acceptor}", "");
      } else {
         filters.put("${amqp-acceptor}", applyFilters(readTextFile(ETC_AMQP_ACCEPTOR_TXT), filters));
      }

      if (noMqttAcceptor) {
         filters.put("${mqtt-acceptor}", "");
      } else {
         filters.put("${mqtt-acceptor}", applyFilters(readTextFile(ETC_MQTT_ACCEPTOR_TXT), filters));
      }

      if (noStompAcceptor) {
         filters.put("${stomp-acceptor}", "");
      } else {
         filters.put("${stomp-acceptor}", applyFilters(readTextFile(ETC_STOMP_ACCEPTOR_TXT), filters));
      }

      if (noHornetQAcceptor) {
         filters.put("${hornetq-acceptor}", "");
      } else {
         filters.put("${hornetq-acceptor}", applyFilters(readTextFile(ETC_HORNETQ_ACCEPTOR_TXT), filters));
      }

      if (paging == null && blocking == null) {
         filters.put("${full-policy}", "PAGE");
      } else if (paging != null && paging) {
         filters.put("${full-policy}", "PAGE");
      } else if (blocking != null && blocking) {
         filters.put("${full-policy}", "BLOCK");
      } else {
         filters.put("${full-policy}", "PAGE");
      }


      filters.put("${auto-create}", isAutoCreate() ? "true" : "false");

      performAutoTune(filters, journalType, dataFolder);

      write(ETC_BROKER_XML, filters, false);
      write(ETC_ARTEMIS_USERS_PROPERTIES, filters, false);

      // we want this variable to remain unchanged so that it will use the value set in the profile
      filters.remove("${artemis.instance}");
      write(ETC_BOOTSTRAP_XML, filters, false);

      context.out.println("");
      context.out.println("You can now start the broker by executing:  ");
      context.out.println("");
      context.out.println(String.format("   \"%s\" run", path(new File(directory, "bin/artemis"), true)));

      File service = new File(directory, BIN_ARTEMIS_SERVICE);
      context.out.println("");

      if (!IS_WINDOWS || IS_CYGWIN) {
         context.out.println("Or you can run the broker in the background using:");
         context.out.println("");
         context.out.println(String.format("   \"%s\" start", path(service, true)));
         context.out.println("");
      }

      if (IS_WINDOWS) {
         service = new File(directory, BIN_ARTEMIS_SERVICE_EXE);
         context.out.println("Or you can setup the broker as Windows service and run it in the background:");
         context.out.println("");
         context.out.println(String.format("   \"%s\" install", path(service, true)));
         context.out.println(String.format("   \"%s\" start", path(service, true)));
         context.out.println("");
         context.out.println("   To stop the windows service:");
         context.out.println(String.format("      \"%s\" stop", path(service, true)));
         context.out.println("");
         context.out.println("   To uninstall the windows service");
         context.out.println(String.format("      \"%s\" uninstall", path(service, true)));
      }

      return null;
   }

   private void setupJournalType() {
      int countJournalTypes = countBoolean(aio, nio, mapped);
      if (countJournalTypes > 1) {
         throw new RuntimeException("You can only select one journal type (--nio | --aio | --mapped).");
      }

      if (countJournalTypes == 0) {
         if (supportsLibaio()) {
            aio = true;
         } else {
            nio = true;
         }
      }

      if (aio) {
         journalType = JournalType.ASYNCIO;
      } else if (nio) {
         journalType = JournalType.NIO;
      } else if (mapped) {
         journalType = JournalType.MAPPED;
      }
   }

   private static int countBoolean(boolean...b) {
      int count = 0;

      for (boolean itemB : b) {
         if (itemB) count++;
      }

      return count;
   }

   private String getLogManager() throws IOException {
      String logManager = "";
      File dir = new File(path(getHome().toString(), false) + "/lib");

      File[] matches = dir.listFiles(new FilenameFilter() {
         @Override
         public boolean accept(File dir, String name) {
            return name.startsWith("jboss-logmanager") && name.endsWith(".jar");
         }
      });

      if (matches != null && matches.length > 0) {
         logManager = matches[0].getName();
      }

      return logManager;
   }

   /**
    * It will create the address and queue configurations
    */
   private void applyAddressesAndQueues(HashMap<String, String> filters) {
      StringWriter writer = new StringWriter();
      PrintWriter printWriter = new PrintWriter(writer);
      printWriter.println();

      for (String str : getQueueList()) {
         printWriter.println("         <address name=\"" + str + "\">");
         printWriter.println("            <anycast>");
         printWriter.println("               <queue name=\"" + str + "\" />");
         printWriter.println("            </anycast>");
         printWriter.println("         </address>");
      }
      for (String str : getAddressList()) {
         printWriter.println("         <address name=\"" + str + "\"/>");
      }
      filters.put("${address-queue.settings}", writer.toString());
   }

   private void performAutoTune(HashMap<String, String> filters, JournalType journalType, File dataFolder) {
      if (noAutoTune) {
         filters.put("${journal-buffer.settings}", "");
      } else {
         try {
            int writes = 250;
            System.out.println("");
            System.out.println("Auto tuning journal ...");

            long time = SyncCalculation.syncTest(dataFolder, 4096, writes, 5, verbose, !noJournalSync, journalType);
            long nanoseconds = SyncCalculation.toNanos(time, writes, verbose);
            double writesPerMillisecond = (double) writes / (double) time;

            String writesPerMillisecondStr = new DecimalFormat("###.##").format(writesPerMillisecond);

            HashMap<String, String> syncFilter = new HashMap<>();
            syncFilter.put("${nanoseconds}", Long.toString(nanoseconds));
            syncFilter.put("${writesPerMillisecond}", writesPerMillisecondStr);

            System.out.println("done! Your system can make " + writesPerMillisecondStr +
                                  " writes per millisecond, your journal-buffer-timeout will be " + nanoseconds);

            filters.put("${journal-buffer.settings}", applyFilters(readTextFile(ETC_JOURNAL_BUFFER_SETTINGS), syncFilter));

         } catch (Exception e) {
            filters.put("${journal-buffer.settings}", "");
            e.printStackTrace();
            System.err.println("Couldn't perform sync calculation, using default values");
         }
      }
   }

   public boolean supportsLibaio() {
      if (IS_WINDOWS) {
         return false;
      }
      if (LibaioContext.isLoaded()) {
         try (LibaioContext context = new LibaioContext(1, true, true)) {
            File tmpFile = new File(directory, "validateAIO.bin");
            boolean supportsLibaio = true;
            try {
               LibaioFile file = context.openFile(tmpFile, true);
               file.close();
            } catch (Exception e) {
               supportsLibaio = false;
            }
            tmpFile.delete();
            if (!supportsLibaio) {
               System.err.println("The filesystem used on " + directory + " doesn't support libAIO and O_DIRECT files, switching journal-type to NIO");
            }
            return supportsLibaio;
         }
      } else {
         return false;
      }
   }

   private void makeExec(String path) throws IOException {
      FileUtil.makeExec(new File(directory, path));
   }

   private String[] getQueueList() {
      if (queues == null) {
         return new String[0];
      } else {
         return queues.split(",");
      }
   }

   private String[] getAddressList() {
      if (addresses == null) {
         return new String[0];
      } else {
         return addresses.split(",");
      }
   }

   private String path(String value, boolean unixPaths) throws IOException {
      return path(new File(value), unixPaths);
   }

   private String path(File value, boolean unixPaths) throws IOException {
      if (unixPaths && IS_CYGWIN) {
         return value.getCanonicalPath();
      } else {
         return value.getCanonicalPath();
      }
   }

   private void write(String source, HashMap<String, String> filters, boolean unixTarget) throws Exception {
      write(source, new File(directory, source), filters, unixTarget);
   }

   private void write(String source,
                      File target,
                      HashMap<String, String> filters,
                      boolean unixTarget) throws Exception {
      if (target.exists() && !force) {
         throw new CLIException(String.format("The file '%s' already exists.  Use --force to overwrite.", target));
      }

      String content = applyFilters(readTextFile(source), filters);

      // and then writing out in the new target encoding..  Let's also replace \n with the values
      // that is correct for the current platform.
      String separator = unixTarget && IS_CYGWIN ? "\n" : System.getProperty("line.separator");
      content = content.replaceAll("\\r?\\n", Matcher.quoteReplacement(separator));
      ByteArrayInputStream in = new ByteArrayInputStream(content.getBytes(encoding));

      try (FileOutputStream fout = new FileOutputStream(target)) {
         copy(in, fout);
      }
   }

   private String applyFilters(String content, HashMap<String, String> filters) throws IOException {

      if (filters != null) {
         for (Map.Entry<String, String> entry : filters.entrySet()) {
            content = replace(content, entry.getKey(), entry.getValue());
         }
      }
      return content;
   }

   private String readTextFile(String source) throws IOException {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      try (InputStream in = openStream(source)) {
         copy(in, out);
      }
      return new String(out.toByteArray(), StandardCharsets.UTF_8);
   }

   private void write(String source) throws IOException {
      File target = new File(directory, source);
      if (target.exists() && !force) {
         throw new RuntimeException(String.format("The file '%s' already exists.  Use --force to overwrite.", target));
      }
      try (FileOutputStream fout = new FileOutputStream(target)) {
         try (InputStream in = openStream(source)) {
            copy(in, fout);
         }
      }
   }

   private String replace(String content, String key, String value) {
      return content.replaceAll(Pattern.quote(key), Matcher.quoteReplacement(value));
   }

   private void copy(InputStream is, OutputStream os) throws IOException {
      byte[] buffer = new byte[1024 * 4];
      int c = is.read(buffer);
      while (c >= 0) {
         os.write(buffer, 0, c);
         c = is.read(buffer);
      }
   }
}
