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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.activemq.artemis.core.asyncio.impl.AsynchronousFileImpl;
import org.apache.activemq.artemis.core.server.cluster.impl.MessageLoadBalancingType;

import static java.nio.file.attribute.PosixFilePermission.GROUP_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.GROUP_READ;
import static java.nio.file.attribute.PosixFilePermission.GROUP_WRITE;
import static java.nio.file.attribute.PosixFilePermission.OTHERS_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.OTHERS_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.OWNER_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;

/**
 * CLI action that creates a broker instance directory.
 */
@Command(name = "create", description = "creates a new broker instance")
public class Create extends InputAbstract
{
   private static final Integer DEFAULT_PORT = 61616;

   private static final Integer AMQP_PORT = 5672;

   private static final Integer STOMP_PORT = 61613;

   private static final Integer HQ_PORT = 5445;

   private static final Integer HTTP_PORT = 8161;

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
   public static final String ETC_REPLICATED_SETTINGS_TXT = "etc/replicated-settings.txt";
   public static final String ETC_SHARED_STORE_SETTINGS_TXT = "etc/shared-store-settings.txt";
   public static final String ETC_CLUSTER_SECURITY_SETTINGS_TXT = "etc/cluster-security-settings.txt";
   public static final String ETC_CLUSTER_SETTINGS_TXT = "etc/cluster-settings.txt";
   public static final String ETC_CONNECTOR_SETTINGS_TXT = "etc/connector-settings.txt";
   public static final String ETC_BOOTSTRAP_WEB_SETTINGS_TXT = "etc/bootstrap-web-settings.txt";

   @Arguments(description = "The instance directory to hold the broker's configuration and data.  Path must be writable.", required = true)
   File directory;

   @Option(name = "--host", description = "The host name of the broker (Default: 0.0.0.0 or input if clustered)")
   String host;

   @Option(name = "--port-offset", description = "Off sets the default ports")
   int portOffset;

   @Option(name = "--force", description = "Overwrite configuration at destination directory")
   boolean force;

   @Option(name = "--home", description = "Directory where ActiveMQ Artemis is installed")
   File home;

   @Option(name = "--data", description = "Directory where ActiveMQ Data is used. Paths are relative to artemis.instance")
   String data = "./data";

   @Option(name = "--clustered", description = "Enable clustering")
   boolean clustered = false;

   @Option(name = "--max-hops", description = "Number of hops on the cluster configuration")
   int maxHops = 0;

   @Option(name = "--message-load-balancing", description = "Load balancing policy on cluster. [ON_DEMAND (default) | STRICT | OFF]")
   MessageLoadBalancingType messageLoadBalancing = MessageLoadBalancingType.ON_DEMAND;

   @Option(name = "--replicated", description = "Enable broker replication")
   boolean replicated = false;

   @Option(name = "--shared-store", description = "Enable broker shared store")
   boolean sharedStore = false;

   @Option(name = "--cluster-user", description = "The cluster user to use for clustering. (Default: input)")
   String clusterUser = null;

   @Option(name = "--cluster-password", description = "The cluster password to use for clustering. (Default: input)")
   String clusterPassword = null;

   @Option(name = "--encoding", description = "The encoding that text files should use")
   String encoding = "UTF-8";

   @Option(name = "--java-options", description = "Extra java options to be passed to the profile")
   String javaOptions = "";

   @Option(name = "--allow-anonymous", description = "Enables anonymous configuration on security, opposite of --require-login (Default: input)")
   Boolean allowAnonymous = null;

   @Option(name = "--require-login", description = "This will configure security to require user / password, opposite of --allow-anonymous")
   Boolean requireLogin = null;

   @Option(name = "--user", description = "The username (Default: input)")
   String user;

   @Option(name = "--password", description = "The user's password (Default: input)")
   String password;

   @Option(name = "--role", description = "The name for the role created (Default: amq)")
   String role;

   @Option(name = "--no-web", description = "This will remove the web server definition from bootstrap.xml")
   boolean noWeb;

   boolean IS_WINDOWS;

   boolean IS_CYGWIN;

   public int getMaxHops()
   {
      return maxHops;
   }

   public void setMaxHops(int maxHops)
   {
      this.maxHops = maxHops;
   }

   public boolean isNoWeb()
   {
      return noWeb;
   }

   public void setNoWeb(boolean noWeb)
   {
      this.noWeb = noWeb;
   }

   public int getPortOffset()
   {
      return portOffset;
   }

   public void setPortOffset(int portOffset)
   {
      this.portOffset = portOffset;
   }

   public MessageLoadBalancingType getMessageLoadBalancing()
   {
      return messageLoadBalancing;
   }

   public void setMessageLoadBalancing(MessageLoadBalancingType messageLoadBalancing)
   {
      this.messageLoadBalancing = messageLoadBalancing;
   }

   public String getJavaOptions()
   {
      return javaOptions;
   }

   public void setJavaOptions(String javaOptions)
   {
      this.javaOptions = javaOptions;
   }

   public File getInstance()
   {
      return directory;
   }

   public void setInstance(File directory)
   {
      this.directory = directory;
   }

   public String getHost()
   {
      if (host == null)
      {
         host = "0.0.0.0";
      }
      return host;
   }

   public String getHostForClustered()
   {
      if (getHost().equals("0.0.0.0"))
      {
         host = input("--host", "Host " + host + " is not valid for clustering, please provide a valid IP or hostname", "localhost");
      }

      return host;
   }

   public void setHost(String host)
   {
      this.host = host;
   }

   public boolean isForce()
   {
      return force;
   }

   public void setForce(boolean force)
   {
      this.force = force;
   }

   public File getHome()
   {
      if (home == null)
      {
         home = new File(getBrokerHome());
      }
      return home;
   }

   public void setHome(File home)
   {
      this.home = home;
   }

   public boolean isClustered()
   {
      return clustered;
   }

   public void setClustered(boolean clustered)
   {
      this.clustered = clustered;
   }

   public boolean isReplicated()
   {
      return replicated;
   }

   public void setReplicated(boolean replicated)
   {
      this.replicated = replicated;
   }

   public boolean isSharedStore()
   {
      return sharedStore;
   }

   public void setSharedStore(boolean sharedStore)
   {
      this.sharedStore = sharedStore;
   }

   public String getEncoding()
   {
      return encoding;
   }

   public void setEncoding(String encoding)
   {
      this.encoding = encoding;
   }

   public String getData()
   {
      return data;
   }

   public void setData(String data)
   {
      this.data = data;
   }

   public String getClusterUser()
   {
      if (clusterUser == null)
      {
         clusterUser = input("--cluster-user", "Please provide the username:", "cluster-admin");
      }
      return clusterUser;
   }

   public void setClusterUser(String clusterUser)
   {
      this.clusterUser = clusterUser;
   }

   public String getClusterPassword()
   {
      if (clusterPassword == null)
      {
         clusterPassword = inputPassword("--cluster-password", "Please enter the password:", "password-admin");
      }
      return clusterPassword;
   }

   public void setClusterPassword(String clusterPassword)
   {
      this.clusterPassword = clusterPassword;
   }

   public boolean isAllowAnonymous()
   {
      if (allowAnonymous == null)
      {
         String value = input("--allow-anonymous | --require-login", "Allow anonymous access? (Y/N):", "Y");
         allowAnonymous = Boolean.valueOf(value.toLowerCase().equals("y"));
      }
      return allowAnonymous.booleanValue();
   }

   public void setAllowAnonymous(boolean allowGuest)
   {
      this.allowAnonymous = Boolean.valueOf(allowGuest);
   }

   public Boolean getRequireLogin()
   {
      if (requireLogin == null)
      {
         requireLogin = !isAllowAnonymous();
      }
      return requireLogin;
   }

   public void setRequireLogin(Boolean requireLogin)
   {
      this.requireLogin = requireLogin;
   }

   public String getPassword()
   {

      if (password == null)
      {
         this.password = inputPassword("--password", "Please provide the default password:", "admin");
      }

      return password;
   }

   public void setPassword(String password)
   {
      this.password = password;
   }

   public String getUser()
   {
      if (user == null)
      {
         user = input("--user", "Please provide the default username:", "admin");
      }
      return user;
   }

   public void setUser(String user)
   {
      this.user = user;
   }

   public String getRole()
   {
      if (role == null)
      {
         role = "amq";
      }
      return role;
   }

   public void setRole(String role)
   {
      this.role = role;
   }

   @Override
   public Object execute(ActionContext context) throws Exception
   {
      this.checkDirectory();
      super.execute(context);

      try
      {
         return run(context);
      }
      catch (Throwable e)
      {
         e.printStackTrace(context.err);
         throw e;
      }
   }


   /** This method is made public for the testsuite */
   public InputStream openStream(String source)
   {
      return this.getClass().getResourceAsStream(source);
   }

   /**
    * Checks that the directory provided either exists and is writable or doesn't exist but can be created.
    */
   private void checkDirectory()
   {
      if (!directory.exists())
      {
         boolean created = directory.mkdirs();
         if (!created)
         {
            throw new RuntimeException(String.format("Unable to create the path '%s'.", directory));
         }
      }
      else if (!directory.canWrite())
      {
         throw new RuntimeException(String.format("The path '%s' is not writable.", directory));
      }
   }

   public Object run(ActionContext context) throws Exception
   {
      IS_WINDOWS = System.getProperty("os.name").toLowerCase().trim().startsWith("win");
      IS_CYGWIN = IS_WINDOWS && "cygwin".equals(System.getenv("OSTYPE"));

      // requireLogin should set alloAnonymous=false, to avoid user's questions
      if (requireLogin != null && requireLogin.booleanValue())
      {
         allowAnonymous = Boolean.FALSE;
      }

      context.out.println(String.format("Creating ActiveMQ Artemis instance at: %s", directory.getCanonicalPath()));

      HashMap<String, String> filters = new HashMap<String, String>();

      if (replicated)
      {
         clustered = true;
         filters.put("${replicated.settings}", readTextFile(ETC_REPLICATED_SETTINGS_TXT));
      }
      else
      {
         filters.put("${replicated.settings}", "");
      }

      if (sharedStore)
      {
         clustered = true;
         filters.put("${shared-store.settings}", readTextFile(ETC_SHARED_STORE_SETTINGS_TXT));
      }
      else
      {
         filters.put("${shared-store.settings}", "");
      }

      if (IS_WINDOWS || !AsynchronousFileImpl.isLoaded())
      {
         filters.put("${journal.settings}", "NIO");
      }
      else
      {
         filters.put("${journal.settings}", "ASYNCIO");
      }

      filters.put("${user}", System.getProperty("user.name", ""));
      filters.put("${default.port}", String.valueOf(DEFAULT_PORT + portOffset));
      filters.put("${amqp.port}", String.valueOf(AMQP_PORT + portOffset));
      filters.put("${stomp.port}", String.valueOf(STOMP_PORT + portOffset));
      filters.put("${hq.port}", String.valueOf(HQ_PORT + portOffset));
      filters.put("${http.port}", String.valueOf(HTTP_PORT + portOffset));
      filters.put("${data.dir}", data);
      filters.put("${max-hops}", String.valueOf(maxHops));

      filters.put("${message-load-balancing}", messageLoadBalancing.toString());
      filters.put("${user}", getUser());
      filters.put("${password}", getPassword());
      filters.put("${role}", getRole());


      if (clustered)
      {
         filters.put("${host}", getHostForClustered());
         String connectorSettings = readTextFile(ETC_CONNECTOR_SETTINGS_TXT);
         connectorSettings = applyFilters(connectorSettings, filters);

         filters.put("${connector-config.settings}", connectorSettings);
         filters.put("${cluster-security.settings}", readTextFile(ETC_CLUSTER_SECURITY_SETTINGS_TXT));
         filters.put("${cluster.settings}", applyFilters(readTextFile(ETC_CLUSTER_SETTINGS_TXT), filters));
         filters.put("${cluster-user}", getClusterUser());
         filters.put("${cluster-password}", getClusterPassword());
      }
      else
      {
         filters.put("${host}", getHost());
         filters.put("${connector-config.settings}", "");
         filters.put("${cluster-security.settings}", "");
         filters.put("${cluster.settings}", "");
         filters.put("${cluster-user}", "");
         filters.put("${cluster-password}", "");
      }


      if (home != null)
      {
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
      new File(directory, "data").mkdirs();

      if (javaOptions == null || javaOptions.length() == 0)
      {
         javaOptions = "";
      }

      filters.put("${java-opts}", javaOptions);

      if (IS_WINDOWS)
      {
         write(BIN_ARTEMIS_CMD, null, false);
         write(BIN_ARTEMIS_SERVICE_EXE);
         write(BIN_ARTEMIS_SERVICE_XML, filters, false);
         write(ETC_ARTEMIS_PROFILE_CMD, filters, false);
      }

      if (!IS_WINDOWS || IS_CYGWIN)
      {
         write(BIN_ARTEMIS, null, true);
         makeExec(BIN_ARTEMIS);
         write(BIN_ARTEMIS_SERVICE, null, true);
         makeExec(BIN_ARTEMIS_SERVICE);
         write(ETC_ARTEMIS_PROFILE, filters, true);
         makeExec(ETC_ARTEMIS_PROFILE);
      }

      write(ETC_LOGGING_PROPERTIES, null, false);

      if (isAllowAnonymous())
      {
         filters.put("${bootstrap.guest}", "default-user=\"" + getUser() + "\"");
      }
      else
      {
         filters.put("${bootstrap.guest}", "");
      }


      if (noWeb)
      {
         filters.put("${bootstrap-web-settings}", "");
      }
      else
      {
         filters.put("${bootstrap-web-settings}", applyFilters(readTextFile(ETC_BOOTSTRAP_WEB_SETTINGS_TXT), filters));
      }



      write(ETC_BOOTSTRAP_XML, filters, false);
      write(ETC_BROKER_XML, filters, false);
      write(ETC_ARTEMIS_ROLES_PROPERTIES, filters, false);
      write(ETC_ARTEMIS_USERS_PROPERTIES, filters, false);

      context.out.println("");
      context.out.println("You can now start the broker by executing:  ");
      context.out.println("");
      context.out.println(String.format("   \"%s\" run", path(new File(directory, "bin/artemis"), true)));

      File service = new File(directory, BIN_ARTEMIS_SERVICE);
      context.out.println("");

      if (!IS_WINDOWS || IS_CYGWIN)
      {

         // Does it look like we are on a System V init system?
         if (new File("/etc/init.d/").isDirectory())
         {
            context.out.println("Or you can setup the broker as system service and run it in the background:");
            context.out.println("");
            context.out.println("   sudo ln -s \"%s\" /etc/init.d/".format(service.getCanonicalPath()));
            context.out.println("   /etc/init.d/artemis-service start");
            context.out.println("");

         }
         else
         {

            context.out.println("Or you can run the broker in the background using:");
            context.out.println("");
            context.out.println(String.format("   \"%s\" start", path(service, true)));
            context.out.println("");
         }
      }

      if (IS_WINDOWS)
      {
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

   private void makeExec(String path) throws IOException
   {
      try
      {
         File file = new File(directory, path);
         Files.setPosixFilePermissions(file.toPath(), new HashSet<PosixFilePermission>(Arrays.asList(
            OWNER_READ, OWNER_WRITE, OWNER_EXECUTE,
            GROUP_READ, GROUP_WRITE, GROUP_EXECUTE,
            OTHERS_READ, OTHERS_EXECUTE
         )));
      }
      catch (Throwable ignore)
      {
         // Our best effort was not good enough :)
      }
   }

   String path(String value, boolean unixPaths) throws IOException
   {
      return path(new File(value), unixPaths);
   }

   private String path(File value, boolean unixPaths) throws IOException
   {
      if (unixPaths && IS_CYGWIN)
      {
         return value.getCanonicalPath();
      }
      else
      {
         return value.getCanonicalPath();
      }
   }

   private void write(String source, HashMap<String, String> filters, boolean unixTarget) throws IOException
   {
      write(source, new File(directory, source), filters, unixTarget);
   }

   private void write(String source, File target, HashMap<String, String> filters, boolean unixTarget) throws IOException
   {
      if (target.exists() && !force)
      {
         throw new RuntimeException(String.format("The file '%s' already exists.  Use --force to overwrite.", target));
      }

      String content = applyFilters(readTextFile(source), filters);

      // and then writing out in the new target encoding..  Let's also replace \n with the values
      // that is correct for the current platform.
      String separator = unixTarget && IS_CYGWIN ? "\n" : System.getProperty("line.separator");
      content = content.replaceAll("\\r?\\n", Matcher.quoteReplacement(separator));
      ByteArrayInputStream in = new ByteArrayInputStream(content.getBytes(encoding));

      try (FileOutputStream fout = new FileOutputStream(target))
      {
         copy(in, fout);
      }
   }

   private String applyFilters(String content, HashMap<String, String> filters) throws IOException
   {

      if (filters != null)
      {
         for (Map.Entry<String, String> entry : filters.entrySet())
         {
            content = replace(content, entry.getKey(), entry.getValue());
         }
      }
      return content;
   }

   private String readTextFile(String source) throws IOException
   {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      try (InputStream in = openStream(source))
      {
         copy(in, out);
      }
      return new String(out.toByteArray(), "UTF-8");
   }

   private void write(String source) throws IOException
   {
      File target = new File(directory, source);
      if (target.exists() && !force)
      {
         throw new RuntimeException(String.format("The file '%s' already exists.  Use --force to overwrite.", target));
      }
      try (FileOutputStream fout = new FileOutputStream(target))
      {
         try (InputStream in = openStream(source))
         {
            copy(in, fout);
         }
      }
   }

   private String replace(String content, String key, String value)
   {
      return content.replaceAll(Pattern.quote(key), Matcher.quoteReplacement(value));
   }

   private void copy(InputStream is, OutputStream os) throws IOException
   {
      byte[] buffer = new byte[1024 * 4];
      int c = is.read(buffer);
      while (c >= 0)
      {
         os.write(buffer, 0, c);
         c = is.read(buffer);
      }
   }

}
