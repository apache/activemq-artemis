/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.integration.karaf;

import javax.inject.Inject;
import javax.security.auth.Subject;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import org.apache.karaf.jaas.boot.principal.RolePrincipal;
import org.apache.karaf.jaas.boot.principal.UserPrincipal;
import org.apache.karaf.shell.api.console.Session;
import org.apache.karaf.shell.api.console.SessionFactory;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.karaf.options.KarafDistributionOption;
import org.ops4j.pax.exam.karaf.options.LogLevelOption;
import org.ops4j.pax.exam.options.UrlReference;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.util.tracker.ServiceTracker;

import static org.ops4j.pax.exam.CoreOptions.maven;
import static org.ops4j.pax.exam.CoreOptions.mavenBundle;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.debugConfiguration;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.editConfigurationFilePut;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.features;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.karafDistributionConfiguration;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.logLevel;

public class KarafBaseTest extends Assert {

   private static Logger LOG = Logger.getLogger(KarafBaseTest.class.getName());

   public static final long ASSERTION_TIMEOUT = 30000L;
   public static final long COMMAND_TIMEOUT = 30000L;
   public static final String USER = "karaf";
   public static final String PASSWORD = "karaf";

   /**
    * plug to add more options on sub tests
    */
   protected void testOptions(List<Option> options) throws Exception {
   }

   public Option[] configureArtemisFeatures(boolean debug, String version, String... features) throws Exception {

      ArrayList<String> featureArray = new ArrayList<>();
      featureArray.addAll(Arrays.asList(features));

      List<Option> optionList = new LinkedList<>();

      optionList.add(karafDistributionConfiguration().
                                    frameworkUrl(maven().groupId("org.apache.karaf").
                                       artifactId("apache-karaf").type("tar.gz").
                                       versionAsInProject()).unpackDirectory(new File("target/containertest/unpack/")));
      optionList.add(KarafDistributionOption.keepRuntimeFolder());
      optionList.add(logLevel(LogLevelOption.LogLevel.INFO));
      optionList.add(editConfigurationFilePut("etc/config.properties", "karaf.startlevel.bundle", "50"));

      if (debug) {
         // uncomment this to debug it.
         optionList.add(debugConfiguration("5005", true));
      }

      optionList.add(features(getArtemisMQKarafFeatureUrl(version), featureArray.toArray(new String[featureArray.size()])));

      testOptions(optionList);

      return optionList.toArray(new Option[optionList.size()]);
   }

   public UrlReference getArtemisMQKarafFeatureUrl(String version) {
      String type = "xml/features";
      UrlReference urlReference;

      if (version == null) {
         urlReference = mavenBundle().groupId("org.apache.activemq").
            artifactId("artemis-features").versionAsInProject().type(type);
      } else {
         urlReference = mavenBundle().groupId("org.apache.activemq").
            artifactId("artemis-features").version(version).type(type);
      }
      LOG.info("FeatureURL: " + urlReference.getURL());
      return urlReference;
   }

   ExecutorService executor = Executors.newCachedThreadPool();

   @Inject
   BundleContext bundleContext;

   @Inject
   SessionFactory sessionFactory;

   protected String executeCommand(final String command, final Long timeout, final Boolean silent) {
      String response;
      final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      final PrintStream printStream = new PrintStream(byteArrayOutputStream);
      final Session commandSession = sessionFactory.create(System.in, printStream, printStream);
      commandSession.put("APPLICATION", System.getProperty("karaf.name", "root"));
      commandSession.put("USER", USER);
      FutureTask<String> commandFuture = new FutureTask<>(new Callable<String>() {
         @Override
         public String call() {

            Subject subject = new Subject();
            subject.getPrincipals().add(new UserPrincipal("admin"));
            subject.getPrincipals().add(new RolePrincipal("admin"));
            subject.getPrincipals().add(new RolePrincipal("manager"));
            subject.getPrincipals().add(new RolePrincipal("viewer"));
            return Subject.doAs(subject, new PrivilegedAction<String>() {
               @Override
               public String run() {
                  try {
                     if (!silent) {
                        System.out.println(command);
                        System.out.flush();
                     }
                     commandSession.execute(command);
                  } catch (Exception e) {
                     e.printStackTrace(System.err);
                  }
                  printStream.flush();
                  return byteArrayOutputStream.toString();
               }
            });
         }
      });

      try {
         executor.submit(commandFuture);
         response = commandFuture.get(timeout, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
         e.printStackTrace(System.err);
         response = "SHELL COMMAND TIMED OUT: ";
      }
      LOG.info("Execute: " + command + " - Response:" + response);
      return response;
   }

   protected String executeCommand(final String command) {
      return executeCommand(command, COMMAND_TIMEOUT, false);
   }

   protected boolean withinReason(Callable<Boolean> callable) throws Throwable {
      long max = System.currentTimeMillis() + ASSERTION_TIMEOUT;
      while (true) {
         try {
            return callable.call();
         } catch (Throwable t) {
            if (System.currentTimeMillis() < max) {
               TimeUnit.SECONDS.sleep(1);
               continue;
            } else {
               throw t;
            }
         }
      }
   }

   public boolean verifyBundleInstalled(final String bundleName) throws Exception {
      boolean found = false;
      for (Bundle bundle : bundleContext.getBundles()) {
         LOG.debug("Checking: " + bundle.getSymbolicName());
         if (bundle.getSymbolicName().contains(bundleName)) {
            found = true;
            break;
         }
      }
      return found;
   }

   protected Object waitForService(String filter, long timeout) throws InvalidSyntaxException, InterruptedException {
      ServiceTracker<Object, Object> st = new ServiceTracker<>(bundleContext, bundleContext.createFilter(filter), null);
      try {
         st.open();
         return st.waitForService(timeout);
      } finally {
         st.close();
      }
   }

}
