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
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.airlift.airline.Arguments;
import io.airlift.airline.Option;
import org.apache.activemq.artemis.cli.CLIException;

public class InstallAbstract extends InputAbstract {

   @Arguments(description = "The instance directory to hold the broker's configuration and data. Path must be writable.", required = true)
   protected File directory;

   @Option(name = "--etc", description = "Directory where ActiveMQ configuration is located. Paths can be absolute or relative to artemis.instance directory ('etc' by default)")
   protected String etc = "etc";

   @Option(name = "--home", description = "Directory where ActiveMQ Artemis is installed")
   protected File home;

   @Option(name = "--encoding", description = "The encoding that text files should use. Default = UTF-8.")
   protected String encoding = "UTF-8";

   @Option(name = "--windows", description = "Force windows script creation. Default based on your actual system.")
   protected boolean windows = false;

   @Option(name = "--cygwin", description = "Force cygwin script creation. Default based on your actual system.")
   protected boolean cygwin = false;

   @Option(name = "--java-options", description = "Extra java options to be passed to the profile")
   protected String javaOptions = "";

   @Option(name = "--java-memory", description = "Define the -Xmx memory parameter for the broker. Default = '2G'")
   protected String javaMemory = "2G";


   public String getEncoding() {
      return encoding;
   }

   public void setEncoding(String encoding) {
      this.encoding = encoding;
   }

   public File getInstance() {
      return directory;
   }

   public void setInstance(File directory) {
      this.directory = directory;
   }

   public File getHome() {
      if (home == null) {
         home = new File(getBrokerHome());
      }
      return home;
   }

   protected boolean IS_WINDOWS;
   protected boolean IS_CYGWIN;

   public Object run(ActionContext context) throws Exception {
      IS_WINDOWS = windows | System.getProperty("os.name").toLowerCase().trim().startsWith("win");
      IS_CYGWIN = cygwin | IS_WINDOWS && "cygwin".equals(System.getenv("OSTYPE"));

      return null;
   }

   protected String applyFilters(String content, Map<String, String> filters) {
      if (filters != null) {
         for (Map.Entry<String, String> entry : filters.entrySet()) {
            content = replace(content, entry.getKey(), entry.getValue());
         }
      }
      return content;
   }

   protected String replace(String content, String key, String value) {
      return content.replaceAll(Pattern.quote(key), Matcher.quoteReplacement(value));
   }

   protected void copy(InputStream is, OutputStream os) throws IOException {
      byte[] buffer = new byte[1024 * 4];
      int c = is.read(buffer);
      while (c >= 0) {
         os.write(buffer, 0, c);
         c = is.read(buffer);
      }
   }

   protected void write(String source,
                      File target,
                      HashMap<String, String> filters,
                      boolean unixTarget, boolean force) throws Exception {
      if (target.exists() && !force) {
         throw new CLIException(String.format("The file '%s' already exists.  Use --force to overwrite.", target));
      }

      String content = readTextFile(source, filters);

      if (content == null) {
         new Exception(source + " not found").printStackTrace();
      }

      // and then writing out in the new target encoding..  Let's also replace \n with the values
      // that is correct for the current platform.
      String separator = unixTarget && IS_CYGWIN ? "\n" : System.getProperty("line.separator");
      content = content.replaceAll("\\r?\\n", Matcher.quoteReplacement(separator));
      ByteArrayInputStream in = new ByteArrayInputStream(content.getBytes(encoding));
      try (FileOutputStream fout = new FileOutputStream(target)) {
         copy(in, fout);
      }
   }

   protected String readTextFile(String source, Map<String, String> filters) throws IOException {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      try (InputStream in = openStream(source)) {
         if (in == null) {
            throw new IOException("could not find resource " + source);
         }
         copy(in, out);
      }
      return applyFilters(new String(out.toByteArray(), StandardCharsets.UTF_8), filters);
   }

   protected void write(String source, boolean force) throws IOException {
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

   protected InputStream openStream(String source) {
      return this.getClass().getResourceAsStream(source);
   }
}
