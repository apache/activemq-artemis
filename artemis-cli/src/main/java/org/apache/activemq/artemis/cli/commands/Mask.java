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

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.activemq.artemis.utils.DefaultSensitiveStringCodec;
import org.apache.activemq.artemis.utils.PasswordMaskingUtil;

@Command(name = "mask", description = "mask a password and print it out")
public class Mask implements Action {

   @Arguments(description = "The password to be masked", required = true)
   String password;

   @Option(name = "--hash", description = "whether to use hash (one-way), default false")
   boolean hash = false;

   @Option(name = "--key", description = "the key (Blowfish) to mask a password")
   String key;

   private DefaultSensitiveStringCodec codec;

   @Override
   public Object execute(ActionContext context) throws Exception {
      Map<String, String> params = new HashMap<>();

      if (hash) {
         params.put(DefaultSensitiveStringCodec.ALGORITHM, DefaultSensitiveStringCodec.ONE_WAY);
      }

      if (key != null) {
         if (hash) {
            context.out.println("Option --key ignored in case of hashing");
         } else {
            params.put(DefaultSensitiveStringCodec.BLOWFISH_KEY, key);
         }
      }

      codec = PasswordMaskingUtil.getDefaultCodec();
      codec.init(params);

      String masked = codec.encode(password);
      context.out.println("result: " + masked);
      return masked;
   }

   @Override
   public boolean isVerbose() {
      return false;
   }

   @Override
   public void setHomeValues(File brokerHome, File brokerInstance) {
   }

   @Override
   public String getBrokerInstance() {
      return null;
   }

   @Override
   public String getBrokerHome() {
      return null;
   }

   public void setPassword(String password) {
      this.password = password;
   }

   public void setHash(boolean hash) {
      this.hash = hash;
   }

   public void setKey(String key) {
      this.key = key;
   }

   public DefaultSensitiveStringCodec getCodec() {
      return codec;
   }

   @Override
   public void checkOptions(String[] options) throws InvalidOptionsError {
      OptionsUtil.checkCommandOptions(this.getClass(), options);
   }

}
