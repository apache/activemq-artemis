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

package org.apache.activemq.artemis.cli.commands;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;

import io.airlift.airline.Option;

public class OptionsUtil {

   private static void findAllOptions(Set<String> options, Class<? extends Action> command) {
      for (Field field  : command.getDeclaredFields()) {
         if (field.isAnnotationPresent(Option.class)) {
            Option annotation = field.getAnnotation(Option.class);
            String[] names = annotation.name();
            for (String n : names) {
               options.add(n);
            }
         }
      }
      Class parent = command.getSuperclass();
      if (Action.class.isAssignableFrom(parent)) {
         findAllOptions(options, parent);
      }
   }

   private static Set<String> findCommandOptions(Class<? extends Action> command) {
      Set<String> options = new HashSet<>();
      findAllOptions(options, command);

      return options;
   }

   public static void checkCommandOptions(Class<? extends Action> cmdClass, String[] options) throws InvalidOptionsError {
      Set<String> definedOptions = OptionsUtil.findCommandOptions(cmdClass);
      for (String opt : options) {
         if (opt.startsWith("--") && !"--".equals(opt.trim())) {
            int index = opt.indexOf("=");
            if (index > 0) {
               opt = opt.substring(0, index);
            }
            if (!definedOptions.contains(opt)) {
               throw new InvalidOptionsError("Found unexpected parameters: [" + opt + "]");
            }
         }
      }
   }
}
