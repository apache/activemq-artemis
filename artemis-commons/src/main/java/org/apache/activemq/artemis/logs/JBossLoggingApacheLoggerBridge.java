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

package org.apache.activemq.artemis.logs;

import org.apache.commons.logging.Log;
import org.jboss.logging.Logger;

/**
 * There is an issue on the Testsuite, as it's not possible
 * to bring JBossLogging on the booting classLoader.
 * As a result classes like BeanUtils will be using apache logging,
 * which will not have the bootstrapping necessary.
 *
 * As a result BeanUtils is keeping showing log.debug into the test output and test console.
 *
 * This class will give an opportunity to the CI and our applications to use proper logging even on the testsuite.
 * */
public class JBossLoggingApacheLoggerBridge implements Log {

   final Logger bridgeLog;

   public JBossLoggingApacheLoggerBridge(Class clazz) {
      bridgeLog = Logger.getLogger(clazz);
   }

   public JBossLoggingApacheLoggerBridge(String name) {
      bridgeLog = Logger.getLogger(name);
   }

   @Override
   public void debug(Object message) {
      bridgeLog.debug(message);
   }

   @Override
   public void debug(Object message, Throwable t) {
      bridgeLog.debug(message, t);
   }

   @Override
   public void error(Object message) {
      bridgeLog.error(message);
   }

   @Override
   public void error(Object message, Throwable t) {
      bridgeLog.error(message, t);
   }

   @Override
   public void fatal(Object message) {
      bridgeLog.fatal(message);
   }

   @Override
   public void fatal(Object message, Throwable t) {
      bridgeLog.fatal(message, t);
   }

   @Override
   public void info(Object message) {
      bridgeLog.info(message);
   }

   @Override
   public void info(Object message, Throwable t) {
      bridgeLog.info(message, t);
   }

   @Override
   public boolean isDebugEnabled() {
      return bridgeLog.isDebugEnabled();
   }

   @Override
   public boolean isErrorEnabled() {
      return bridgeLog.isEnabled(Logger.Level.ERROR);
   }

   @Override
   public boolean isFatalEnabled() {
      return bridgeLog.isEnabled(Logger.Level.FATAL);
   }

   @Override
   public boolean isInfoEnabled() {
      return bridgeLog.isInfoEnabled();
   }

   @Override
   public boolean isTraceEnabled() {
      return bridgeLog.isTraceEnabled();
   }

   @Override
   public boolean isWarnEnabled() {
      return bridgeLog.isEnabled(Logger.Level.WARN);
   }

   @Override
   public void trace(Object message) {
      bridgeLog.trace(message);
   }

   @Override
   public void trace(Object message, Throwable t) {
      bridgeLog.trace(message, t);
   }

   @Override
   public void warn(Object message) {
      bridgeLog.warn(message);
   }

   @Override
   public void warn(Object message, Throwable t) {
      bridgeLog.warn(message, t);
   }
}
