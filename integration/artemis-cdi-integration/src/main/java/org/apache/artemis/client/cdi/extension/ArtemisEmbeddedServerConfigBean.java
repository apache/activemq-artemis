/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.artemis.client.cdi.extension;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.InjectionPoint;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMAcceptorFactory;

import static java.util.Collections.emptySet;

public class ArtemisEmbeddedServerConfigBean implements Bean<Configuration> {

   @Override
   public Class<?> getBeanClass() {
      return ConfigurationImpl.class;
   }

   @Override
   public Set<InjectionPoint> getInjectionPoints() {
      return emptySet();
   }

   @Override
   public boolean isNullable() {
      return false;
   }

   @Override
   public Configuration create(CreationalContext<Configuration> creationalContext) {
      Map<String, Object> params = new HashMap<>();
      params.put(org.apache.activemq.artemis.core.remoting.impl.invm.TransportConstants.SERVER_ID_PROP_NAME, "1");
      return new ConfigurationImpl().setSecurityEnabled(false).setPersistenceEnabled(false).setJMXManagementEnabled(false).addAcceptorConfiguration(new TransportConfiguration(InVMAcceptorFactory.class.getName(), params));
   }

   @Override
   public void destroy(Configuration configuration, CreationalContext<Configuration> creationalContext) {
   }

   @Override
   public Set<Type> getTypes() {
      Set<Type> types = new HashSet<>();
      types.add(ConfigurationImpl.class);
      types.add(Configuration.class);
      return types;
   }

   @Override
   public Set<Annotation> getQualifiers() {
      Set<Annotation> qualifiers = new HashSet<>();
      qualifiers.add(AnyLiteral.INSTANCE);
      qualifiers.add(DefaultLiteral.INSTANCE);
      return qualifiers;

   }

   @Override
   public Class<? extends Annotation> getScope() {
      return ApplicationScoped.class;
   }

   @Override
   public String getName() {
      return null;
   }

   @Override
   public Set<Class<? extends Annotation>> getStereotypes() {
      return emptySet();
   }

   @Override
   public boolean isAlternative() {
      return false;
   }
}
