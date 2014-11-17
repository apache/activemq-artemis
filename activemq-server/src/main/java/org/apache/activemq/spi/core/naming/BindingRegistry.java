/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.activemq6.spi.core.naming;

/**
 * Abstract interface for a registry to store endpoints like connection factories into.
 *
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public interface BindingRegistry
{
   /** The context used by the registry.
    *   This may be used to setup the JNDI Context on the JNDI Registry.
    *   We keep it as an object here as the interface needs to be generic
    *   as this could be reused by others Registries (e.g set/get the Map on MapRegistry)
    * @return
    */
   // XXX Unused?
   Object getContext();

   void setContext(Object ctx);

   Object lookup(String name);

   boolean bind(String name, Object obj);

   void unbind(String name);

   void close();
}
