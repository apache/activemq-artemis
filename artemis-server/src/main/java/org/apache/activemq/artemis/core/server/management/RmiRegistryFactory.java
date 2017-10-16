/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.core.server.management;

import java.net.UnknownHostException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

public class RmiRegistryFactory {

   private int port = Registry.REGISTRY_PORT;
   private Registry registry;
   /**
    * @return the port
    */
   public int getPort() {
      return port;
   }

   /**
    * @param port the port to set
    */
   public void setPort(int port) {
      this.port = port;
   }

   public Object getObject() throws Exception {
      return registry;
   }

   public void init() throws RemoteException, UnknownHostException {
      registry = LocateRegistry.createRegistry(port);
   }

   public void destroy() throws RemoteException {
      if (registry != null) {
         UnicastRemoteObject.unexportObject(registry, true);
      }
   }
}
