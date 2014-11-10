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

package org.hornetq.spi.core.remoting;

import java.util.Map;

/**
 * A Connector is used by the client for creating and controlling a connection.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author Clebert Suconic
 */
public interface Connector
{
   /**
    * starts the connector
    */
   void start();

   /**
    * closes the connector
    */
   void close();

   /**
    * returns true if the connector is started, oterwise false.
    *
    * @return true if the connector is started
    */
   boolean isStarted();

   /**
    * Create and return a connection from this connector.
    * <p>
    * This method must NOT throw an exception if it fails to create the connection
    * (e.g. network is not available), in this case it MUST return null
    *
    * @return The connection, or null if unable to create a connection (e.g. network is unavailable)
    */
   Connection createConnection();

   /**
    * If the configuration is equivalent to this connector, which means
    * if the parameter configuration is used to create a connection to a target
    * node, it will be the same node as of the connections made with this connector.
    * @param configuration
    * @return true means the configuration is equivalent to the connector. false otherwise.
    */
   boolean isEquivalent(Map<String, Object> configuration);
}
