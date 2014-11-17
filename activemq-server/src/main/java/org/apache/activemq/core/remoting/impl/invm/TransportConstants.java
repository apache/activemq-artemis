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
package org.apache.activemq6.core.remoting.impl.invm;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.activemq6.api.config.HornetQDefaultConfiguration;

/**
 * A TransportConstants
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:mtaylor@redhat.com">Martyn Taylor</a>
 *
 */
public final class TransportConstants
{
   public static final String SERVER_ID_PROP_NAME = "server-id";

   public static final int DEFAULT_SERVER_ID = 0;

   public static final Set<String> ALLOWABLE_CONNECTOR_KEYS;

   public static final Set<String> ALLOWABLE_ACCEPTOR_KEYS;

   static
   {
      Set<String> allowableAcceptorKeys = new HashSet<String>();
      allowableAcceptorKeys.add(TransportConstants.SERVER_ID_PROP_NAME);
      allowableAcceptorKeys.add(org.apache.activemq6.core.remoting.impl.netty.TransportConstants.CLUSTER_CONNECTION);
      allowableAcceptorKeys.add(HornetQDefaultConfiguration.getPropMaskPassword());
      allowableAcceptorKeys.add(HornetQDefaultConfiguration.getPropPasswordCodec());

      ALLOWABLE_ACCEPTOR_KEYS = Collections.unmodifiableSet(allowableAcceptorKeys);

      Set<String> allowableConnectorKeys = new HashSet<String>();
      allowableConnectorKeys.add(TransportConstants.SERVER_ID_PROP_NAME);
      allowableConnectorKeys.add(HornetQDefaultConfiguration.getPropMaskPassword());
      allowableConnectorKeys.add(HornetQDefaultConfiguration.getPropPasswordCodec());

      ALLOWABLE_CONNECTOR_KEYS = Collections.unmodifiableSet(allowableConnectorKeys);
   }

   private TransportConstants()
   {
      // Utility class
   }
}
