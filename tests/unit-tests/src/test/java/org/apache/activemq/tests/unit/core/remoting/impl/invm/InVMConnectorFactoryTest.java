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

package org.apache.activemq.tests.unit.core.remoting.impl.invm;

import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.core.remoting.impl.invm.InVMConnector;
import org.apache.activemq.core.remoting.impl.invm.InVMConnectorFactory;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * @author <a href="mailto:mtaylor@redhat.com">Martyn Taylor</a>
 */

public class InVMConnectorFactoryTest
{
   @Test
   public void testCreateConnectorSetsDefaults()
   {
      // Test defaults are added when TransportConfig params are empty
      TransportConfiguration tc = new TransportConfiguration(InVMConnectorFactory.class.getName(), new HashMap<String, Object>());
      assertTrue(tc.getParams().equals(InVMConnector.DEFAULT_CONFIG));

      // Test defaults are added when TransportConfig params are null
      tc = new TransportConfiguration(InVMConnectorFactory.class.getName(), null);
      assertTrue(tc.getParams().equals(InVMConnector.DEFAULT_CONFIG));

      // Test defaults are added when TransportConfig params are null
      tc = new TransportConfiguration(InVMConnectorFactory.class.getName());
      assertTrue(tc.getParams().equals(InVMConnector.DEFAULT_CONFIG));

      // Test defaults are not set when TransportConfig params are not empty
      Map<String, Object> params = new HashMap<String, Object>();
      params.put("Foo", "Bar");
      tc = new TransportConfiguration(InVMConnectorFactory.class.getName(), params);
      assertTrue(tc.getParams().size() == 1);
      assertTrue(tc.getParams().containsKey("Foo"));
   }
}
