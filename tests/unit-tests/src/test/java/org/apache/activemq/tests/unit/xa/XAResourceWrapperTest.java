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

package org.apache.activemq.tests.unit.xa;

import javax.transaction.xa.XAResource;

import org.apache.activemq.ra.HornetQRAXAResource;
import org.apache.activemq.ra.HornetQXAResourceWrapper;
import org.apache.activemq.tests.util.UnitTestCase;
import org.jboss.tm.XAResourceWrapper;
import org.junit.Test;

/**
 * @author <a href="mailto:mtaylor@redhat.com">Martyn Taylor</a>
 */

public class XAResourceWrapperTest extends UnitTestCase
{
   @Test
   public void testXAResourceWrapper()
   {
      String jndiName = "java://jmsXA";
      String nodeId = "0";
      XAResource xaResource = new HornetQRAXAResource(null, null);
      XAResourceWrapper xaResourceWrapper = new HornetQXAResourceWrapper(xaResource, jndiName, nodeId);

      assertEquals(xaResource, xaResourceWrapper.getResource());

      String expectedJndiNodeId = jndiName + " NodeId:" + nodeId;
      assertEquals(expectedJndiNodeId, xaResourceWrapper.getJndiName());
   }
}
