/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.core.management.impl;

import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMAcceptorFactory;
import org.apache.activemq.artemis.spi.core.remoting.Acceptor;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class AcceptorControlImplTest extends Assert {

   @Test
   public void testParameters() throws Exception {
      HashMap<String, Object> params = new HashMap<>();
      params.put("param", RandomUtil.randomString());

      HashMap<String, Object> extraProps = new HashMap<>();
      extraProps.put("extraProp", RandomUtil.randomString());

      Acceptor acceptor = Mockito.mock(Acceptor.class);
      StorageManager storageManager = Mockito.mock(StorageManager.class);
      TransportConfiguration transportConfiguration = new TransportConfiguration(
         InVMAcceptorFactory.class.getName(), params, RandomUtil.randomString(), extraProps);
      AcceptorControlImpl acceptorControl = new AcceptorControlImpl(acceptor, storageManager, transportConfiguration);

      Map<String, Object> acceptorPrameters = acceptorControl.getParameters();
      Assert.assertEquals(params.get("param"), acceptorPrameters.get("param"));
      Assert.assertEquals(extraProps.get("extraProp"), acceptorPrameters.get("extraProp"));
   }
}
