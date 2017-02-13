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
package org.apache.activemq.artemis.tests.integration.cli;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.text.MessageFormat;
import java.util.EnumSet;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.cli.commands.AbstractAction;
import org.apache.activemq.artemis.cli.commands.ActionContext;
import org.apache.activemq.artemis.cli.commands.address.CreateAddress;
import org.apache.activemq.artemis.cli.commands.address.DeleteAddress;
import org.apache.activemq.artemis.cli.commands.address.ShowAddress;
import org.apache.activemq.artemis.cli.commands.address.UpdateAddress;
import org.apache.activemq.artemis.core.config.DivertConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.tests.util.JMSTestBase;
import org.junit.Before;
import org.junit.Test;

public class AddressCommandTest extends JMSTestBase {

   //the command
   private ByteArrayOutputStream output;
   private ByteArrayOutputStream error;

   @Before
   @Override
   public void setUp() throws Exception {
      super.setUp();
      this.output = new ByteArrayOutputStream(1024);
      this.error = new ByteArrayOutputStream(1024);
   }

   @Test
   public void testCreateAddress() throws Exception {
      String address = "address";
      CreateAddress command = new CreateAddress();
      command.setName(address);
      command.setAnycast(true);
      command.setMulticast(true);
      command.execute(new ActionContext(System.in, new PrintStream(output), new PrintStream(error)));
      checkExecutionPassed(command);
      AddressInfo addressInfo = server.getAddressInfo(new SimpleString(address));
      assertNotNull(addressInfo);
      assertTrue(addressInfo.getRoutingTypes().contains(RoutingType.ANYCAST));
      assertTrue(addressInfo.getRoutingTypes().contains(RoutingType.MULTICAST));
   }

   @Test
   public void testCreateAddressAlreadyExistsShowsError() throws Exception {
      String address = "address";
      CreateAddress command = new CreateAddress();
      command.setName(address);
      command.execute(new ActionContext(System.in, new PrintStream(output), new PrintStream(error)));
      checkExecutionPassed(command);
      assertNotNull(server.getAddressInfo(new SimpleString(address)));

      command.execute(new ActionContext(System.in, new PrintStream(output), new PrintStream(error)));
      checkExecutionFailure(command, "Address already exists");
   }

   @Test
   public void testDeleteAddress() throws Exception {
      String address = "address";
      CreateAddress command = new CreateAddress();
      command.setName(address);
      command.execute(new ActionContext());
      assertNotNull(server.getAddressInfo(new SimpleString(address)));

      DeleteAddress deleteAddress = new DeleteAddress();
      deleteAddress.setName(address);
      deleteAddress.execute(new ActionContext(System.in, new PrintStream(output), new PrintStream(error)));
      checkExecutionPassed(deleteAddress);
      assertNull(server.getAddressInfo(new SimpleString(address)));
   }

   @Test
   public void testDeleteAddressDoesNotExistsShowsError() throws Exception {
      String address = "address";
      DeleteAddress deleteAddress = new DeleteAddress();
      deleteAddress.setName(address);
      deleteAddress.execute(new ActionContext(System.in, new PrintStream(output), new PrintStream(error)));
      checkExecutionFailure(deleteAddress, "Address Does Not Exist");
   }

   @Test
   public void testFailDeleteAddressWhenExistsQueues() throws Exception {
      final String addressName = "address";
      final SimpleString addressSimpleString = new SimpleString(addressName);
      final AddressInfo addressInfo = new AddressInfo(addressSimpleString, EnumSet.of(RoutingType.ANYCAST, RoutingType.MULTICAST));
      server.addAddressInfo(addressInfo);
      server.createQueue(addressSimpleString, RoutingType.MULTICAST, new SimpleString("queue1"), null, true, false);

      final DeleteAddress deleteAddress = new DeleteAddress();
      deleteAddress.setName(addressName);
      deleteAddress.execute(new ActionContext(System.in, new PrintStream(output), new PrintStream(error)));
      checkExecutionFailure(deleteAddress, "Address " + addressName + " has bindings");
   }

   @Test
   public void testShowAddress() throws Exception {
      String address = "address";
      CreateAddress command = new CreateAddress();
      command.setName(address);
      command.execute(new ActionContext());
      assertNotNull(server.getAddressInfo(new SimpleString(address)));

      ShowAddress showAddress = new ShowAddress();
      showAddress.setName(address);
      showAddress.execute(new ActionContext(System.in, new PrintStream(output), new PrintStream(error)));
      System.out.println(output.toString());
   }

   @Test
   public void testShowAddressDoesNotExist() throws Exception {
      String address = "address";
      ShowAddress showAddress = new ShowAddress();
      showAddress.setName(address);
      showAddress.execute(new ActionContext(System.in, new PrintStream(output), new PrintStream(error)));
      checkExecutionFailure(showAddress, "Address Does Not Exist");
   }

   @Test
   public void testShowAddressBindings() throws Exception {

      // Create bindings
      SimpleString address = new SimpleString("address");
      server.addAddressInfo(new AddressInfo(address, RoutingType.MULTICAST));
      server.createQueue(address, RoutingType.MULTICAST, new SimpleString("queue1"), null, true, false);
      server.createQueue(address, RoutingType.MULTICAST, new SimpleString("queue2"), null, true, false);
      server.createQueue(address, RoutingType.MULTICAST, new SimpleString("queue3"), null, true, false);

      DivertConfiguration divertConfiguration = new DivertConfiguration();
      divertConfiguration.setName(address.toString());
      divertConfiguration.setAddress(address.toString());
      server.deployDivert(divertConfiguration);

      ShowAddress showAddress = new ShowAddress();
      showAddress.setName(address.toString());
      showAddress.setBindings(true);
      showAddress.execute(new ActionContext(System.in, new PrintStream(output), new PrintStream(error)));
      System.out.println(output.toString());
   }

   @Test
   public void testUpdateAddressRoutingTypes() throws Exception {
      final String addressName = "address";
      final SimpleString address = new SimpleString(addressName);
      server.addAddressInfo(new AddressInfo(address, RoutingType.ANYCAST));

      final UpdateAddress updateAddress = new UpdateAddress();
      updateAddress.setName(addressName);
      updateAddress.setAnycast(true);
      updateAddress.setMulticast(true);
      updateAddress.execute(new ActionContext(System.in, new PrintStream(output), new PrintStream(error)));
      checkExecutionPassed(updateAddress);

      final AddressInfo addressInfo = server.getAddressInfo(address);
      assertNotNull(addressInfo);
      assertEquals(EnumSet.of(RoutingType.ANYCAST, RoutingType.MULTICAST), addressInfo.getRoutingTypes());
   }

   @Test
   public void testFailUpdateAddressDoesNotExist() throws Exception {
      final String addressName = "address";
      final UpdateAddress updateAddress = new UpdateAddress();
      updateAddress.setName(addressName);
      updateAddress.execute(new ActionContext(System.in, new PrintStream(output), new PrintStream(error)));
      checkExecutionFailure(updateAddress, "Address Does Not Exist");
   }

   @Test
   public void testFailUpdateAddressRoutingTypesWhenExistsQueues() throws Exception {
      final String addressName = "address";
      final SimpleString addressSimpleString = new SimpleString(addressName);
      final AddressInfo addressInfo = new AddressInfo(addressSimpleString, EnumSet.of(RoutingType.ANYCAST, RoutingType.MULTICAST));
      server.addAddressInfo(addressInfo);
      server.createQueue(addressSimpleString, RoutingType.MULTICAST, new SimpleString("queue1"), null, true, false);

      final UpdateAddress updateAddress = new UpdateAddress();
      updateAddress.setName(addressName);
      updateAddress.setAnycast(true);
      updateAddress.setMulticast(false);
      updateAddress.execute(new ActionContext(System.in, new PrintStream(output), new PrintStream(error)));

      final String expectedErrorMessage = MessageFormat.format("Can''t remove routing type {0}, queues exists for address: {1}. Please delete queues before removing this routing type.", RoutingType.MULTICAST, addressName);
      checkExecutionFailure(updateAddress, expectedErrorMessage);
   }

   private void checkExecutionPassed(AbstractAction command) throws Exception {
      String fullMessage = output.toString();
      System.out.println("output: " + fullMessage);
      assertTrue(fullMessage, fullMessage.contains("successfully"));
   }

   private void checkExecutionFailure(AbstractAction command, String message) throws Exception {
      String fullMessage = error.toString();
      System.out.println("error: " + fullMessage);
      assertTrue(fullMessage, fullMessage.contains(message));
   }
}
