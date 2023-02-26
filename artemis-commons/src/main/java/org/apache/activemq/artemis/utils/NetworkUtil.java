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

package org.apache.activemq.artemis.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * This is a wrapper class of some InetAddress methods
 * so we can mock them with static values in test environment
 */
public class NetworkUtil {

   public static final InetAddressWrapper[] EMPTY_ADDRESSES = {};
   public static TestMode testMode = null;

   public static AddressWrapper[] getAllByName(String hostName) throws UnknownHostException {
      if (testMode != null) {
         return testMode.getAllByName(hostName);
      }
      InetAddress[] addresses = InetAddress.getAllByName(hostName);
      if (addresses != null && addresses.length > 0) {
         InetAddressWrapper[] wrappers = new InetAddressWrapper[addresses.length];
         for (int i = 0; i < addresses.length; ++i) {
            wrappers[i] = new InetAddressWrapper(addresses[i]);
         }
         return wrappers;
      }
      return EMPTY_ADDRESSES;
   }

   //It compares two different hostnames by their IP addresses using InetAddress.
   //Callers make sure the hostnaes not to be null values
   //It wraps the InetAddress methods to we can mock it for testing.
   public static boolean isSameHost(String hostParam, String otherHost) throws UnknownHostException {
      AddressWrapper[] addresses0 = getAllByName(hostParam);
      AddressWrapper[] addresses1 = getAllByName(otherHost);
      for (AddressWrapper addr0 : addresses0) {
         for (AddressWrapper addr1 : addresses1) {
            if (addr0.getHostAddress().equals(addr1.getHostAddress())) {
               return true;
            }
         }
      }
      return false;
   }

   public interface AddressWrapper {
      String getHostAddress();
   }

   public static class InetAddressWrapper implements AddressWrapper {

      InetAddress address;

      public InetAddressWrapper(InetAddress address) {
         this.address = address;
      }

      @Override
      public String getHostAddress() {
         return this.address.getHostAddress();
      }
   }

   public interface TestMode {
      AddressWrapper[] getAllByName(String hostName) throws UnknownHostException;
   }
}
