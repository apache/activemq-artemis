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
package org.apache.activemq.artemis.tests.integration.rest;

import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;

@XmlRootElement(name = "order")
public class Order implements Serializable {

   private static final long serialVersionUID = -3462346058107018735L;
   private String name;
   private String amount;
   private String item;

   public Order() {
   }

   public Order(String name, String amount, String item) {
      assert (name != null);
      assert (amount != null);
      assert (item != null);

      this.name = name;
      this.amount = amount;
      this.item = item;
   }

   public String getName() {
      return name;
   }

   public void setName(String name) {
      this.name = name;
   }

   public String getAmount() {
      return amount;
   }

   public void setAmount(String amount) {
      this.amount = amount;
   }

   public String getItem() {
      return item;
   }

   public void setItem(String item) {
      this.item = item;
   }

   @Override
   public boolean equals(Object other) {
      if (!(other instanceof Order)) {
         return false;
      }
      Order order = (Order) other;
      return name.equals(order.name) && amount.equals(order.amount) && item.equals(order.item);
   }

   @Override
   public int hashCode() {
      return name.hashCode() + amount.hashCode() + item.hashCode();
   }
}
