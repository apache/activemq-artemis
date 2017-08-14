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
package org.apache.activemq.artemis.core.management.impl.view.predicate;

import java.util.Collection;

import com.google.common.base.Predicate;

public class ActiveMQFilterPredicate<T> implements Predicate<T> {

   enum Operation {
      CONTAINS, EQUALS;
   }

   protected String field;

   protected String value;

   protected Operation operation;

   public static boolean contains(String field, String value) {
      return field.contains(value);
   }

   public ActiveMQFilterPredicate() {
   }

   @Override
   public boolean apply(T input) {
      return true;
   }

   public String getField() {
      return field;
   }

   public void setField(String field) {
      this.field = field;
   }

   public String getValue() {
      return value;
   }

   public void setValue(String value) {
      this.value = value;
   }

   public Operation getOperation() {
      return operation;
   }

   public void setOperation(String operation) {
      if (operation != null && !operation.equals("")) {
         this.operation = Operation.valueOf(operation);
      }
   }

   public boolean matches(Object field) {
      if (operation != null) {
         switch (operation) {
            case EQUALS:
               return equals(field, value);
            case CONTAINS:
               return contains(field, value);
         }
      }
      return true;
   }

   public boolean matchAny(Collection objects) {
      for (Object o : objects) {
         if (matches(o))
            return true;
      }
      return false;
   }

   private boolean equals(Object field, Object value) {
      if (field == null) {
         return (value.equals("") || value == null);
      }
      return field.toString().equals(value);
   }

   private boolean contains(Object field, Object value) {
      if (field == null) {
         return (value.equals("") || value == null);
      }
      return field.toString().contains(value.toString());
   }
}
