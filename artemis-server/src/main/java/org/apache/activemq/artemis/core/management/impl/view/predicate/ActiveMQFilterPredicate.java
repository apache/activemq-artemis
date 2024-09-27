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
import java.util.function.Predicate;


public class ActiveMQFilterPredicate<T> implements Predicate<T> {

   public enum Operation {
      CONTAINS, NOT_CONTAINS, EQUALS, NOT_EQUALS, GREATER_THAN, LESS_THAN;
   }

   protected String field;

   protected String value;

   protected Operation operation;

   public ActiveMQFilterPredicate() {
   }

   @Override
   public boolean test(T input) {
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
      if (operation != null && !operation.isBlank()) {
         this.operation = Operation.valueOf(operation);
      }
   }

   public boolean matches(Object field) {
      if (operation != null) {
         switch (operation) {
            case EQUALS:
               return equals(field, value);
            case NOT_EQUALS:
               return !equals(field, value);
            case CONTAINS:
               return contains(field, value);
            case NOT_CONTAINS:
               return !contains(field, value);
            case GREATER_THAN:
               return false;
            case LESS_THAN:
               return false;
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

   public boolean matches(long field) {
      long longValue;
      if (operation != null) {

         try {
            longValue = Long.parseLong(value);
         } catch (NumberFormatException ex) {
            //cannot compare
            if (operation == Operation.NOT_EQUALS || operation == Operation.NOT_CONTAINS) {
               return true;
            } else {
               return false;
            }
         }

         switch (operation) {
            case EQUALS:
               return field == longValue;
            case NOT_EQUALS:
               return field != longValue;
            case CONTAINS:
               return false;
            case NOT_CONTAINS:
               return true;
            case LESS_THAN:
               return field < longValue;
            case GREATER_THAN:
               return field > longValue;
         }
      }
      return true;
   }

   public boolean matches(int field) {
      int intValue;
      if (operation != null) {

         try {
            intValue = Integer.parseInt(value);
         } catch (NumberFormatException ex) {
            //cannot compare
            if (operation == Operation.NOT_EQUALS || operation == Operation.NOT_CONTAINS) {
               return true;
            } else {
               return false;
            }
         }

         switch (operation) {
            case EQUALS:
               return field == intValue;
            case NOT_EQUALS:
               return field != intValue;
            case CONTAINS:
               return false;
            case NOT_CONTAINS:
               return true;
            case LESS_THAN:
               return field < intValue;
            case GREATER_THAN:
               return field > intValue;
         }
      }
      return true;
   }

   public boolean matches(float field) {
      float floatValue;
      if (operation != null) {

         try {
            floatValue = Float.parseFloat(value);
         } catch (NumberFormatException ex) {
            //cannot compare
            if (operation == Operation.NOT_EQUALS || operation == Operation.NOT_CONTAINS) {
               return true;
            } else {
               return false;
            }
         }

         switch (operation) {
            case EQUALS:
               return field == floatValue;
            case NOT_EQUALS:
               return field != floatValue;
            case CONTAINS:
               return false;
            case NOT_CONTAINS:
               return true;
            case LESS_THAN:
               return field < floatValue;
            case GREATER_THAN:
               return field > floatValue;
         }
      }
      return true;
   }

   private boolean equals(Object field, Object value) {
      if (field == null) {
         return (value == null || value.equals(""));
      }
      return field.toString().equals(value);
   }

   private boolean contains(Object field, Object value) {
      if (field == null) {
         return (value == null || value.equals(""));
      }
      return field.toString().contains(value.toString());
   }
}
