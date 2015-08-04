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
package org.apache.activemq.artemis.selector.filter;

/**
 * Represents a property expression
 *
 * @version $Revision: 1.5 $
 */
public class PropertyExpression implements Expression {

   private final String name;

   public PropertyExpression(String name) {
      this.name = name;
   }

   public Object evaluate(Filterable message) throws FilterException {
      return message.getProperty(name);
   }

   public String getName() {
      return name;
   }

   /**
    * @see java.lang.Object#toString()
    */
   public String toString() {
      return name;
   }

   /**
    * @see java.lang.Object#hashCode()
    */
   public int hashCode() {
      return name.hashCode();
   }

   /**
    * @see java.lang.Object#equals(java.lang.Object)
    */
   public boolean equals(Object o) {
      if (o == null || !this.getClass().equals(o.getClass())) {
         return false;
      }
      return name.equals(((PropertyExpression) o).name);

   }

}
