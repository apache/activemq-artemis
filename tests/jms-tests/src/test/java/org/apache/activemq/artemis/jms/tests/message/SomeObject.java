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
package org.apache.activemq.artemis.jms.tests.message;

import java.io.Serializable;

/**
 * ObjectMessageTest needed a simple class to test ClassLoadingIsolations
 */
public class SomeObject implements Serializable {

   private static final long serialVersionUID = -2939720794544432834L;

   int i;

   int j;

   public SomeObject(final int i, final int j) {
      this.i = i;
      this.j = j;
   }

   @Override
   public boolean equals(final Object o) {
      if (this == o) {
         return true;
      }
      if (o == null || getClass() != o.getClass()) {
         return false;
      }

      SomeObject that = (SomeObject) o;

      if (i != that.i) {
         return false;
      }
      if (j != that.j) {
         return false;
      }

      return true;
   }

   @Override
   public int hashCode() {
      int result;
      result = i;
      result = 31 * result + j;
      return result;
   }
}
