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
package org.apache.activemq.artemis.api.core;

import java.io.Serializable;

/**
 * A Pair is a holder for 2 objects.
 * <p>
 * This is a utility class.
 */
public final class Pair<A, B> implements Serializable {

   private static final long serialVersionUID = -2496357457812368127L;

   public Pair(final A a, final B b) {
      this.a = a;

      this.b = b;
   }

   private A a;

   private B b;

   private int hash = -1;

   @Override
   public int hashCode() {
      if (hash == -1) {
         if (a == null && b == null) {
            return super.hashCode();
         } else {
            hash = (a == null ? 0 : a.hashCode()) + 37 * (b == null ? 0 : b.hashCode());
         }
      }

      return hash;
   }

   @Override
   public boolean equals(final Object other) {
      if (other == this) {
         return true;
      }

      if (other instanceof Pair == false) {
         return false;
      }

      Pair<A, B> pother = (Pair<A, B>) other;

      return (pother.a == null ? a == null : pother.a.equals(a)) && (pother.b == null ? b == null : pother.b.equals(b));

   }

   @Override
   public String toString() {
      return "Pair[a=" + a + ", b=" + b + "]";
   }

   public void setA(A a) {
      hash = -1;
      this.a = a;
   }

   public A getA() {
      return a;
   }

   public void setB(B b) {
      hash = -1;
      this.b = b;
   }

   public B getB() {
      return b;
   }
}
