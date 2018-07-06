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
package org.apache.activemq.artemis.utils;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import sun.reflect.CallerSensitive;

public class AtomicBooleanFieldUpdater<T> {

   /**
    * Creates and returns an updater for objects with the given field.
    * The Class argument is needed to check that reflective types and
    * generic types match.
    *
    * @param tclass the class of the objects holding the field
    * @param fieldName the name of the field to be updated
    * @param <U> the type of instances of tclass
    * @return the updater
    * @throws IllegalArgumentException if the field is not a
    * volatile long type
    * @throws RuntimeException with a nested reflection-based
    * exception if the class does not hold field or is the wrong type,
    * or the field is inaccessible to the caller according to Java language
    * access control
    */
   @CallerSensitive
   public static <U> AtomicBooleanFieldUpdater<U> newUpdater(Class<U> tclass, String fieldName) {
      return new AtomicBooleanFieldUpdater<>(AtomicIntegerFieldUpdater.newUpdater(tclass, fieldName));
   }

   private static int toInt(boolean value) {
      return value ? 1 : 0;
   }

   private static boolean toBoolean(int value) {
      return value != 0;
   }

   private final AtomicIntegerFieldUpdater<T> atomicIntegerFieldUpdater;

   /**
    * Protected do-nothing constructor for use by subclasses.
    */
   protected AtomicBooleanFieldUpdater(AtomicIntegerFieldUpdater<T> atomicIntegerFieldUpdater) {
      this.atomicIntegerFieldUpdater = atomicIntegerFieldUpdater;
   }

   /**
    * Atomically sets the field of the given object managed by this updater
    * to the given updated value if the current value {@code ==} the
    * expected value. This method is guaranteed to be atomic with respect to
    * other calls to {@code compareAndSet} and {@code set}, but not
    * necessarily with respect to other changes in the field.
    *
    * @param obj An object whose field to conditionally set
    * @param expect the expected value
    * @param update the new value
    * @return {@code true} if successful
    * @throws ClassCastException if {@code obj} is not an instance
    * of the class possessing the field established in the constructor
    */
   public boolean compareAndSet(T obj, boolean expect, boolean update) {
      return atomicIntegerFieldUpdater.compareAndSet(obj, toInt(expect), toInt(update));
   }



   /**
    * Atomically sets the field of the given object managed by this updater
    * to the given updated value if the current value {@code ==} the
    * expected value. This method is guaranteed to be atomic with respect to
    * other calls to {@code compareAndSet} and {@code set}, but not
    * necessarily with respect to other changes in the field.
    *
    * <p><a href="package-summary.html#weakCompareAndSet">May fail
    * spuriously and does not provide ordering guarantees</a>, so is
    * only rarely an appropriate alternative to {@code compareAndSet}.
    *
    * @param obj An object whose field to conditionally set
    * @param expect the expected value
    * @param update the new value
    * @return {@code true} if successful
    * @throws ClassCastException if {@code obj} is not an instance
    * of the class possessing the field established in the constructor
    */
   public boolean weakCompareAndSet(T obj, boolean expect, boolean update) {
      return atomicIntegerFieldUpdater.weakCompareAndSet(obj, toInt(expect), toInt(update));
   }

   /**
    * Sets the field of the given object managed by this updater to the
    * given updated value. This operation is guaranteed to act as a volatile
    * store with respect to subsequent invocations of {@code compareAndSet}.
    *
    * @param obj An object whose field to set
    * @param newValue the new value
    */
   public void set(T obj, boolean newValue) {
      atomicIntegerFieldUpdater.set(obj, toInt(newValue));
   }

   /**
    * Eventually sets the field of the given object managed by this
    * updater to the given updated value.
    *
    * @param obj An object whose field to set
    * @param newValue the new value
    * @since 1.6
    */
   public void lazySet(T obj, boolean newValue) {
      atomicIntegerFieldUpdater.lazySet(obj, toInt(newValue));
   }

   /**
    * Gets the current value held in the field of the given object managed
    * by this updater.
    *
    * @param obj An object whose field to get
    * @return the current value
    */
   public boolean get(T obj) {
      return toBoolean(atomicIntegerFieldUpdater.get(obj));
   }

   /**
    * Atomically sets the field of the given object managed by this updater
    * to the given value and returns the old value.
    *
    * @param obj An object whose field to get and set
    * @param newValue the new value
    * @return the previous value
    */
   public boolean getAndSet(T obj, boolean newValue) {
      return toBoolean(atomicIntegerFieldUpdater.getAndSet(obj, toInt(newValue)));
   }

   public String toString(T obj) {
      return Boolean.toString(get(obj));
   }
}
