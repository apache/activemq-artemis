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
package org.apache.activemq.artemis.core.settings.impl;

import java.util.Objects;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.settings.HierarchicalRepositoryChangeListener;

public abstract class NamedHierarchicalRepositoryChangeListener implements HierarchicalRepositoryChangeListener {

   private SimpleString name;

   public NamedHierarchicalRepositoryChangeListener(SimpleString name) {
      this.name = Objects.requireNonNull(name);
   }

   SimpleString getName() {
      return this.name;
   }

   @Override
   public int hashCode() {
      return name.hashCode();
   }

   @Override
   public boolean equals(Object o) {
      if (this == o)
         return true;
      if (o == null || getClass() != o.getClass())
         return false;

      NamedHierarchicalRepositoryChangeListener that = (NamedHierarchicalRepositoryChangeListener) o;

      if (!Objects.equals(name, that.name)) {
         return false;
      } else {
         return true;
      }
   }
}
