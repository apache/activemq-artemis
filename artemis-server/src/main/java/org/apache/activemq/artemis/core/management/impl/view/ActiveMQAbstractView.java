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
package org.apache.activemq.artemis.core.management.impl.view;

import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.activemq.artemis.api.core.JsonUtil;
import org.apache.activemq.artemis.core.management.impl.view.predicate.ActiveMQFilterPredicate;
import org.apache.activemq.artemis.utils.JsonLoader;

public abstract class ActiveMQAbstractView<T> {

   private static final String FILTER_FIELD = "field";

   private static final String FILTER_OPERATION = "operation";

   private static final String FILTER_VALUE = "value";

   private static final String SORT_ORDER = "sortOrder";

   private static final String SORT_COLUMN = "sortColumn";

   protected Collection<T> collection;

   protected ActiveMQFilterPredicate<T> predicate;

   protected String sortColumn;

   protected String sortOrder;

   protected String options;

   public ActiveMQAbstractView() {
      this.sortColumn = getDefaultOrderColumn();
      this.sortOrder = "asc";
   }

   public void setCollection(Collection<T> collection) {
      this.collection = collection;
   }

   public String getResultsAsJson(int page, int pageSize) {
      JsonObjectBuilder obj = JsonLoader.createObjectBuilder();
      JsonArrayBuilder array = JsonLoader.createArrayBuilder();
      collection = collection.stream().filter(getPredicate()).collect(Collectors.toList());
      for (T element : getPagedResult(page, pageSize)) {
         JsonObjectBuilder jsonObjectBuilder = toJson(element);
         //toJson() may return a null
         if (jsonObjectBuilder != null) {
            array.add(jsonObjectBuilder);
         }
      }
      obj.add("data", array);
      obj.add("count", collection.size());
      return obj.build().toString();
   }

   public List<T> getPagedResult(int page, int pageSize) {
      List<T> builder = new ArrayList<>();
      int start = (page - 1) * pageSize;
      int end = Math.min(page * pageSize, collection.size());
      int i = 0;
      for (T e : collection.stream().sorted(getComparator()).collect(Collectors.toList())) {
         if (i >= start && i < end) {
            builder.add(e);
         }
         i++;
      }
      return Collections.unmodifiableList(builder);
   }

   public Predicate<T> getPredicate() {
      return predicate;
   }

   public Comparator<T> getComparator() {
      return (left, right) -> {
         try {
            Object leftValue = getField(left, sortColumn);
            Object rightValue = getField(right, sortColumn);
            if (leftValue instanceof Comparable && rightValue instanceof Comparable) {
               if (sortOrder.equals("desc")) {
                  return ((Comparable) rightValue).compareTo(leftValue);
               } else {
                  return ((Comparable) leftValue).compareTo(rightValue);
               }
            }
            return 0;
         } catch (Exception e) {
            //LOG.info("Exception sorting destinations", e);
            return 0;
         }
      };
   }

   abstract Object getField(T t, String fieldName);

   public void setOptions(String options) {
      JsonObject json = JsonUtil.readJsonObject(options);
      if (predicate != null) {
         predicate.setField(json.getString(FILTER_FIELD));
         predicate.setOperation(json.getString(FILTER_OPERATION));
         predicate.setValue(json.getString(FILTER_VALUE));
         if (json.containsKey(SORT_COLUMN) && json.containsKey(SORT_ORDER)) {
            this.sortColumn = json.getString(SORT_COLUMN);
            this.sortOrder = json.getString(SORT_ORDER);
         }
      }
   }

   public abstract Class getClassT();

   public abstract JsonObjectBuilder toJson(T obj);

   public abstract String getDefaultOrderColumn();

   /**
    * JsonObjectBuilder will throw an NPE if a null value is added.  For this reason we check for null explicitly when
    * adding objects.
    *
    * @param o
    * @return
    */
   protected String toString(Object o) {
      return o == null ? "" : o.toString();
   }
}
