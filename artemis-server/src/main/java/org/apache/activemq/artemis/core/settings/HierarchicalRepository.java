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
package org.apache.activemq.artemis.core.settings;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * allows objects to be mapped against a regex pattern and held in order in a list
 */
//tmp comment: Can we use AddressInfo as the 'match' key?
public interface HierarchicalRepository<T> {

   void disableListeners();

   void enableListeners();

   /**
    * Add a new match to the repository
    *
    * @param match The regex to use to match against
    * @param value the value to hold against the match
    */
   void addMatch(String match, T value);

   void addMatch(String match, T value, boolean immutableMatch);

   /**
    * return the value held against the nearest match
    *
    * @param match the match to look for
    * @return the value
    */
   T getMatch(String match);

   /**
    * Return a list of Values being added
    *
    * @return
    */
   List<T> values();

   /**
    * set the default value to fallback to if none found
    *
    * @param defaultValue the value
    */
   void setDefault(T defaultValue);

   /**
    * remove a match from the repository
    *
    * @param match the match to remove
    */
   void removeMatch(String match);

   /**
    * register a listener to listen for changes in the repository
    *
    * @param listener
    */
   void registerListener(HierarchicalRepositoryChangeListener listener);

   /**
    * unregister a listener
    *
    * @param listener
    */
   void unRegisterListener(HierarchicalRepositoryChangeListener listener);

   /**
    * clear the repository
    */
   void clear();

   void swap(Set<Map.Entry<String, T>> entries);

   /**
    * Removes all listeners.
    */
   void clearListeners();

   /**
    * Clears the cache.
    */
   void clearCache();

   int getCacheSize();
}
