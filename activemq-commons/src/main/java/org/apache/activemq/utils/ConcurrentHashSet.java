/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.activemq6.utils;

import java.util.AbstractSet;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *
 * A ConcurrentHashSet.
 *
 * Offers same concurrency as ConcurrentHashMap but for a Set
 *
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1935 $</tt>
 *
 */
public class ConcurrentHashSet<E> extends AbstractSet<E> implements ConcurrentSet<E>
{
   private final ConcurrentMap<E, Object> theMap;

   private static final Object dummy = new Object();

   public ConcurrentHashSet()
   {
      theMap = new ConcurrentHashMap<E, Object>();
   }

   @Override
   public int size()
   {
      return theMap.size();
   }

   @Override
   public Iterator<E> iterator()
   {
      return theMap.keySet().iterator();
   }

   @Override
   public boolean isEmpty()
   {
      return theMap.isEmpty();
   }

   @Override
   public boolean add(final E o)
   {
      return theMap.put(o, ConcurrentHashSet.dummy) == null;
   }

   @Override
   public boolean contains(final Object o)
   {
      return theMap.containsKey(o);
   }

   @Override
   public void clear()
   {
      theMap.clear();
   }

   @Override
   public boolean remove(final Object o)
   {
      return theMap.remove(o) == ConcurrentHashSet.dummy;
   }

   public boolean addIfAbsent(final E o)
   {
      Object obj = theMap.putIfAbsent(o, ConcurrentHashSet.dummy);

      return obj == null;
   }

}
