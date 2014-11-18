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
package org.apache.activemq.core.remoting.impl.invm;

import org.apache.activemq.core.server.ActiveMQMessageBundle;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A InVMRegistry
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public final class InVMRegistry
{
   public static final InVMRegistry instance = new InVMRegistry();

   private final ConcurrentMap<Integer, InVMAcceptor> acceptors = new ConcurrentHashMap<Integer, InVMAcceptor>();

   public void registerAcceptor(final int id, final InVMAcceptor acceptor)
   {
      if (acceptors.putIfAbsent(id, acceptor) != null)
      {
         throw ActiveMQMessageBundle.BUNDLE.acceptorExists(id);
      }
   }

   public void unregisterAcceptor(final int id)
   {
      if (acceptors.remove(id) == null)
      {
         throw ActiveMQMessageBundle.BUNDLE.acceptorNotExists(id);
      }
   }

   public InVMAcceptor getAcceptor(final int id)
   {
      return acceptors.get(id);
   }

   public void clear()
   {
      acceptors.clear();
   }

   public int size()
   {
      return acceptors.size();
   }
}
