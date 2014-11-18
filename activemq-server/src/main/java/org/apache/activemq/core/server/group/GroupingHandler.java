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
package org.apache.activemq.core.server.group;

import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.core.server.HornetQComponent;
import org.apache.activemq.core.server.group.impl.GroupBinding;
import org.apache.activemq.core.server.group.impl.Proposal;
import org.apache.activemq.core.server.group.impl.Response;
import org.apache.activemq.core.server.management.NotificationListener;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public interface GroupingHandler extends NotificationListener, HornetQComponent
{
   // this method should maintain a WeakHash list, no need to remove the elements
   void addListener(UnproposalListener listener);

   SimpleString getName();

   void resendPending() throws Exception;

   Response propose(Proposal proposal) throws Exception;

   void proposed(Response response) throws Exception;

   void sendProposalResponse(Response response, int distance) throws Exception;

   Response receive(Proposal proposal, int distance) throws Exception;

   void addGroupBinding(GroupBinding groupBinding);

   Response getProposal(SimpleString fullID, boolean touchTime);

   void awaitBindings() throws Exception;

   /**
    * this will force a removal of the group everywhere with an unproposal (dinstance=0).
    * This is for the case where a node goes missing
    */
   void forceRemove(SimpleString groupid, SimpleString clusterName) throws Exception;

   void remove(SimpleString groupid, SimpleString clusterName) throws Exception;

   void remove(SimpleString groupid, SimpleString clusterName, int distance) throws Exception;
}
