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
package org.apache.activemq6.api.core.management;

import java.util.Map;

/**
 * An AcceptorControl is used to manage Acceptors.
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @see Acceptor
 */
public interface AcceptorControl extends HornetQComponentControl
{
   /**
    * Returns the name of the acceptor
    */
   String getName();

   /**
    * Returns the class name of the AcceptorFactory implementation
    * used by this acceptor.
    *
    * @see AcceptorFactory
    */
   String getFactoryClassName();

   /**
    * Returns the parameters used to configure this acceptor
    */
   Map<String, Object> getParameters();
}
