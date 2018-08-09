/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.core.server.plugin;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.transaction.Transaction;

/**
 *
 */
public interface ActiveMQServerBindingPlugin extends ActiveMQServerBasePlugin {

   /**
    * Before a binding is added
    *
    * @param binding
    * @throws ActiveMQException
    */
   default void beforeAddBinding(Binding binding) throws ActiveMQException {

   }

   /**
    * After a binding has been added
    *
    * @param binding The newly added binding
    * @throws ActiveMQException
    */
   default void afterAddBinding(Binding binding) throws ActiveMQException {

   }

   /**
    * Before a binding is removed
    *
    * @param uniqueName
    * @param tx
    * @param deleteData
    * @throws ActiveMQException
    */
   default void beforeRemoveBinding(SimpleString uniqueName, Transaction tx, boolean deleteData) throws ActiveMQException {

   }

   /**
    * After a binding is removed
    *
    * @param binding
    * @param tx
    * @param deleteData
    * @throws ActiveMQException
    */
   default void afterRemoveBinding(Binding binding, Transaction tx, boolean deleteData) throws ActiveMQException {

   }
}
