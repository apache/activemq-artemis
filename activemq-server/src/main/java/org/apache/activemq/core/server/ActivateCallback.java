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
package org.apache.activemq6.core.server;

/**
 * A ActivateCallback
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public interface ActivateCallback
{
   /*
    * this is called before any services are started when the server first initialised
    */
   void preActivate();

   /*
    * this is called after most of the services have been started but before any cluster resources or JMS resources have been
    */
   void activated();

   /*
    * this is called when the server is stopping, after any network resources and clients are closed but before the rest
    * of the resources
    */
   void deActivate();

   /*
    * this is called when all resources have been started including any JMS resources
    */
   void activationComplete();
}
