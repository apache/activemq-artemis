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

package org.apache.activemq.api.core;

/**
 * Security exception thrown when the cluster user fails authentication.
 */
public final class HornetQClusterSecurityException extends HornetQException
{
   private static final long serialVersionUID = -5890578849781297933L;

   public HornetQClusterSecurityException()
   {
      super(HornetQExceptionType.CLUSTER_SECURITY_EXCEPTION);
   }

   public HornetQClusterSecurityException(final String msg)
   {
      super(HornetQExceptionType.CLUSTER_SECURITY_EXCEPTION, msg);
   }
}
