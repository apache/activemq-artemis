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
package org.apache.activemq6.core.postoffice;

import org.apache.activemq6.core.server.HornetQMessageBundle;

/**
 * A BindingType
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 * Created 22 Dec 2008 13:37:23
 *
 *
 */
public enum BindingType
{
   LOCAL_QUEUE, REMOTE_QUEUE, DIVERT;

   public static final int LOCAL_QUEUE_INDEX = 0;

   public static final int REMOTE_QUEUE_INDEX = 1;

   public static final int DIVERT_INDEX = 2;

   public int toInt()
   {
      if (equals(BindingType.LOCAL_QUEUE))
      {
         return BindingType.LOCAL_QUEUE_INDEX;
      }
      else if (equals(BindingType.REMOTE_QUEUE))
      {
         return BindingType.REMOTE_QUEUE_INDEX;
      }
      else if (equals(BindingType.DIVERT))
      {
         return BindingType.DIVERT_INDEX;
      }
      else
      {
         throw HornetQMessageBundle.BUNDLE.cannotConvertToInt();
      }
   }

}
