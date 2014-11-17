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
package org.apache.activemq.tests.integration.stomp.util;

/**
 *
 * @author <a href="mailto:hgao@redhat.com">Howard Gao</a>
 *
 */
public class StompFrameFactoryFactory
{
   public static StompFrameFactory getFactory(String version)
   {
      if ("1.0".equals(version))
      {
         return new StompFrameFactoryV10();
      }

      if ("1.1".equals(version))
      {
         return new StompFrameFactoryV11();
      }

      if ("1.2".equals(version))
      {
         return new StompFrameFactoryV12();
      }

      return null;
   }

}
