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
package org.apache.activemq6.core.protocol.stomp;

/**
 * Stomp Spec Versions
 *
 * @author <a href="mailto:hgao@redhat.com">Howard Gao</a>
 */
public enum StompVersions
{
   V1_0("1.0"),
   V1_1("1.1"),
   V1_2("1.2");

   private String version;

   private StompVersions(String ver)
   {
      this.version = ver;
   }

   public String toString()
   {
      return this.version;
   }
}
