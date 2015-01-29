/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.uri;

import org.apache.activemq.api.core.client.ServerLocator;
import org.apache.activemq.utils.uri.URISchema;

import java.net.URI;
import java.util.Map;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public abstract class AbstractServerLocatorSchema extends URISchema<ServerLocator>
{
   protected ConnectionOptions newConnectionOptions(URI uri, Map<String, String> query) throws Exception
   {
      return setData(uri, new ConnectionOptions(), query);
   }
}
