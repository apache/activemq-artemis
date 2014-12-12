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

package org.apache.activemq.utils.uri;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author clebertsuconic
 */

public class URIFactory<T>
{

   private URI defaultURI;

   private final Map<String, URISchema<T>> schemas = new ConcurrentHashMap<>();

   public URI getDefaultURI()
   {
      return defaultURI;
   }

   public void setDefaultURI(URI uri)
   {
      this.defaultURI = uri;
   }

   public void registerSchema(URISchema<T> schemaFactory)
   {
      schemas.put(schemaFactory.getSchemaName(), schemaFactory);
      schemaFactory.setFactory(this);
   }

   public void removeSchema(final String schemaName)
   {
      schemas.remove(schemaName);
   }

   public T newObject(URI uri) throws Exception
   {
      URISchema<T> schemaFactory = schemas.get(uri.getScheme());

      if (schemaFactory == null)
      {
         throw new NullPointerException("Schema " + uri.getScheme() + " not found");
      }


      return schemaFactory.newObject(uri);
   }


}
