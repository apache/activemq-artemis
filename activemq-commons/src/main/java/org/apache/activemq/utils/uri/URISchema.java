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

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.beanutils.BeanUtilsBean;
import org.apache.commons.beanutils.FluentPropertyBeanIntrospector;

/**
 * @author clebertsuconic
 */

public abstract class URISchema<T>
{
   public abstract String getSchemaName();

   public T newObject(URI uri) throws Exception
   {
      return newObject(uri, null);
   }

   private URIFactory<T> parentFactory;


   void setFactory(URIFactory<T> factory)
   {
      this.parentFactory = parentFactory;
   }

   protected URIFactory<T> getFactory()
   {
      return parentFactory;
   }


   protected String getHost(URI uri)
   {
      URI defaultFactory = getDefaultURI();
      if (defaultFactory != null && uri.getHost() == null && defaultFactory.getScheme().equals(uri.getScheme()))
      {
         uri = defaultFactory;
      }
      return uri.getHost();
   }

   protected URI getDefaultURI()
   {
      URIFactory<T> factory = getFactory();
      if (factory == null)
      {
         return null;
      }
      else
      {
         return factory.getDefaultURI();
      }
   }

   protected int getPort(URI uri)
   {
      URI defaultFactory = getDefaultURI();
      if (defaultFactory != null && uri.getPort() < 0 && defaultFactory.getScheme().equals(uri.getScheme()))
      {
         uri = defaultFactory;
      }
      return uri.getPort();
   }

   /**
    * It will create a new Object for the URI selected schema.
    * the propertyOverrides is used to replace whatever was defined on the URL string
    * @param uri
    * @param propertyOverrides
    * @return
    * @throws Exception
    */
   public T newObject(URI uri, Map<String, String> propertyOverrides) throws Exception
   {
      return internalNewObject(uri, parseQuery(uri.getQuery(), propertyOverrides));
   }

   protected abstract T internalNewObject(URI uri, Map<String, String> query) throws Exception;

   private static final BeanUtilsBean beanUtils = new BeanUtilsBean();


   static
   {
      // This is to customize the BeanUtils to use Fluent Proeprties as well
      beanUtils.getPropertyUtils().addBeanIntrospector(new FluentPropertyBeanIntrospector());
   }


   public static Map<String, String> parseQuery(String uri, Map<String, String> propertyOverrides) throws URISyntaxException
   {
      try
      {
         Map<String, String> rc = new HashMap<String, String>();
         if (uri != null && !uri.isEmpty())
         {
            String[] parameters = uri.split("&");
            for (int i = 0; i < parameters.length; i++)
            {
               int p = parameters[i].indexOf("=");
               if (p >= 0)
               {
                  String name = URLDecoder.decode(parameters[i].substring(0, p), "UTF-8");
                  String value = URLDecoder.decode(parameters[i].substring(p + 1), "UTF-8");
                  rc.put(name, value);
               }
               else
               {
                  rc.put(parameters[i], null);
               }
            }
         }

         if (propertyOverrides != null)
         {
            for (Map.Entry<String, String> entry: propertyOverrides.entrySet())
            {
               rc.put(entry.getKey(), entry.getValue());
            }
         }
         return rc;
      }
      catch (UnsupportedEncodingException e)
      {
         throw (URISyntaxException) new URISyntaxException(e.toString(), "Invalid encoding").initCause(e);
      }
   }



   protected String printQuery(Map<String, String> query)
   {
      StringBuffer buffer = new StringBuffer();
      for (Map.Entry<String, String> entry : query.entrySet())
      {
         buffer.append(entry.getKey() + "=" + entry.getValue());
         buffer.append("\n");
      }

      return  buffer.toString();
   }

   protected static <P> P copyData(P source, P target) throws Exception
   {
      synchronized (beanUtils)
      {
         beanUtils.copyProperties(source, target);
      }
      return target;
   }

   protected static <P> P setData(URI uri, P obj, Map<String, String> query) throws Exception
   {
      synchronized (beanUtils)
      {
         beanUtils.setProperty(obj, "host", uri.getHost());
         beanUtils.setProperty(obj, "port", uri.getPort());
         beanUtils.setProperty(obj, "userInfo", uri.getUserInfo());
         beanUtils.populate(obj, query);
      }
      return obj;
   }
}
