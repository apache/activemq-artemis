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

package org.apache.activemq.utils;

import org.apache.activemq.utils.uri.URIFactory;
import org.apache.activemq.utils.uri.URISchema;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.util.Map;

/**
 * @author clebertsuconic
 */

public class URIParserTest
{

   /**
    * this is just a simple test to validate the model
    *
    * @throws Throwable
    */
   @Test
   public void testSchemaFruit() throws Throwable
   {
      FruitParser parser = new FruitParser();
      Fruit fruit = (Fruit)parser.newObject(new URI("fruit://some:guy@fair-market:3030?color=green&fluentName=something"));

      Assert.assertEquals("fruit", fruit.getName());
      Assert.assertEquals(3030, fruit.getPort());
      Assert.assertEquals("fair-market", fruit.getHost());
      Assert.assertEquals("some:guy", fruit.getUserInfo());
      Assert.assertEquals("green", fruit.getColor());
      Assert.assertEquals("something", fruit.getFluentName());


   }

   /**
    * Even thought there's no host Poperty on FruitBase.. this should still work fine without throwing any exceptions
    *
    * @throws Throwable
    */
   @Test
   public void testSchemaNoHosPropertyt() throws Throwable
   {
      FruitParser parser = new FruitParser();
      FruitBase fruit = parser.newObject(new URI("base://some:guy@fair-market:3030?color=green&fluentName=something"));
      Assert.assertEquals("base", fruit.getName());
      Assert.assertEquals("green", fruit.getColor());
      Assert.assertEquals("something", fruit.getFluentName());
   }

   /**
    * Even thought there's no host Poperty on FruitBase.. this should still work fine without throwing any exceptions
    *
    * @throws Throwable
    */
   @Test
   public void testSchemaNoHostOnURL() throws Throwable
   {
      FruitParser parser = new FruitParser();
      Fruit fruit = (Fruit)parser.newObject(new URI("fruit://some:guy@port?color=green&fluentName=something"));

      System.out.println("fruit:" + fruit);
      Assert.assertEquals("fruit", fruit.getName());
      Assert.assertEquals("green", fruit.getColor());
      Assert.assertEquals("something", fruit.getFluentName());
   }


   class FruitParser extends URIFactory<FruitBase>
   {
      FruitParser()
      {
         this.registerSchema(new FruitSchema());
         this.registerSchema(new FruitBaseSchema());
      }
   }

   class FruitSchema extends URISchema<FruitBase>
   {
      @Override
      public String getSchemaName()
      {
         return "fruit";
      }


      @Override
      public FruitBase internalNewObject(URI uri, Map<String, String> query) throws Exception
      {
         return setData(uri, new Fruit(getSchemaName()), query);
      }
   }

   class FruitBaseSchema extends URISchema<FruitBase>
   {
      @Override
      public String getSchemaName()
      {
         return "base";
      }


      @Override
      public FruitBase internalNewObject(URI uri, Map<String, String> query) throws Exception
      {
         return setData(uri, new FruitBase(getSchemaName()), query);
      }
   }


   public static class FruitBase
   {
      final String name;
      String fluentName;
      String color;

      FruitBase(final String name)
      {
         this.name = name;
      }


      public String getName()
      {
         return name;
      }


      public String getColor()
      {
         return color;
      }

      public void setColor(String color)
      {
         this.color = color;
      }

      public String getFluentName()
      {
         return fluentName;
      }

      public FruitBase setFluentName(String name)
      {
         this.fluentName = name;

         return this;
      }

      @Override
      public String toString()
      {
         return "FruitBase{" +
            "name='" + name + '\'' +
            ", fluentName='" + fluentName + '\'' +
            ", color='" + color + '\'' +
            '}';
      }
   }

   public static class Fruit extends FruitBase
   {


      public Fruit(String name)
      {
         super(name);
      }

      String host;
      int port;
      String userInfo;



      public void setHost(String host)
      {
         this.host = host;
      }

      public String getHost()
      {
         return host;
      }

      public void setPort(int port)
      {
         this.port = port;
      }

      public int getPort()
      {
         return port;
      }

      public void setUserInfo(String userInfo)
      {
         this.userInfo = userInfo;
      }

      public String getUserInfo()
      {
         return userInfo;
      }

      @Override
      public String toString()
      {
         return "Fruit{" +
            "host='" + host + '\'' +
            ", port=" + port +
            ", userInfo='" + userInfo + '\'' +
            "super=" + super.toString() + '}';
      }
   }
}
