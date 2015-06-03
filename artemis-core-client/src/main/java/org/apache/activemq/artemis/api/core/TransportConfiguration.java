/*
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
package org.apache.activemq.artemis.api.core;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.artemis.core.client.ActiveMQClientMessageBundle;
import org.apache.activemq.artemis.core.remoting.impl.TransportConfigurationUtil;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.utils.UUIDGenerator;

/**
 * A TransportConfiguration is used by a client to specify connections to a server and its backup if
 * one exists.
 * <p>
 * Typically the constructors take the class name and parameters for needed to create the
 * connection. These will be different dependent on which connector is being used, i.e. Netty or
 * InVM etc. For example:
 *
 * <pre>
 * HashMap&lt;String, Object&gt; map = new HashMap&lt;String, Object&gt;();
 * map.put(&quot;host&quot;, &quot;localhost&quot;);
 * map.put(&quot;port&quot;, 61616);
 * TransportConfiguration config = new TransportConfiguration(InVMConnectorFactory.class.getName(), map);
 * ClientSessionFactory sf = new ClientSessionFactoryImpl(config);
 * </pre>
 */
public class TransportConfiguration implements Serializable
{
   private static final long serialVersionUID = -3994528421527392679L;

   private String name;

   private String factoryClassName;

   private Map<String, Object> params;

   private static final byte TYPE_BOOLEAN = 0;

   private static final byte TYPE_INT = 1;

   private static final byte TYPE_LONG = 2;

   private static final byte TYPE_STRING = 3;

   /**
    * Utility method for splitting a comma separated list of hosts
    *
    * @param commaSeparatedHosts the comma separated host string
    * @return the hosts
    */
   public static String[] splitHosts(final String commaSeparatedHosts)
   {
      if (commaSeparatedHosts == null)
      {
         return new String[0];
      }
      String[] hosts = commaSeparatedHosts.split(",");

      for (int i = 0; i < hosts.length; i++)
      {
         hosts[i] = hosts[i].trim();
      }
      return hosts;
   }

   /**
    * Creates a default TransportConfiguration with no configured transport.
    */
   public TransportConfiguration()
   {
      this.params = new HashMap<>();
   }

   /**
    * Creates a TransportConfiguration with a specific name providing the class name of the {@link org.apache.activemq.artemis.spi.core.remoting.ConnectorFactory}
    * and any parameters needed.
    *
    * @param className The class name of the ConnectorFactory
    * @param params    The parameters needed by the ConnectorFactory
    * @param name      The name of this TransportConfiguration
    */
   public TransportConfiguration(final String className, final Map<String, Object> params, final String name)
   {
      factoryClassName = className;

      if (params == null || params.isEmpty())
      {
         this.params = TransportConfigurationUtil.getDefaults(className);
      }
      else
      {
         this.params = params;
      }

      this.name = name;
   }

   /**
    * Creates a TransportConfiguration providing the class name of the {@link org.apache.activemq.artemis.spi.core.remoting.ConnectorFactory}
    * and any parameters needed.
    *
    * @param className The class name of the ConnectorFactory
    * @param params    The parameters needed by the ConnectorFactory
    */
   public TransportConfiguration(final String className, final Map<String, Object> params)
   {
      this(className, params, UUIDGenerator.getInstance().generateStringUUID());
   }

   /**
    * Creates a TransportConfiguration providing the class name of the {@link org.apache.activemq.artemis.spi.core.remoting.ConnectorFactory}
    *
    * @param className The class name of the ConnectorFactory
    */
   public TransportConfiguration(final String className)
   {
      this(className, new HashMap<String, Object>(), UUIDGenerator.getInstance().generateStringUUID());
   }

   /**
    * Returns the name of this TransportConfiguration.
    *
    * @return the name
    */
   public String getName()
   {
      return name;
   }

   /**
    * Returns the class name of ConnectorFactory being used by this TransportConfiguration
    *
    * @return The factory's class name
    */
   public String getFactoryClassName()
   {
      return factoryClassName;
   }

   /**
    * Returns any parameters set for this TransportConfiguration
    *
    * @return the parameters
    */
   public Map<String, Object> getParams()
   {
      return params;
   }

   @Override
   public int hashCode()
   {
      return factoryClassName.hashCode();
   }

   @Override
   public boolean equals(final Object other)
   {
      if (other instanceof TransportConfiguration == false)
      {
         return false;
      }

      TransportConfiguration kother = (TransportConfiguration) other;

      if (factoryClassName.equals(kother.factoryClassName))
      {
         if (params == null || params.isEmpty())
         {
            return kother.params == null || kother.params.isEmpty();
         }
         else
         {
            if (kother.params == null || kother.params.isEmpty())
            {
               return false;
            }
            else if (params.size() == kother.params.size())
            {
               for (Map.Entry<String, Object> entry : params.entrySet())
               {
                  Object thisVal = entry.getValue();

                  Object otherVal = kother.params.get(entry.getKey());

                  if (otherVal == null || !otherVal.equals(thisVal))
                  {
                     return false;
                  }
               }
               return true;
            }
            else
            {
               return false;
            }
         }
      }
      else
      {
         return false;
      }
   }

   /**
    * There's a case on ClusterConnections that we need to find an equivalent Connector and we can't
    * use a Netty Cluster Connection on an InVM ClusterConnection (inVM used on tests) for that
    * reason I need to test if the two instances of the TransportConfiguration are equivalent while
    * a test a connector against an acceptor
    * @param otherConfig
    * @return {@code true} if the factory class names are equivalents
    */
   public boolean isEquivalent(TransportConfiguration otherConfig)
   {
      if (this.getFactoryClassName().equals(otherConfig.getFactoryClassName()))
      {
         return true;
      }
      else if (this.getFactoryClassName().contains("Netty") && otherConfig.getFactoryClassName().contains("Netty"))
      {
         return true;
      }
      else if (this.getFactoryClassName().contains("InVM") && otherConfig.getFactoryClassName().contains("InVM"))
      {
         return true;
      }
      else
      {
         return false;
      }
   }

   @Override
   public String toString()
   {
      StringBuilder str = new StringBuilder(TransportConfiguration.class.getSimpleName());
      str.append("(name=" + name + ", ");
      str.append("factory=" + replaceWildcardChars(factoryClassName));
      str.append(") ");
      if (params != null)
      {
         if (!params.isEmpty())
         {
            str.append("?");
         }

         boolean first = true;
         for (Map.Entry<String, Object> entry : params.entrySet())
         {
            if (!first)
            {
               str.append("&");
            }

            String key = entry.getKey();

            // HORNETQ-1281 - don't log passwords
            String val;
            if (key.equals(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME) || key.equals(TransportConstants.DEFAULT_TRUSTSTORE_PASSWORD))
            {
               val = "****";
            }
            else
            {
               val = entry.getValue() == null ? "null" : entry.getValue().toString();
            }

            str.append(replaceWildcardChars(key)).append('=').append(replaceWildcardChars(val));

            first = false;
         }
      }
      return str.toString();
   }

   /**
    * Encodes this TransportConfiguration into a buffer.
    * <p>
    * Note that this is only used internally ActiveMQ Artemis.
    *
    * @param buffer the buffer to encode into
    */
   public void encode(final ActiveMQBuffer buffer)
   {
      buffer.writeString(name);
      buffer.writeString(factoryClassName);

      buffer.writeInt(params == null ? 0 : params.size());

      if (params != null)
      {
         for (Map.Entry<String, Object> entry : params.entrySet())
         {
            buffer.writeString(entry.getKey());

            Object val = entry.getValue();

            if (val instanceof Boolean)
            {
               buffer.writeByte(TransportConfiguration.TYPE_BOOLEAN);
               buffer.writeBoolean((Boolean) val);
            }
            else if (val instanceof Integer)
            {
               buffer.writeByte(TransportConfiguration.TYPE_INT);
               buffer.writeInt((Integer) val);
            }
            else if (val instanceof Long)
            {
               buffer.writeByte(TransportConfiguration.TYPE_LONG);
               buffer.writeLong((Long) val);
            }
            else if (val instanceof String)
            {
               buffer.writeByte(TransportConfiguration.TYPE_STRING);
               buffer.writeString((String) val);
            }
            else
            {
               throw ActiveMQClientMessageBundle.BUNDLE.invalidEncodeType(val);
            }
         }
      }
   }

   /**
    * Decodes this TransportConfiguration from a buffer.
    * <p>
    * Note this is only used internally by ActiveMQ
    *
    * @param buffer the buffer to decode from
    */
   public void decode(final ActiveMQBuffer buffer)
   {
      name = buffer.readString();
      factoryClassName = buffer.readString();

      int num = buffer.readInt();

      if (params == null)
      {
         if (num > 0)
         {
            params = new HashMap<String, Object>();
         }
      }
      else
      {
         params.clear();
      }

      for (int i = 0; i < num; i++)
      {
         String key = buffer.readString();

         byte type = buffer.readByte();

         Object val;

         switch (type)
         {
            case TYPE_BOOLEAN:
            {
               val = buffer.readBoolean();

               break;
            }
            case TYPE_INT:
            {
               val = buffer.readInt();

               break;
            }
            case TYPE_LONG:
            {
               val = buffer.readLong();

               break;
            }
            case TYPE_STRING:
            {
               val = buffer.readString();

               break;
            }
            default:
            {
               throw ActiveMQClientMessageBundle.BUNDLE.invalidType(type);
            }
         }

         params.put(key, val);
      }
   }

   private static String replaceWildcardChars(final String str)
   {
      return str.replace('.', '-');
   }
}
