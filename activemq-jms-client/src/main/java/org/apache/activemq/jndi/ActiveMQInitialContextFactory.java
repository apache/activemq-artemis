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
package org.apache.activemq.jndi;

import javax.jms.Queue;
import javax.jms.Topic;
import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.activemq.api.core.DiscoveryGroupConfiguration;
import org.apache.activemq.api.core.JGroupsBroadcastGroupConfiguration;
import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.api.core.UDPBroadcastGroupConfiguration;
import org.apache.activemq.api.core.client.ActiveMQClient;
import org.apache.activemq.api.jms.ActiveMQJMSClient;
import org.apache.activemq.api.jms.JMSFactoryType;
import org.apache.activemq.core.client.ActiveMQClientLogger;
import org.apache.activemq.core.remoting.impl.netty.NettyConnectorFactory;
import org.apache.activemq.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.jms.client.ActiveMQConnectionFactory;

/**
 * A factory of the ActiveMQ InitialContext which contains
 * {@link javax.jms.ConnectionFactory} instances as well as a child context called
 * <i>destinations</i> which contain all of the current active destinations, in
 * child context depending on the QoS such as transient or durable and queue or
 * topic.
 */
public class ActiveMQInitialContextFactory implements InitialContextFactory
{
   public static final String CONNECTION_FACTORY_NAMES = "connectionFactoryNames";
   public static final String REFRESH_TIMEOUT = "refresh-timeout";
   public static final String DISCOVERY_INITIAL_WAIT_TIMEOUT = "discovery-initial-wait-timeout";

   private static final String[] DEFAULT_CONNECTION_FACTORY_NAMES = {"ConnectionFactory", "XAConnectionFactory", "QueueConnectionFactory", "TopicConnectionFactory"};
   public static final String TCP_SCHEME = "tcp";
   public static final String JGROUPS_SCHEME = "jgroups";
   public static final String UDP_SCHEME = "udp";
   public static final String VM_SCHEME = "vm";
   public static final String HA = "ha";
   public static final String CF_TYPE = "type";
   public static final String QUEUE_CF = "QUEUE_CF";
   public static final String TOPIC_CF = "TOPIC_CF";
   public static final String QUEUE_XA_CF = "QUEUE_XA_CF";
   public static final String TOPIC_XA_CF = "TOPIC_XA_CF";
   public static final String XA_CF = "XA_CF";
   public static final String DYNAMIC_QUEUE_CONTEXT = "dynamicQueues";
   public static final String DYNAMIC_TOPIC_CONTEXT = "dynamicTopics";

   private String connectionPrefix = "connection.";
   private String queuePrefix = "queue.";
   private String topicPrefix = "topic.";

   public Context getInitialContext(Hashtable environment) throws NamingException
   {
      // lets create a factory
      Map<String, Object> data = new ConcurrentHashMap<String, Object>();
      String[] names = getConnectionFactoryNames(environment);
      for (int i = 0; i < names.length; i++)
      {
         ActiveMQConnectionFactory factory = null;
         String name = names[i];

         try
         {
            factory = createConnectionFactory(name, environment);
         }
         catch (Exception e)
         {
            e.printStackTrace();
            throw new NamingException("Invalid broker URL");
         }

         data.put(name, factory);
      }

      createQueues(data, environment);
      createTopics(data, environment);

      data.put(DYNAMIC_QUEUE_CONTEXT, new LazyCreateContext()
      {
         private static final long serialVersionUID = 6503881346214855588L;

         protected Object createEntry(String name)
         {
            return ActiveMQJMSClient.createQueue(name);
         }
      });
      data.put(DYNAMIC_TOPIC_CONTEXT, new LazyCreateContext()
      {
         private static final long serialVersionUID = 2019166796234979615L;

         protected Object createEntry(String name)
         {
            return ActiveMQJMSClient.createTopic(name);
         }
      });

      return createContext(environment, data);
   }

   // Properties
   // -------------------------------------------------------------------------
   public String getTopicPrefix()
   {
      return topicPrefix;
   }

   public void setTopicPrefix(String topicPrefix)
   {
      this.topicPrefix = topicPrefix;
   }

   public String getQueuePrefix()
   {
      return queuePrefix;
   }

   public void setQueuePrefix(String queuePrefix)
   {
      this.queuePrefix = queuePrefix;
   }

   // Implementation methods
   // -------------------------------------------------------------------------

   protected ReadOnlyContext createContext(Hashtable environment, Map<String, Object> data)
   {
      return new ReadOnlyContext(environment, data);
   }

   protected ActiveMQConnectionFactory createConnectionFactory(String name, Hashtable environment) throws URISyntaxException, MalformedURLException
   {
      Hashtable connectionFactoryProperties = new Hashtable(environment);
      if (DEFAULT_CONNECTION_FACTORY_NAMES[1].equals(name))
      {
         connectionFactoryProperties.put(CF_TYPE, XA_CF);
      }
      if (DEFAULT_CONNECTION_FACTORY_NAMES[2].equals(name))
      {
         connectionFactoryProperties.put(CF_TYPE, QUEUE_CF);
      }
      if (DEFAULT_CONNECTION_FACTORY_NAMES[3].equals(name))
      {
         connectionFactoryProperties.put(CF_TYPE, TOPIC_CF);
      }
      String prefix = connectionPrefix + name + ".";
      for (Iterator iter = environment.entrySet().iterator(); iter.hasNext(); )
      {
         Map.Entry entry = (Map.Entry) iter.next();
         String key = (String) entry.getKey();
         if (key.startsWith(prefix))
         {
            // Rename the key...
            connectionFactoryProperties.remove(key);
            key = key.substring(prefix.length());
            connectionFactoryProperties.put(key, entry.getValue());
         }
      }
      return createConnectionFactory(connectionFactoryProperties);
   }

   protected String[] getConnectionFactoryNames(Map environment)
   {
      String factoryNames = (String) environment.get(CONNECTION_FACTORY_NAMES);
      if (factoryNames != null)
      {
         List<String> list = new ArrayList<String>();
         for (StringTokenizer enumeration = new StringTokenizer(factoryNames, ","); enumeration.hasMoreTokens(); )
         {
            list.add(enumeration.nextToken().trim());
         }
         int size = list.size();
         if (size > 0)
         {
            String[] answer = new String[size];
            list.toArray(answer);
            return answer;
         }
      }
      return DEFAULT_CONNECTION_FACTORY_NAMES;
   }

   protected void createQueues(Map<String, Object> data, Hashtable environment)
   {
      for (Iterator iter = environment.entrySet().iterator(); iter.hasNext(); )
      {
         Map.Entry entry = (Map.Entry) iter.next();
         String key = entry.getKey().toString();
         if (key.startsWith(queuePrefix))
         {
            String jndiName = key.substring(queuePrefix.length());
            data.put(jndiName, createQueue(entry.getValue().toString()));
         }
      }
   }

   protected void createTopics(Map<String, Object> data, Hashtable environment)
   {
      for (Iterator iter = environment.entrySet().iterator(); iter.hasNext(); )
      {
         Map.Entry entry = (Map.Entry) iter.next();
         String key = entry.getKey().toString();
         if (key.startsWith(topicPrefix))
         {
            String jndiName = key.substring(topicPrefix.length());
            data.put(jndiName, createTopic(entry.getValue().toString()));
         }
      }
   }

   /**
    * Factory method to create new Queue instances
    */
   protected Queue createQueue(String name)
   {
      return ActiveMQJMSClient.createQueue(name);
   }

   /**
    * Factory method to create new Topic instances
    */
   protected Topic createTopic(String name)
   {
      return ActiveMQJMSClient.createTopic(name);
   }

   /**
    * Factory method to create a new connection factory from the given environment
    */
   protected ActiveMQConnectionFactory createConnectionFactory(Hashtable environment) throws URISyntaxException, MalformedURLException
   {
      ActiveMQConnectionFactory connectionFactory;
      Map transportConfig = new HashMap();

      if (environment.containsKey(Context.PROVIDER_URL))
      {
         URI providerURI = new URI(((String)environment.get(Context.PROVIDER_URL)));

         if (providerURI.getQuery() != null)
         {
            try
            {
               transportConfig = parseQuery(providerURI.getQuery());
            }
            catch (URISyntaxException e)
            {
            }
         }

         if (providerURI.getScheme().equals(TCP_SCHEME))
         {
            String[] connectors = providerURI.getAuthority().split(",");
            TransportConfiguration[] transportConfigurations = new TransportConfiguration[connectors.length];
            for (int i = 0; i < connectors.length; i++)
            {
               Map individualTransportConfig = new HashMap(transportConfig);
               String[] hostAndPort = connectors[i].split(":");
               individualTransportConfig.put(TransportConstants.HOST_PROP_NAME, hostAndPort[0]);
               individualTransportConfig.put(TransportConstants.PORT_PROP_NAME, hostAndPort[1]);
               transportConfigurations[i] = new TransportConfiguration(NettyConnectorFactory.class.getCanonicalName(), individualTransportConfig);
            }

            if (Boolean.TRUE.equals(environment.get(HA)))
            {
               connectionFactory = ActiveMQJMSClient.createConnectionFactoryWithHA(getJmsFactoryType(environment), transportConfigurations);
            }
            else
            {
               connectionFactory = ActiveMQJMSClient.createConnectionFactoryWithoutHA(getJmsFactoryType(environment), transportConfigurations);
            }
         }
         else if (providerURI.getScheme().equals(UDP_SCHEME))
         {
            DiscoveryGroupConfiguration dgc = new DiscoveryGroupConfiguration()
               .setRefreshTimeout(transportConfig.containsKey(REFRESH_TIMEOUT) ? Long.parseLong((String) transportConfig.get(REFRESH_TIMEOUT)) : ActiveMQClient.DEFAULT_DISCOVERY_REFRESH_TIMEOUT)
               .setDiscoveryInitialWaitTimeout(transportConfig.containsKey(DISCOVERY_INITIAL_WAIT_TIMEOUT) ? Long.parseLong((String) transportConfig.get(DISCOVERY_INITIAL_WAIT_TIMEOUT)) : ActiveMQClient.DEFAULT_DISCOVERY_INITIAL_WAIT_TIMEOUT)
               .setBroadcastEndpointFactoryConfiguration(new UDPBroadcastGroupConfiguration()
                                                            .setGroupAddress(providerURI.getHost())
                                                            .setGroupPort(providerURI.getPort())
                                                            .setLocalBindAddress(transportConfig.containsKey(TransportConstants.LOCAL_ADDRESS_PROP_NAME) ? (String) transportConfig.get(TransportConstants.LOCAL_ADDRESS_PROP_NAME) : null)
                                                            .setLocalBindPort(transportConfig.containsKey(TransportConstants.LOCAL_PORT_PROP_NAME) ? Integer.parseInt((String) transportConfig.get(TransportConstants.LOCAL_PORT_PROP_NAME)) : -1));
            if (Boolean.TRUE.equals(environment.get(HA)))
            {
               connectionFactory = ActiveMQJMSClient.createConnectionFactoryWithHA(dgc, getJmsFactoryType(environment));
            }
            else
            {
               connectionFactory = ActiveMQJMSClient.createConnectionFactoryWithoutHA(dgc, getJmsFactoryType(environment));
            }
         }
         else if (providerURI.getScheme().equals(JGROUPS_SCHEME))
         {
            JGroupsBroadcastGroupConfiguration config = new JGroupsBroadcastGroupConfiguration(providerURI.getAuthority(), providerURI.getPath() != null ? providerURI.getPath() : UUID.randomUUID().toString());

            DiscoveryGroupConfiguration dgc = new DiscoveryGroupConfiguration()
               .setRefreshTimeout(transportConfig.containsKey(REFRESH_TIMEOUT) ? Long.parseLong((String) transportConfig.get(REFRESH_TIMEOUT)) : ActiveMQClient.DEFAULT_DISCOVERY_REFRESH_TIMEOUT)
               .setDiscoveryInitialWaitTimeout(transportConfig.containsKey(DISCOVERY_INITIAL_WAIT_TIMEOUT) ? Long.parseLong((String) transportConfig.get(DISCOVERY_INITIAL_WAIT_TIMEOUT)) : ActiveMQClient.DEFAULT_DISCOVERY_INITIAL_WAIT_TIMEOUT)
               .setBroadcastEndpointFactoryConfiguration(config);
            if (Boolean.TRUE.equals(environment.get(HA)))
            {
               connectionFactory = ActiveMQJMSClient.createConnectionFactoryWithHA(dgc, getJmsFactoryType(environment));
            }
            else
            {
               connectionFactory = ActiveMQJMSClient.createConnectionFactoryWithoutHA(dgc, getJmsFactoryType(environment));
            }
         }
         else if (providerURI.getScheme().equals(VM_SCHEME))
         {
            Map inVmTransportConfig = new HashMap();
            inVmTransportConfig.put("server-id", providerURI.getHost());
            TransportConfiguration tc = new TransportConfiguration("org.apache.activemq.core.remoting.impl.invm.InVMConnectorFactory", inVmTransportConfig);
            connectionFactory = ActiveMQJMSClient.createConnectionFactoryWithoutHA(getJmsFactoryType(environment), tc);
         }
         else
         {
            throw new IllegalArgumentException("Invalid scheme");
         }
      }
      else
      {
         TransportConfiguration tc = new TransportConfiguration("org.apache.activemq.core.remoting.impl.invm.InVMConnectorFactory");
         connectionFactory = ActiveMQJMSClient.createConnectionFactoryWithoutHA(getJmsFactoryType(environment), tc);
      }

      Properties properties = new Properties();
      properties.putAll(environment);

      for (Object key : environment.keySet())
      {
         invokeSetter(connectionFactory, (String) key, environment.get(key));
      }

      return connectionFactory;
   }

   private JMSFactoryType getJmsFactoryType(Hashtable environment)
   {
      JMSFactoryType ultimateType = JMSFactoryType.CF; // default value
      if (environment.containsKey(CF_TYPE))
      {
         String tempType = (String) environment.get(CF_TYPE);
         if (QUEUE_CF.equals(tempType))
         {
            ultimateType = JMSFactoryType.QUEUE_CF;
         }
         else if (TOPIC_CF.equals(tempType))
         {
            ultimateType = JMSFactoryType.TOPIC_CF;
         }
         else if (QUEUE_XA_CF.equals(tempType))
         {
            ultimateType = JMSFactoryType.QUEUE_XA_CF;
         }
         else if (TOPIC_XA_CF.equals(tempType))
         {
            ultimateType = JMSFactoryType.TOPIC_XA_CF;
         }
         else if (XA_CF.equals(tempType))
         {
            ultimateType = JMSFactoryType.XA_CF;
         }
      }
      return ultimateType;
   }


   public static Map<String, String> parseQuery(String uri) throws URISyntaxException
   {
      try
      {
         uri = uri.substring(uri.lastIndexOf("?") + 1); // get only the relevant part of the query
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
         return rc;
      }
      catch (UnsupportedEncodingException e)
      {
         throw (URISyntaxException) new URISyntaxException(e.toString(), "Invalid encoding").initCause(e);
      }
   }

   public String getConnectionPrefix()
   {
      return connectionPrefix;
   }

   public void setConnectionPrefix(String connectionPrefix)
   {
      this.connectionPrefix = connectionPrefix;
   }

   private void invokeSetter(Object target, final String propertyName, final Object propertyValue)
   {
      Method setter = null;

      Method[] methods = target.getClass().getMethods();

      // turn something like "consumerWindowSize" to "setConsumerWindowSize"
      String setterMethodName = "set" + Character.toUpperCase(propertyName.charAt(0)) + propertyName.substring(1);

      for (Method m : methods)
      {
         if (m.getName().equals(setterMethodName))
         {
            setter = m;
            break;
         }
      }

      try
      {
         if (setter != null)
         {
            ActiveMQClientLogger.LOGGER.info("Invoking: " + setter + " that takes a " + setter.getParameterTypes()[0] + " with a " + propertyValue.getClass());
            if (propertyValue.getClass() == String.class && setter.getParameterTypes()[0] != String.class)
            {
               String stringPropertyValue = (String) propertyValue;
               if (setter.getParameterTypes()[0] == Integer.TYPE)
               {
                  setter.invoke(target, Integer.parseInt(stringPropertyValue));
               }
               else if (setter.getParameterTypes()[0] == Long.TYPE)
               {
                  setter.invoke(target, Long.parseLong(stringPropertyValue));
               }
               else if (setter.getParameterTypes()[0] == Double.TYPE)
               {
                  setter.invoke(target, Double.parseDouble(stringPropertyValue));
               }
               else if (setter.getParameterTypes()[0] == Boolean.TYPE)
               {
                  setter.invoke(target, Boolean.parseBoolean(stringPropertyValue));
               }
            }
            else
            {
               setter.invoke(target, propertyValue);
            }
         }
      }
      catch (Exception e)
      {
         ActiveMQClientLogger.LOGGER.warn("Caught exception during invocation of: " + setter, e);
      }
   }
}
