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
package org.apache.activemq.integration.aerogear;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.core.filter.Filter;
import org.apache.activemq.core.filter.impl.FilterImpl;
import org.apache.activemq.core.postoffice.Binding;
import org.apache.activemq.core.postoffice.PostOffice;
import org.apache.activemq.core.server.ConnectorService;
import org.apache.activemq.core.server.Consumer;
import org.apache.activemq.core.server.HandleStatus;
import org.apache.activemq.core.server.HornetQServerLogger;
import org.apache.activemq.core.server.MessageReference;
import org.apache.activemq.core.server.Queue;
import org.apache.activemq.core.server.ServerMessage;
import org.apache.activemq.utils.ConfigurationHelper;
import org.jboss.aerogear.unifiedpush.JavaSender;
import org.jboss.aerogear.unifiedpush.SenderClient;
import org.jboss.aerogear.unifiedpush.message.MessageResponseCallback;
import org.jboss.aerogear.unifiedpush.message.UnifiedMessage;

public class AeroGearConnectorService implements ConnectorService, Consumer, MessageResponseCallback
{
   private final String connectorName;

   private final PostOffice postOffice;

   private final ScheduledExecutorService scheduledThreadPool;

   private final String queueName;

   private final String endpoint;

   private final String applicationId;

   private final String applicationMasterSecret;

   private final int ttl;

   private final String badge;

   private final String sound;

   private final boolean contentAvailable;

   private final String actionCategory;

   private String[] variants;

   private String[] aliases;

   private String[] deviceTypes;

   private final String filterString;

   private final int retryInterval;

   private final int retryAttempts;

   private Queue queue;

   private Filter filter;

   private volatile boolean handled = false;

   private boolean started = false;

   private boolean reconnecting = false;

   public AeroGearConnectorService(String connectorName, Map<String, Object> configuration, PostOffice postOffice, ScheduledExecutorService scheduledThreadPool)
   {
      this.connectorName = connectorName;
      this.postOffice = postOffice;
      this.scheduledThreadPool = scheduledThreadPool;
      this.queueName = ConfigurationHelper.getStringProperty(AeroGearConstants.QUEUE_NAME, null, configuration);
      this.endpoint = ConfigurationHelper.getStringProperty(AeroGearConstants.ENDPOINT_NAME, null, configuration);
      this.applicationId = ConfigurationHelper.getStringProperty(AeroGearConstants.APPLICATION_ID_NAME, null, configuration);
      this.applicationMasterSecret = ConfigurationHelper.getStringProperty(AeroGearConstants.APPLICATION_MASTER_SECRET_NAME, null, configuration);
      this.ttl = ConfigurationHelper.getIntProperty(AeroGearConstants.TTL_NAME, AeroGearConstants.DEFAULT_TTL, configuration);
      this.badge = ConfigurationHelper.getStringProperty(AeroGearConstants.BADGE_NAME, null, configuration);
      this.sound = ConfigurationHelper.getStringProperty(AeroGearConstants.SOUND_NAME, AeroGearConstants.DEFAULT_SOUND, configuration);
      this.contentAvailable = ConfigurationHelper.getBooleanProperty(AeroGearConstants.CONTENT_AVAILABLE_NAME, false, configuration);
      this.actionCategory = ConfigurationHelper.getStringProperty(AeroGearConstants.ACTION_CATEGORY_NAME, null, configuration);
      this.filterString = ConfigurationHelper.getStringProperty(AeroGearConstants.FILTER_NAME, null, configuration);
      this.retryInterval = ConfigurationHelper.getIntProperty(AeroGearConstants.RETRY_INTERVAL_NAME, AeroGearConstants.DEFAULT_RETRY_INTERVAL, configuration);
      this.retryAttempts = ConfigurationHelper.getIntProperty(AeroGearConstants.RETRY_ATTEMPTS_NAME, AeroGearConstants.DEFAULT_RETRY_ATTEMPTS, configuration);
      String variantsString = ConfigurationHelper.getStringProperty(AeroGearConstants.VARIANTS_NAME, null, configuration);
      if (variantsString != null)
      {
         variants = variantsString.split(",");
      }
      String aliasesString = ConfigurationHelper.getStringProperty(AeroGearConstants.ALIASES_NAME, null, configuration);
      if (aliasesString != null)
      {
         aliases = aliasesString.split(",");
      }
      String deviceTypeString = ConfigurationHelper.getStringProperty(AeroGearConstants.DEVICE_TYPE_NAME, null, configuration);
      if (deviceTypeString != null)
      {
         deviceTypes = deviceTypeString.split(",");
      }
   }

   @Override
   public String getName()
   {
      return connectorName;
   }

   @Override
   public void start() throws Exception
   {
      if (started)
      {
         return;
      }
      if (filterString != null)
      {
         filter = FilterImpl.createFilter(filterString);
      }

      if (endpoint == null || endpoint.isEmpty())
      {
         throw HornetQAeroGearBundle.BUNDLE.endpointNull();
      }
      if (applicationId == null || applicationId.isEmpty())
      {
         throw HornetQAeroGearBundle.BUNDLE.applicationIdNull();
      }
      if (applicationMasterSecret == null || applicationMasterSecret.isEmpty())
      {
         throw HornetQAeroGearBundle.BUNDLE.masterSecretNull();
      }

      Binding b = postOffice.getBinding(new SimpleString(queueName));
      if (b == null)
      {
         throw HornetQAeroGearBundle.BUNDLE.noQueue(connectorName, queueName);
      }

      queue = (Queue) b.getBindable();

      queue.addConsumer(this);

      started = true;
   }

   @Override
   public void stop() throws Exception
   {
      if (!started)
      {
         return;
      }
      queue.removeConsumer(this);
   }

   @Override
   public boolean isStarted()
   {
      return started;
   }

   @Override
   public HandleStatus handle(final MessageReference reference) throws Exception
   {
      if (reconnecting)
      {
         return HandleStatus.BUSY;
      }
      ServerMessage message = reference.getMessage();

      if (filter != null && !filter.match(message))
      {
         if (HornetQServerLogger.LOGGER.isTraceEnabled())
         {
            HornetQServerLogger.LOGGER.trace("Reference " + reference + " is a noMatch on consumer " + this);
         }
         return HandleStatus.NO_MATCH;
      }

      //we only accept if the alert is set
      if (!message.containsProperty(AeroGearConstants.AEROGEAR_ALERT))
      {
         return HandleStatus.NO_MATCH;
      }

      String alert = message.getTypedProperties().getProperty(AeroGearConstants.AEROGEAR_ALERT).toString();

      JavaSender sender = new SenderClient.Builder(endpoint).build();

      UnifiedMessage.Builder builder = new UnifiedMessage.Builder();

      builder.pushApplicationId(applicationId)
         .masterSecret(applicationMasterSecret)
         .alert(alert);

      String sound = message.containsProperty(AeroGearConstants.AEROGEAR_SOUND) ? message.getStringProperty(AeroGearConstants.AEROGEAR_SOUND) : this.sound;

      if (sound != null)
      {
         builder.sound(sound);
      }

      String badge = message.containsProperty(AeroGearConstants.AEROGEAR_BADGE) ? message.getStringProperty(AeroGearConstants.AEROGEAR_BADGE) : this.badge;

      if (badge != null)
      {
         builder.badge(badge);
      }

      boolean contentAvailable = message.containsProperty(AeroGearConstants.AEROGEAR_CONTENT_AVAILABLE) ? message.getBooleanProperty(AeroGearConstants.AEROGEAR_CONTENT_AVAILABLE) : this.contentAvailable;

      if (contentAvailable)
      {
         builder.contentAvailable();
      }

      String actionCategory = message.containsProperty(AeroGearConstants.AEROGEAR_ACTION_CATEGORY) ? message.getStringProperty(AeroGearConstants.AEROGEAR_ACTION_CATEGORY) : this.actionCategory;

      if (actionCategory != null)
      {
         builder.actionCategory(actionCategory);
      }

      Integer ttl = message.containsProperty(AeroGearConstants.AEROGEAR_TTL) ? message.getIntProperty(AeroGearConstants.AEROGEAR_TTL) : this.ttl;

      if (ttl != null)
      {
         builder.timeToLive(ttl);
      }

      String variantsString = message.containsProperty(AeroGearConstants.AEROGEAR_VARIANTS) ? message.getStringProperty(AeroGearConstants.AEROGEAR_VARIANTS) : null;

      String[] variants = variantsString != null ? variantsString.split(",") : this.variants;

      if (variants != null)
      {
         builder.variants(Arrays.asList(variants));
      }

      String aliasesString = message.containsProperty(AeroGearConstants.AEROGEAR_ALIASES) ? message.getStringProperty(AeroGearConstants.AEROGEAR_ALIASES) : null;

      String[] aliases = aliasesString != null ? aliasesString.split(",") : this.aliases;

      if (aliases != null)
      {
         builder.aliases(Arrays.asList(aliases));
      }

      String deviceTypesString = message.containsProperty(AeroGearConstants.AEROGEAR_DEVICE_TYPES) ? message.getStringProperty(AeroGearConstants.AEROGEAR_DEVICE_TYPES) : null;

      String[] deviceTypes = deviceTypesString != null ? deviceTypesString.split(",") : this.deviceTypes;

      if (deviceTypes != null)
      {
         builder.deviceType(Arrays.asList(deviceTypes));
      }

      Set<SimpleString> propertyNames = message.getPropertyNames();

      for (SimpleString propertyName : propertyNames)
      {
         if (propertyName.toString().startsWith("AEROGEAR_") && !AeroGearConstants.ALLOWABLE_PROPERTIES.contains(propertyName))
         {
            Object property = message.getTypedProperties().getProperty(propertyName);
            builder.attribute(propertyName.toString(), property.toString());
         }
      }

      UnifiedMessage unifiedMessage = builder.build();

      sender.send(unifiedMessage, this);

      if (handled)
      {
         reference.acknowledge();
         return HandleStatus.HANDLED;
      }
      //if we have been stopped we must return no match as we have been removed as a consumer,
      // anything else will cause an exception
      else if (!started)
      {
         return HandleStatus.NO_MATCH;
      }
      //we must be reconnecting
      return HandleStatus.BUSY;
   }


   @Override
   public void onComplete(int statusCode)
   {
      if (statusCode != 200)
      {
         handled = false;
         if (statusCode == 401)
         {
            HornetQAeroGearLogger.LOGGER.reply401();
         }
         else if (statusCode == 404)
         {
            HornetQAeroGearLogger.LOGGER.reply404();
         }
         else
         {
            HornetQAeroGearLogger.LOGGER.replyUnknown(statusCode);
         }

         queue.removeConsumer(this);
         started = false;
      }
      else
      {
         handled = true;
      }
   }

   @Override
   public void onError(Throwable throwable)
   {
      HornetQAeroGearLogger.LOGGER.sendFailed(retryInterval);
      handled = false;
      reconnecting = true;
      scheduledThreadPool.schedule(new ReconnectRunnable(0), retryInterval, TimeUnit.SECONDS);
   }

   private class ReconnectRunnable implements Runnable
   {

      private int retryAttempt;

      public ReconnectRunnable(int retryAttempt)
      {
         this.retryAttempt = retryAttempt;
      }

      @Override
      public void run()
      {
         try
         {
            HttpURLConnection conn = (HttpURLConnection) new URL(endpoint).openConnection();
            conn.connect();
            reconnecting = false;
            HornetQAeroGearLogger.LOGGER.connected(endpoint);
            queue.deliverAsync();
         }
         catch (Exception e)
         {
            retryAttempt++;
            if (retryAttempts == -1 || retryAttempt < retryAttempts)
            {
               scheduledThreadPool.schedule(this, retryInterval, TimeUnit.SECONDS);
            }
            else
            {
               HornetQAeroGearLogger.LOGGER.unableToReconnect(retryAttempt);
               started = false;
            }
         }
      }
   }

   @Override
   public List<MessageReference> getDeliveringMessages()
   {
      return Collections.emptyList();
   }

   @Override
   public void proceedDeliver(MessageReference reference) throws Exception
   {
      //noop
   }

   @Override
   public Filter getFilter()
   {
      return filter;
   }

   @Override
   public String debug()
   {
      return "aerogear connected to " + endpoint;
   }

   @Override
   public String toManagementString()
   {
      return "aerogear connected to " + endpoint;
   }

   @Override
   public void disconnect()
   {
   }
}
