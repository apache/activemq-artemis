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
package org.apache.activemq.integration.twitter.impl;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.core.filter.Filter;
import org.apache.activemq.core.postoffice.Binding;
import org.apache.activemq.core.postoffice.PostOffice;
import org.apache.activemq.core.server.ConnectorService;
import org.apache.activemq.core.server.Consumer;
import org.apache.activemq.core.server.HandleStatus;
import org.apache.activemq.core.server.HornetQServerLogger;
import org.apache.activemq.core.server.MessageReference;
import org.apache.activemq.core.server.Queue;
import org.apache.activemq.core.server.ServerMessage;
import org.apache.activemq.integration.twitter.TwitterConstants;
import org.apache.activemq.twitter.HornetQTwitterLogger;
import org.apache.activemq.utils.ConfigurationHelper;
import twitter4j.GeoLocation;
import twitter4j.StatusUpdate;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.http.AccessToken;

/**
 * OutgoingTweetsHandler consumes from configured HornetQ address
 * and forwards to the twitter.
 */
public class OutgoingTweetsHandler implements Consumer, ConnectorService
{
   private final String connectorName;

   private final String consumerKey;

   private final String consumerSecret;

   private final String accessToken;

   private final String accessTokenSecret;

   private final String queueName;

   private final PostOffice postOffice;

   private Twitter twitter = null;

   private Queue queue = null;

   private Filter filter = null;

   private boolean isStarted = false;


   public String debug()
   {
      return toString();
   }

   public OutgoingTweetsHandler(final String connectorName,
                                final Map<String, Object> configuration,
                                final PostOffice postOffice)
   {
      this.connectorName = connectorName;
      this.consumerKey = ConfigurationHelper.getStringProperty(TwitterConstants.CONSUMER_KEY, null, configuration);
      this.consumerSecret = ConfigurationHelper.getStringProperty(TwitterConstants.CONSUMER_SECRET, null, configuration);
      this.accessToken = ConfigurationHelper.getStringProperty(TwitterConstants.ACCESS_TOKEN, null, configuration);
      this.accessTokenSecret = ConfigurationHelper.getStringProperty(TwitterConstants.ACCESS_TOKEN_SECRET, null, configuration);
      this.queueName = ConfigurationHelper.getStringProperty(TwitterConstants.QUEUE_NAME, null, configuration);
      this.postOffice = postOffice;
   }

   /**
    * TODO streaming API support
    * TODO rate limit support
    */
   public synchronized void start() throws Exception
   {
      if (this.isStarted)
      {
         return;
      }

      if (this.connectorName == null || this.connectorName.trim().equals(""))
      {
         throw new Exception("invalid connector name: " + this.connectorName);
      }

      if (this.queueName == null || this.queueName.trim().equals(""))
      {
         throw new Exception("invalid queue name: " + queueName);
      }

      SimpleString name = new SimpleString(this.queueName);
      Binding b = this.postOffice.getBinding(name);
      if (b == null)
      {
         throw new Exception(connectorName + ": queue " + queueName + " not found");
      }
      this.queue = (Queue) b.getBindable();

      TwitterFactory tf = new TwitterFactory();
      this.twitter = tf.getOAuthAuthorizedInstance(this.consumerKey,
                                                   this.consumerSecret,
                                                   new AccessToken(this.accessToken,
                                                                   this.accessTokenSecret));
      this.twitter.verifyCredentials();

      // TODO make filter-string configurable
      // this.filter = FilterImpl.createFilter(filterString);
      this.filter = null;

      this.queue.addConsumer(this);

      this.queue.deliverAsync();
      this.isStarted = true;
      HornetQTwitterLogger.LOGGER.debug(connectorName + ": started");
   }

   public boolean isStarted()
   {
      return isStarted;  //To change body of implemented methods use File | Settings | File Templates.
   }

   public synchronized void stop() throws Exception
   {
      if (!this.isStarted)
      {
         return;
      }

      HornetQTwitterLogger.LOGGER.debug(connectorName + ": receive shutdown request");

      this.queue.removeConsumer(this);

      this.isStarted = false;
      HornetQTwitterLogger.LOGGER.debug(connectorName + ": shutdown");
   }

   public String getName()
   {
      return connectorName;
   }

   public Filter getFilter()
   {
      return filter;
   }

   /* (non-Javadoc)
    * @see org.apache.activemq.core.server.Consumer#getDeliveringMessages()
    */
   @Override
   public List<MessageReference> getDeliveringMessages()
   {
      return Collections.emptyList();
   }

   public HandleStatus handle(final MessageReference ref) throws Exception
   {
      if (filter != null && !filter.match(ref.getMessage()))
      {
         return HandleStatus.NO_MATCH;
      }

      synchronized (this)
      {
         ref.handled();

         ServerMessage message = ref.getMessage();

         StatusUpdate status = new StatusUpdate(message.getBodyBuffer().readString());

         // set optional property

         if (message.containsProperty(TwitterConstants.KEY_IN_REPLY_TO_STATUS_ID))
         {
            status.setInReplyToStatusId(message.getLongProperty(TwitterConstants.KEY_IN_REPLY_TO_STATUS_ID));
         }

         if (message.containsProperty(TwitterConstants.KEY_GEO_LOCATION_LATITUDE))
         {
            double geolat = message.getDoubleProperty(TwitterConstants.KEY_GEO_LOCATION_LATITUDE);
            double geolong = message.getDoubleProperty(TwitterConstants.KEY_GEO_LOCATION_LONGITUDE);
            status.setLocation(new GeoLocation(geolat, geolong));
         }

         if (message.containsProperty(TwitterConstants.KEY_PLACE_ID))
         {
            status.setPlaceId(message.getStringProperty(TwitterConstants.KEY_PLACE_ID));
         }

         if (message.containsProperty(TwitterConstants.KEY_DISPLAY_COODINATES))
         {
            status.setDisplayCoordinates(message.getBooleanProperty(TwitterConstants.KEY_DISPLAY_COODINATES));
         }

         // send to Twitter
         try
         {
            this.twitter.updateStatus(status);
         }
         catch (TwitterException e)
         {
            if (e.getStatusCode() == 403)
            {
               // duplicated message
               HornetQTwitterLogger.LOGGER.error403(connectorName);
               queue.acknowledge(ref);

               return HandleStatus.HANDLED;
            }
            else
            {
               throw e;
            }
         }

         queue.acknowledge(ref);
         HornetQTwitterLogger.LOGGER.debug(connectorName + ": forwarded to twitter: " + message.getMessageID());
         return HandleStatus.HANDLED;
      }
   }

   public void proceedDeliver(MessageReference ref)
   {
      // no op
   }

   @Override
   public String toManagementString()
   {
      return toString();
   }

   @Override
   public void disconnect()
   {
      try
      {
         stop();
      }
      catch (Exception e)
      {
         HornetQServerLogger.LOGGER.errorStoppingConnectorService(e, getName());
      }
   }
}
