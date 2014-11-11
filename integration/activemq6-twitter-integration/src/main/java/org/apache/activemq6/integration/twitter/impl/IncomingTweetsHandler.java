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
package org.apache.activemq6.integration.twitter.impl;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.activemq6.api.core.SimpleString;
import org.apache.activemq6.core.persistence.StorageManager;
import org.apache.activemq6.core.postoffice.Binding;
import org.apache.activemq6.core.postoffice.PostOffice;
import org.apache.activemq6.core.server.ConnectorService;
import org.apache.activemq6.core.server.ServerMessage;
import org.apache.activemq6.core.server.impl.ServerMessageImpl;
import org.apache.activemq6.integration.twitter.TwitterConstants;
import org.apache.activemq6.twitter.HornetQTwitterLogger;
import org.apache.activemq6.utils.ConfigurationHelper;
import twitter4j.GeoLocation;
import twitter4j.Paging;
import twitter4j.Place;
import twitter4j.ResponseList;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.http.AccessToken;

/**
 * IncomingTweetsHandler consumes from twitter and forwards to the
 * configured HornetQ address.
 */
public class IncomingTweetsHandler implements ConnectorService
{
   private final String connectorName;

   private final String consumerKey;

   private final String consumerSecret;

   private final String accessToken;

   private final String accessTokenSecret;

   private final String queueName;

   private final int intervalSeconds;

   private final StorageManager storageManager;

   private final PostOffice postOffice;

   private Paging paging;

   private Twitter twitter;

   private boolean isStarted = false;

   private final ScheduledExecutorService scheduledPool;

   private ScheduledFuture<?> scheduledFuture;

   public IncomingTweetsHandler(final String connectorName,
                                final Map<String, Object> configuration,
                                final StorageManager storageManager,
                                final PostOffice postOffice,
                                final ScheduledExecutorService scheduledThreadPool)
   {
      this.connectorName = connectorName;
      this.consumerKey = ConfigurationHelper.getStringProperty(TwitterConstants.CONSUMER_KEY, null, configuration);
      this.consumerSecret = ConfigurationHelper.getStringProperty(TwitterConstants.CONSUMER_SECRET, null, configuration);
      this.accessToken = ConfigurationHelper.getStringProperty(TwitterConstants.ACCESS_TOKEN, null, configuration);
      this.accessTokenSecret = ConfigurationHelper.getStringProperty(TwitterConstants.ACCESS_TOKEN_SECRET, null, configuration);
      this.queueName = ConfigurationHelper.getStringProperty(TwitterConstants.QUEUE_NAME, null, configuration);
      Integer intervalSeconds = ConfigurationHelper.getIntProperty(TwitterConstants.INCOMING_INTERVAL, 0, configuration);
      if (intervalSeconds > 0)
      {
         this.intervalSeconds = intervalSeconds;
      }
      else
      {
         this.intervalSeconds = TwitterConstants.DEFAULT_POLLING_INTERVAL_SECS;
      }
      this.storageManager = storageManager;
      this.postOffice = postOffice;
      this.scheduledPool = scheduledThreadPool;
   }

   public void start() throws Exception
   {
      Binding b = postOffice.getBinding(new SimpleString(queueName));
      if (b == null)
      {
         throw new Exception(connectorName + ": queue " + queueName + " not found");
      }

      paging = new Paging();
      TwitterFactory tf = new TwitterFactory();
      this.twitter = tf.getOAuthAuthorizedInstance(this.consumerKey,
                                                   this.consumerSecret,
                                                   new AccessToken(this.accessToken,
                                                                   this.accessTokenSecret));
      this.twitter.verifyCredentials();

      // getting latest ID
      this.paging.setCount(TwitterConstants.FIRST_ATTEMPT_PAGE_SIZE);

      // If I used annotations here, it won't compile under JDK 1.7
      ResponseList res = this.twitter.getHomeTimeline(paging);
      this.paging.setSinceId(((Status) res.get(0)).getId());
      HornetQTwitterLogger.LOGGER.debug(connectorName + " initialise(): got latest ID: " + this.paging.getSinceId());

      // TODO make page size configurable
      this.paging.setCount(TwitterConstants.DEFAULT_PAGE_SIZE);

      scheduledFuture = this.scheduledPool.scheduleWithFixedDelay(new TweetsRunnable(),
                                                                  intervalSeconds,
                                                                  intervalSeconds,
                                                                  TimeUnit.SECONDS);
      isStarted = true;
   }

   public void stop() throws Exception
   {
      if (!isStarted)
      {
         return;
      }
      scheduledFuture.cancel(true);
      paging = null;
      isStarted = false;
   }

   public boolean isStarted()
   {
      return isStarted;
   }

   private void poll() throws Exception
   {
      // get new tweets
      // If I used annotations here, it won't compile under JDK 1.7
      ResponseList res = this.twitter.getHomeTimeline(paging);

      if (res == null || res.size() == 0)
      {
         return;
      }

      for (int i = res.size() - 1; i >= 0; i--)
      {
         Status status = (Status) res.get(i);

         ServerMessage msg = new ServerMessageImpl(this.storageManager.generateID(),
                                                   TwitterConstants.INITIAL_MESSAGE_BUFFER_SIZE);
         msg.setAddress(new SimpleString(this.queueName));
         msg.setDurable(true);
         msg.encodeMessageIDToBuffer();

         putTweetIntoMessage(status, msg);

         this.postOffice.route(msg, false);
         HornetQTwitterLogger.LOGGER.debug(connectorName + ": routed: " + status.toString());
      }

      this.paging.setSinceId(((Status) res.get(0)).getId());
      HornetQTwitterLogger.LOGGER.debug(connectorName + ": update latest ID: " + this.paging.getSinceId());
   }

   private void putTweetIntoMessage(final Status status, final ServerMessage msg)
   {
      msg.getBodyBuffer().writeString(status.getText());
      msg.putLongProperty(TwitterConstants.KEY_ID, status.getId());
      msg.putStringProperty(TwitterConstants.KEY_SOURCE, status.getSource());

      msg.putLongProperty(TwitterConstants.KEY_CREATED_AT, status.getCreatedAt().getTime());
      msg.putBooleanProperty(TwitterConstants.KEY_IS_TRUNCATED, status.isTruncated());
      msg.putLongProperty(TwitterConstants.KEY_IN_REPLY_TO_STATUS_ID, status.getInReplyToStatusId());
      msg.putIntProperty(TwitterConstants.KEY_IN_REPLY_TO_USER_ID, status.getInReplyToUserId());
      msg.putBooleanProperty(TwitterConstants.KEY_IS_FAVORITED, status.isFavorited());
      msg.putBooleanProperty(TwitterConstants.KEY_IS_RETWEET, status.isRetweet());
      msg.putObjectProperty(TwitterConstants.KEY_CONTRIBUTORS, status.getContributors());
      GeoLocation gl;
      if ((gl = status.getGeoLocation()) != null)
      {
         msg.putDoubleProperty(TwitterConstants.KEY_GEO_LOCATION_LATITUDE, gl.getLatitude());
         msg.putDoubleProperty(TwitterConstants.KEY_GEO_LOCATION_LONGITUDE, gl.getLongitude());
      }
      Place place;
      if ((place = status.getPlace()) != null)
      {
         msg.putStringProperty(TwitterConstants.KEY_PLACE_ID, place.getId());
      }
   }

   public String getName()
   {
      return connectorName;
   }

   private final class TweetsRunnable implements Runnable
   {
      /**
       * TODO streaming API support
       * TODO rate limit support
       */
      public void run()
      {
         // Avoid canceling the task with RuntimeException
         try
         {
            poll();
         }
         catch (Throwable t)
         {
            HornetQTwitterLogger.LOGGER.errorPollingTwitter(t);
         }
      }
   }
}
