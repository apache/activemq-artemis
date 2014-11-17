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
package org.apache.activemq.integration.twitter;

import java.util.HashSet;
import java.util.Set;

/**
 * A TwitterConstants
 *
 * @author <a href="tm.igarashi@gmail.com">Tomohisa Igarashi</a>
 */
public final class TwitterConstants
{
   public static final String KEY_ID = "id";
   public static final String KEY_SOURCE = "source";
   public static final String KEY_CREATED_AT = "createdAt";
   public static final String KEY_IS_TRUNCATED = "isTruncated";
   public static final String KEY_IN_REPLY_TO_STATUS_ID = "inReplyToStatusId";
   public static final String KEY_IN_REPLY_TO_USER_ID = "inReplyToUserId";
   public static final String KEY_IN_REPLY_TO_SCREEN_NAME = "inReplyToScreenName";
   public static final String KEY_IS_FAVORITED = "isFavorited";
   public static final String KEY_IS_RETWEET = "isRetweet";
   public static final String KEY_CONTRIBUTORS = "contributors";
   public static final String KEY_GEO_LOCATION_LATITUDE = "geoLocation.latitude";
   public static final String KEY_GEO_LOCATION_LONGITUDE = "geoLocation.longitude";
   public static final String KEY_PLACE_ID = "place.id";
   public static final String KEY_DISPLAY_COODINATES = "displayCoodinates";

   public static final int DEFAULT_POLLING_INTERVAL_SECS = 10;
   public static final int DEFAULT_PAGE_SIZE = 100;
   public static final int FIRST_ATTEMPT_PAGE_SIZE = 1;
   public static final int START_SINCE_ID = 1;
   public static final int INITIAL_MESSAGE_BUFFER_SIZE = 50;

   public static final Set<String> ALLOWABLE_INCOMING_CONNECTOR_KEYS;
   public static final Set<String> REQUIRED_INCOMING_CONNECTOR_KEYS;

   public static final Set<String> ALLOWABLE_OUTGOING_CONNECTOR_KEYS;
   public static final Set<String> REQUIRED_OUTGOING_CONNECTOR_KEYS;

   public static final String CONSUMER_KEY = "consumerKey";
   public static final String CONSUMER_SECRET = "consumerSecret";
   public static final String ACCESS_TOKEN = "accessToken";
   public static final String ACCESS_TOKEN_SECRET = "accessTokenSecret";
   public static final String QUEUE_NAME = "queue";
   public static final String INCOMING_INTERVAL = "interval";

   static
   {
      ALLOWABLE_INCOMING_CONNECTOR_KEYS = new HashSet<String>();
      ALLOWABLE_INCOMING_CONNECTOR_KEYS.add(CONSUMER_KEY);
      ALLOWABLE_INCOMING_CONNECTOR_KEYS.add(CONSUMER_SECRET);
      ALLOWABLE_INCOMING_CONNECTOR_KEYS.add(ACCESS_TOKEN);
      ALLOWABLE_INCOMING_CONNECTOR_KEYS.add(ACCESS_TOKEN_SECRET);
      ALLOWABLE_INCOMING_CONNECTOR_KEYS.add(QUEUE_NAME);
      ALLOWABLE_INCOMING_CONNECTOR_KEYS.add(INCOMING_INTERVAL);

      REQUIRED_INCOMING_CONNECTOR_KEYS = new HashSet<String>();
      REQUIRED_INCOMING_CONNECTOR_KEYS.add(CONSUMER_KEY);
      REQUIRED_INCOMING_CONNECTOR_KEYS.add(CONSUMER_SECRET);
      REQUIRED_INCOMING_CONNECTOR_KEYS.add(ACCESS_TOKEN);
      REQUIRED_INCOMING_CONNECTOR_KEYS.add(ACCESS_TOKEN_SECRET);
      REQUIRED_INCOMING_CONNECTOR_KEYS.add(QUEUE_NAME);

      ALLOWABLE_OUTGOING_CONNECTOR_KEYS = new HashSet<String>();
      ALLOWABLE_OUTGOING_CONNECTOR_KEYS.add(CONSUMER_KEY);
      ALLOWABLE_OUTGOING_CONNECTOR_KEYS.add(CONSUMER_SECRET);
      ALLOWABLE_OUTGOING_CONNECTOR_KEYS.add(ACCESS_TOKEN);
      ALLOWABLE_OUTGOING_CONNECTOR_KEYS.add(ACCESS_TOKEN_SECRET);
      ALLOWABLE_OUTGOING_CONNECTOR_KEYS.add(QUEUE_NAME);

      REQUIRED_OUTGOING_CONNECTOR_KEYS = new HashSet<String>();
      REQUIRED_OUTGOING_CONNECTOR_KEYS.add(CONSUMER_KEY);
      REQUIRED_OUTGOING_CONNECTOR_KEYS.add(CONSUMER_SECRET);
      REQUIRED_OUTGOING_CONNECTOR_KEYS.add(ACCESS_TOKEN);
      REQUIRED_OUTGOING_CONNECTOR_KEYS.add(ACCESS_TOKEN_SECRET);
      REQUIRED_OUTGOING_CONNECTOR_KEYS.add(QUEUE_NAME);
   }

   private TwitterConstants()
   {
      // utility class
   }
}
