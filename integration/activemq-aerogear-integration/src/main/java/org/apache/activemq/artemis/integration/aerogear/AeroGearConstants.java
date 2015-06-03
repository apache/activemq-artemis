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
package org.apache.activemq.artemis.integration.aerogear;

import org.apache.activemq.artemis.api.core.SimpleString;

import java.util.HashSet;
import java.util.Set;

public class AeroGearConstants
{
   public static final Set<String> ALLOWABLE_PROPERTIES = new HashSet<>();
   public static final Set<String> REQUIRED_PROPERTIES = new HashSet<>();

   public static final String QUEUE_NAME = "queue";
   public static final String ENDPOINT_NAME = "endpoint";
   public static final String APPLICATION_ID_NAME = "application-id";
   public static final String APPLICATION_MASTER_SECRET_NAME = "master-secret";
   public static final String TTL_NAME = "ttl";
   public static final String BADGE_NAME = "badge";
   public static final String SOUND_NAME = "sound";
   public static final String CONTENT_AVAILABLE_NAME = "content-available";
   public static final String ACTION_CATEGORY_NAME = "action-category";
   public static final String FILTER_NAME = "filter";
   public static final String RETRY_INTERVAL_NAME = "retry-interval";
   public static final String RETRY_ATTEMPTS_NAME = "retry-attempts";
   public static final String VARIANTS_NAME = "variants";
   public static final String ALIASES_NAME = "aliases";
   public static final String DEVICE_TYPE_NAME = "device-types";


   public static final SimpleString AEROGEAR_ALERT = new SimpleString("AEROGEAR_ALERT");
   public static final SimpleString AEROGEAR_SOUND = new SimpleString("AEROGEAR_SOUND");
   public static final SimpleString AEROGEAR_CONTENT_AVAILABLE = new SimpleString("AEROGEAR_CONTENT_AVAILABLE");
   public static final SimpleString AEROGEAR_ACTION_CATEGORY = new SimpleString("AEROGEAR_ACTION_CATEGORY");
   public static final SimpleString AEROGEAR_BADGE = new SimpleString("AEROGEAR_BADGE");
   public static final SimpleString AEROGEAR_TTL = new SimpleString("AEROGEAR_TTL");
   public static final SimpleString AEROGEAR_VARIANTS = new SimpleString("AEROGEAR_VARIANTS");
   public static final SimpleString AEROGEAR_ALIASES = new SimpleString("AEROGEAR_ALIASES");
   public static final SimpleString AEROGEAR_DEVICE_TYPES = new SimpleString("AEROGEAR_DEVICE_TYPES");

   public static final String DEFAULT_SOUND = "default";
   public static final Integer DEFAULT_TTL = 3600;
   public static final int DEFAULT_RETRY_INTERVAL = 5;
   public static final int DEFAULT_RETRY_ATTEMPTS = 5;

   static
   {
      ALLOWABLE_PROPERTIES.add(QUEUE_NAME);
      ALLOWABLE_PROPERTIES.add(ENDPOINT_NAME);
      ALLOWABLE_PROPERTIES.add(APPLICATION_ID_NAME);
      ALLOWABLE_PROPERTIES.add(APPLICATION_MASTER_SECRET_NAME);
      ALLOWABLE_PROPERTIES.add(TTL_NAME);
      ALLOWABLE_PROPERTIES.add(BADGE_NAME);
      ALLOWABLE_PROPERTIES.add(SOUND_NAME);
      ALLOWABLE_PROPERTIES.add(CONTENT_AVAILABLE_NAME);
      ALLOWABLE_PROPERTIES.add(ACTION_CATEGORY_NAME);
      ALLOWABLE_PROPERTIES.add(FILTER_NAME);
      ALLOWABLE_PROPERTIES.add(RETRY_INTERVAL_NAME);
      ALLOWABLE_PROPERTIES.add(RETRY_ATTEMPTS_NAME);
      ALLOWABLE_PROPERTIES.add(VARIANTS_NAME);
      ALLOWABLE_PROPERTIES.add(ALIASES_NAME);
      ALLOWABLE_PROPERTIES.add(DEVICE_TYPE_NAME);

      REQUIRED_PROPERTIES.add(QUEUE_NAME);
      REQUIRED_PROPERTIES.add(ENDPOINT_NAME);
      REQUIRED_PROPERTIES.add(APPLICATION_ID_NAME);
      REQUIRED_PROPERTIES.add(APPLICATION_MASTER_SECRET_NAME);
   }

}
