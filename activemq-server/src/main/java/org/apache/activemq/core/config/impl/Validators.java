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
package org.apache.activemq.core.config.impl;

import org.apache.activemq.core.server.HornetQMessageBundle;
import org.apache.activemq.core.server.JournalType;
import org.apache.activemq.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.core.settings.impl.SlowConsumerPolicy;

/**
 * A Validators.
 *
 * @author jmesnil
 */
public final class Validators
{
   public interface Validator
   {
      void validate(String name, Object value);
   }

   public static final Validator NO_CHECK = new Validator()
   {
      public void validate(final String name, final Object value)
      {
         return;
      }
   };

   public static final Validator NOT_NULL_OR_EMPTY = new Validator()
   {
      public void validate(final String name, final Object value)
      {
         String str = (String) value;
         if (str == null || str.length() == 0)
         {
            throw HornetQMessageBundle.BUNDLE.emptyOrNull(name);
         }
      }
   };

   public static final Validator GT_ZERO = new Validator()
   {
      public void validate(final String name, final Object value)
      {
         Number val = (Number) value;
         if (val.doubleValue() > 0)
         {
            // OK
         }
         else
         {
            throw HornetQMessageBundle.BUNDLE.greaterThanZero(name, val);
         }
      }
   };

   public static final Validator PERCENTAGE = new Validator()
   {
      public void validate(final String name, final Object value)
      {
         Number val = (Number) value;
         if (val == null || (val.intValue() < 0 || val.intValue() > 100))
         {
            throw HornetQMessageBundle.BUNDLE.notPercent(name, val);
         }
      }
   };

   public static final Validator GE_ZERO = new Validator()
   {
      public void validate(final String name, final Object value)
      {
         Number val = (Number) value;
         if (val.doubleValue() >= 0)
         {
            // OK
         }
         else
         {
            throw HornetQMessageBundle.BUNDLE.greaterThanZero(name, val);
         }
      }
   };

   public static final Validator MINUS_ONE_OR_GT_ZERO = new Validator()
   {
      public void validate(final String name, final Object value)
      {
         Number val = (Number) value;
         if (val.doubleValue() == -1 || val.doubleValue() > 0)
         {
            // OK
         }
         else
         {
            throw HornetQMessageBundle.BUNDLE.greaterThanMinusOne(name, val);
         }
      }
   };

   public static final Validator MINUS_ONE_OR_GE_ZERO = new Validator()
   {
      public void validate(final String name, final Object value)
      {
         Number val = (Number) value;
         if (val.doubleValue() == -1 || val.doubleValue() >= 0)
         {
            // OK
         }
         else
         {
            throw HornetQMessageBundle.BUNDLE.greaterThanZeroOrMinusOne(name, val);
         }
      }
   };

   public static final Validator THREAD_PRIORITY_RANGE = new Validator()
   {
      public void validate(final String name, final Object value)
      {
         Number val = (Number) value;
         if (val.intValue() >= Thread.MIN_PRIORITY && val.intValue() <= Thread.MAX_PRIORITY)
         {
            // OK
         }
         else
         {
            throw HornetQMessageBundle.BUNDLE.mustbeBetween(name, Thread.MIN_PRIORITY, Thread.MAX_PRIORITY, value);
         }
      }
   };

   public static final Validator JOURNAL_TYPE = new Validator()
   {
      public void validate(final String name, final Object value)
      {
         String val = (String) value;
         if (val == null || !val.equals(JournalType.NIO.toString()) && !val.equals(JournalType.ASYNCIO.toString()))
         {
            throw HornetQMessageBundle.BUNDLE.invalidJournalType(val);
         }
      }
   };

   public static final Validator ADDRESS_FULL_MESSAGE_POLICY_TYPE = new Validator()
   {
      public void validate(final String name, final Object value)
      {
         String val = (String) value;
         if (val == null || !val.equals(AddressFullMessagePolicy.PAGE.toString()) &&
               !val.equals(AddressFullMessagePolicy.DROP.toString()) &&
               !val.equals(AddressFullMessagePolicy.BLOCK.toString()) &&
               !val.equals(AddressFullMessagePolicy.FAIL.toString()))
         {
            throw HornetQMessageBundle.BUNDLE.invalidAddressFullPolicyType(val);
         }
      }
   };

   public static final Validator SLOW_CONSUMER_POLICY_TYPE = new Validator()
   {
      public void validate(final String name, final Object value)
      {
         String val = (String) value;
         if (val == null || !val.equals(SlowConsumerPolicy.KILL.toString()) &&
            !val.equals(SlowConsumerPolicy.NOTIFY.toString()))
         {
            throw HornetQMessageBundle.BUNDLE.invalidSlowConsumerPolicyType(val);
         }
      }
   };
}
