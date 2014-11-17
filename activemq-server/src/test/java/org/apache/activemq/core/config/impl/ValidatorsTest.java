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

import org.junit.Test;

import org.junit.Assert;


import org.apache.activemq.core.server.JournalType;
import org.apache.activemq.tests.util.RandomUtil;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class ValidatorsTest extends Assert
{

   private static void success(final Validators.Validator validator, final Object value)
   {
      validator.validate(RandomUtil.randomString(), value);
   }

   private static void failure(final Validators.Validator validator, final Object value)
   {
      try
      {
         validator.validate(RandomUtil.randomString(), value);
         Assert.fail(validator + " must not validate " + value);
      }
      catch (IllegalArgumentException e)
      {

      }
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testGE_ZERO() throws Exception
   {
      ValidatorsTest.failure(Validators.GE_ZERO, -1);
      ValidatorsTest.success(Validators.GE_ZERO, 0);
      ValidatorsTest.success(Validators.GE_ZERO, 0.1);
      ValidatorsTest.success(Validators.GE_ZERO, 1);
   }

   @Test
   public void testGT_ZERO() throws Exception
   {
      ValidatorsTest.failure(Validators.GT_ZERO, -1);
      ValidatorsTest.failure(Validators.GT_ZERO, 0);
      ValidatorsTest.success(Validators.GT_ZERO, 0.1);
      ValidatorsTest.success(Validators.GT_ZERO, 1);
   }

   @Test
   public void testMINUS_ONE_OR_GE_ZERO() throws Exception
   {
      ValidatorsTest.failure(Validators.MINUS_ONE_OR_GE_ZERO, -2);
      ValidatorsTest.success(Validators.MINUS_ONE_OR_GE_ZERO, -1);
      ValidatorsTest.success(Validators.MINUS_ONE_OR_GE_ZERO, 0);
      ValidatorsTest.success(Validators.MINUS_ONE_OR_GE_ZERO, 0.1);
      ValidatorsTest.success(Validators.MINUS_ONE_OR_GE_ZERO, 1);
   }

   @Test
   public void testMINUS_ONE_OR_GT_ZERO() throws Exception
   {
      ValidatorsTest.failure(Validators.MINUS_ONE_OR_GT_ZERO, -2);
      ValidatorsTest.success(Validators.MINUS_ONE_OR_GT_ZERO, -1);
      ValidatorsTest.failure(Validators.MINUS_ONE_OR_GT_ZERO, 0);
      ValidatorsTest.success(Validators.MINUS_ONE_OR_GT_ZERO, 0.1);
      ValidatorsTest.success(Validators.MINUS_ONE_OR_GT_ZERO, 1);
   }

   @Test
   public void testNO_CHECK() throws Exception
   {
      ValidatorsTest.success(Validators.NO_CHECK, -1);
      ValidatorsTest.success(Validators.NO_CHECK, null);
      ValidatorsTest.success(Validators.NO_CHECK, "");
      ValidatorsTest.success(Validators.NO_CHECK, true);
      ValidatorsTest.success(Validators.NO_CHECK, false);
   }

   @Test
   public void testNOT_NULL_OR_EMPTY() throws Exception
   {
      ValidatorsTest.failure(Validators.NOT_NULL_OR_EMPTY, null);
      ValidatorsTest.failure(Validators.NOT_NULL_OR_EMPTY, "");
      ValidatorsTest.success(Validators.NOT_NULL_OR_EMPTY, RandomUtil.randomString());
   }

   @Test
   public void testJOURNAL_TYPE() throws Exception
   {
      for (JournalType type : JournalType.values())
      {
         ValidatorsTest.success(Validators.JOURNAL_TYPE, type.toString());
      }
      ValidatorsTest.failure(Validators.JOURNAL_TYPE, null);
      ValidatorsTest.failure(Validators.JOURNAL_TYPE, "");
      ValidatorsTest.failure(Validators.JOURNAL_TYPE, RandomUtil.randomString());
   }

   @Test
   public void testPERCENTAGE()
   {
      ValidatorsTest.success(Validators.PERCENTAGE, 99);
      ValidatorsTest.success(Validators.PERCENTAGE, 100);
      ValidatorsTest.success(Validators.PERCENTAGE, 0);
      ValidatorsTest.failure(Validators.PERCENTAGE, -1);
      ValidatorsTest.failure(Validators.PERCENTAGE, 101);
      ValidatorsTest.failure(Validators.PERCENTAGE, null);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
