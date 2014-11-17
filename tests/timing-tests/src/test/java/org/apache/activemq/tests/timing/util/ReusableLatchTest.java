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
package org.apache.activemq.tests.timing.util;

import org.junit.Test;

import org.junit.Assert;

import org.apache.activemq.tests.util.UnitTestCase;
import org.apache.activemq.utils.ReusableLatch;

/**
 * @author <a href="csuconic@redhat.com">Clebert Suconic</a>
 */
public class ReusableLatchTest extends UnitTestCase
{
   @Test
   public void testTimeout() throws Exception
   {
      ReusableLatch latch = new ReusableLatch();

      latch.countUp();

      long start = System.currentTimeMillis();
      Assert.assertFalse(latch.await(1000));
      long end = System.currentTimeMillis();

      Assert.assertTrue("Timeout didn't work correctly", end - start >= 1000 && end - start < 2000);
   }
}
