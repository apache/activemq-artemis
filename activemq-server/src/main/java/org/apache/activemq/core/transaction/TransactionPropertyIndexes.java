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
package org.apache.activemq6.core.transaction;

/**
 * A TransactionPropertyIndexes
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 * Created 2 Jan 2009 19:48:07
 *
 *
 */
public class TransactionPropertyIndexes
{

   public static final int LARGE_MESSAGE_CONFIRMATIONS = 1;

   public static final int PAGE_SYNC = 2;

   public static final int PAGE_COUNT_INC = 3;

   public static final int PAGE_TRANSACTION_UPDATE = 4;

   public static final int PAGE_TRANSACTION = 5;

   public static final int REFS_OPERATION = 6;

   public static final int PAGE_DELIVERY = 7;

   public static final int PAGE_CURSOR_POSITIONS = 8;
}
