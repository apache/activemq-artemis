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

package org.apache.activemq.artemis.cli.commands.messages;

import io.airlift.airline.Option;

public class DestAbstract extends ConnectionAbstract {

   @Option(name = "--destination", description = "Destination to be used. It can be prefixed with queue:// or topic:// (Default: queue://TEST)")
   String destination = "queue://TEST";

   @Option(name = "--message-count", description = "Number of messages to act on (Default: 1000)")
   int messageCount = 1000;

   @Option(name = "--sleep", description = "Time wait between each message")
   int sleep = 0;

   @Option(name = "--txt-size", description = "TX Batch Size")
   int txBatchSize;

   @Option(name = "--threads", description = "Number of Threads to be used (Default: 1)")
   int threads = 1;

}
