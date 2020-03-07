/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Threading;
using Amqp.Framing;
using Amqp;
using System.Threading.Tasks;

namespace Artemis.Perf
{
   public class Producer
   {
       string name;

        string addr;
        string queue;
        int numberOfMessages;
        int messagesPerSecond;
        long messagesSent;

        public static long totalSent;

        public Producer(string name, string addr, string queue, int numberOfMessages, int messagesPerSecond) {
            this.name = name;
            this.addr = addr;
            this.queue = queue;
            this.numberOfMessages = numberOfMessages;
            this.messagesPerSecond = messagesPerSecond;
        }


        public void produce() {
            Address address = new Address(addr);

            Connection connection = new Connection(address);


            Session session = new Session(connection);
            SenderLink sender = new SenderLink(session, "sender", queue);

            OutcomeCallback callback = (l, msg, o, s) => { 
                Interlocked.Increment(ref messagesSent);
                Interlocked.Increment(ref totalSent);
            };

            // This is just to limit the number of messages per second we are sending
            TokenBucketLimiterImpl tokens = new TokenBucketLimiterImpl(messagesPerSecond);

            Task.Factory.StartNew(() =>  {
                Console.WriteLine("Sending {0} messages...", numberOfMessages);
                for (var i = 0; i < numberOfMessages; i++)
                {
                    tokens.limit();
                    Message message = new Message("a message!" + i);
                    message.Header = new Header();
                    message.Header.Durable = true;

                    // The callback here is to make the sending to happen as fast as possible
                    sender.Send(message, callback, null);
                }
                Console.WriteLine(".... Done sending");
            }, TaskCreationOptions.LongRunning);

            // Trace.TraceLevel = TraceLevel.Verbose | TraceLevel.Error |
            // TraceLevel.Frame | TraceLevel.Information | TraceLevel.Warning;
            // Trace.TraceListener = (l, f, o) => Console.WriteLine(DateTime.Now.ToString("[hh:mm:ss.fff]") + " " + string.Format(f, o));

            // sender.Close();

            // Task.Factory.StartNew(() =>  {
            //     while (true) {
            //         Console.WriteLine("Sent " + Interlocked.Read(ref messagesSent) + " on queue " + queue + " producer " + this.name);
            //         Thread.Sleep(1000);
            //     }
            // }, TaskCreationOptions.LongRunning);



        }
    }
}
