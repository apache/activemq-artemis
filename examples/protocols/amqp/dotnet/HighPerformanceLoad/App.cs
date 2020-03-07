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
   class App
    {
        static long ReceivedMessages = 0;

        static void TheCallback(int id, Session session, ReceiverLink link, Message message)
        {
            Interlocked.Increment(ref ReceivedMessages);
            link.Accept(message);
        }

        static void Main(string[] args) {
            
            if (args.Length == 0) {
                args = new string[1];
                args[0] = "amqp://127.0.0.1:5672";
            }

            // it will start one client towards each server
            for (int i = 0; i < args.Length; i++) {
                string addr0  = args.Length >= 1 ? args[0] : "amqp://127.0.0.1:5672";
                processOn(addr0, "orders", 100000000, 25000, "p1");
            }

            while (true) {
                long previousRead = Interlocked.Read(ref ReceivedMessages);
                long previousSent = Interlocked.Read(ref Producer.totalSent);
                Thread.Sleep(1000);
                long currentRead = Interlocked.Read(ref ReceivedMessages);
                long currentSent = Interlocked.Read(ref Producer.totalSent);
                Console.WriteLine("Received: " + currentRead + " TotalSent: " +
                      currentSent);
                Console.WriteLine("Rate reading: " + (currentRead - previousRead) + ", Rate sending: " + (currentSent - previousSent));
            }

        }
        static void processOn(string addr, string queue, int totalSend, int maxRateSend, String processName) {
            Address address = new Address(addr);

            Connection connection = new Connection(address);

            ReceiverPool pool = new ReceiverPool(connection, 1, queue, 200, TheCallback);
            pool.start();

            Producer Producer = new Producer(processName, addr, queue, totalSend, maxRateSend);
            Producer.produce(); // this will start an asynchronous producer

        }
    }
}
