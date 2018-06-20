/*
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
     /**
     * This class will start many consumers underneath it to satisfy a pool of consumers
     * While calling a single callback for when messages are received.
     */
     public class ReceiverPool
     {
        public delegate void MessageReceived(int id, Session session, ReceiverLink link, Message msg);

        public int MessagesReceived;

        MessageReceived _callback;
        int _Workers;
        private Object receiverLock = new Object();
        private Boolean running = true;
        
        private ReceiverLink[] Receivers;
        private Session[] Sessions;

        private Connection _Connection;

        private int Credits;

        public ReceiverPool(Connection Connection, int Workers, String queue, int Credits, MessageReceived callback)
        {
            this._Connection = Connection;
            this.Receivers = new ReceiverLink[Workers];
            this.Sessions = new Session[Workers];
            

            for (int i = 0; i < Workers; i++)
            {

                // I was playing with using a single session versus multiple sessions
                if (i == 0) {
                    Sessions[i] = new Session(Connection);
                }
                else {
                    Sessions[i] = Sessions[0];
                }
                Receivers[i] = new ReceiverLink(Sessions[i], "receiver " + queue + " " + i, queue);
            }
            this._Workers = Workers;
            this._callback = callback;
            this.Credits = Credits;
        }


        public void stop() {
            running = false;
            for (int i = 0; i < _Workers; i++) {
                Receivers[i].Close();
                Sessions[i].Close();
            }
        }


        public void start() {
            for (int i = 0; i < _Workers; i++) {
                {
                    // This variable exists otherwise we would get an olderValue of i
                    int value = i;
                    Task.Factory.StartNew(() => WorkerRun(value), TaskCreationOptions.LongRunning);
                }
            }
        }

        void WorkerRun(int i) {
            try {
                Receivers[i].SetCredit(Credits);
                while (running)
                {
                    Message theMessage = Receivers[i].Receive(TimeSpan.FromSeconds(1));

                    if (theMessage != null)
                    {
                        _callback(i, Sessions[i], Receivers[i], theMessage);
                    }
                }
            } catch (Exception e) {
                Console.WriteLine(e);
            }
        }
     }

}