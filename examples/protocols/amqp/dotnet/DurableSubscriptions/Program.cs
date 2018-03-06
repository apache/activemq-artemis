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
using Amqp;
using Amqp.Framing;
using Amqp.Types;
using Amqp.Sasl;
using System.Threading;

namespace aorg.apache.activemq.examples
{
    class Program
    {
        private static string DEFAULT_BROKER_URI = "amqp://localhost:5672";
        private static string DEFAULT_CONTAINER_ID = "client-1";
        private static string DEFAULT_SUBSCRIPTION_NAME = "test-subscription";
        private static string DEFAULT_TOPIC_NAME = "test-topic";

        static void Main(string[] args)
        {
            Console.WriteLine("Starting AMQP durable consumer example.");

            Console.WriteLine("Creating a Durable Subscription");
            CreateDurableSubscription();

            Console.WriteLine("Attempting to recover a Durable Subscription");
            RecoverDurableSubscription();

            Console.WriteLine("Unsubscribe a durable subscription");
            UnsubscribeDurableSubscription();

            Console.WriteLine("Attempting to recover a non-existent durable subscription");
            try
            {
                RecoverDurableSubscription();
                throw new Exception("Subscription was not deleted.");
            } 
            catch (AmqpException)
            {              
                Console.WriteLine("Recover failed as expected");  
            }

            Console.WriteLine("Example Complete.");
        }

        // Creating a durable subscription involves creating a Receiver with a Source that
        // has the address set to the Topic name where the client wants to subscribe along
        // with an expiry policy of 'never', Terminus Durability set to 'unsettled' and the
        // Distribution Mode set to 'Copy'.  The link name of the Receiver represents the
        // desired name of the Subscription and of course the Connection must carry a container
        // ID uniqure to the client that is creating the subscription.
        private static void CreateDurableSubscription() 
        {
            Connection connection = new Connection(new Address(DEFAULT_BROKER_URI), 
                                                   SaslProfile.Anonymous,
                                                   new Open() { ContainerId = DEFAULT_CONTAINER_ID }, null);

            try 
            {
                Session session = new Session(connection);

                Source source = CreateBasicSource();

                // Create a Durable Consumer Source.
                source.Address = DEFAULT_TOPIC_NAME;
                source.ExpiryPolicy = new Symbol("never");
                source.Durable = 2;
                source.DistributionMode = new Symbol("copy");

                ReceiverLink receiver = new ReceiverLink(session, DEFAULT_SUBSCRIPTION_NAME, source, null);                

                session.Close();
            }
            finally
            {
                connection.Close();
            }
        }

        // Recovering an existing subscription allows the client to ask the remote
        // peer if a subscription with the given name for the current 'Container ID' 
        // exists.  The process involves the client attaching a receiver with a null
        // Source on a link with the desired subscription name as the link name and
        // the broker will then return a Source instance if this current container 
        // has a subscription registered with that subscription (link) name.
        private static void RecoverDurableSubscription()
        {
            Connection connection = new Connection(new Address(DEFAULT_BROKER_URI), 
                                                   SaslProfile.Anonymous,
                                                   new Open() { ContainerId = DEFAULT_CONTAINER_ID }, null);

            try 
            {
                Session session = new Session(connection);
                Source recoveredSource = null;
                ManualResetEvent attached = new ManualResetEvent(false);

                OnAttached onAttached = (link, attach) =>
                {
                    recoveredSource = (Source) attach.Source;
                    attached.Set();
                };

                ReceiverLink receiver = new ReceiverLink(session, DEFAULT_SUBSCRIPTION_NAME, (Source) null, onAttached);                

                attached.WaitOne(10000);
                if (recoveredSource == null)
                {
                    // The remote had no subscription matching what we asked for.
                    throw new AmqpException(new Error());
                }
                else
                {
                    Console.WriteLine("  Receovered subscription for address: " + recoveredSource.Address);
                    Console.WriteLine("  Recovered Source Expiry Policy = " + recoveredSource.ExpiryPolicy);
                    Console.WriteLine("  Recovered Source Durability = " + recoveredSource.Durable);
                    Console.WriteLine("  Recovered Source Distribution Mode = " + recoveredSource.DistributionMode);
                }

                session.Close();
            }
            finally
            {
                connection.Close();
            }
        }

        // Unsubscribing a durable subscription involves recovering an existing 
        // subscription and then closing the receiver link explicitly or in AMQP
        // terms the close value of the Detach frame should be 'true'
        private static void UnsubscribeDurableSubscription() 
        {
            Connection connection = new Connection(new Address(DEFAULT_BROKER_URI), 
                                                   SaslProfile.Anonymous,
                                                   new Open() { ContainerId = DEFAULT_CONTAINER_ID }, null);

            try 
            {
                Session session = new Session(connection);
                Source recoveredSource = null;
                ManualResetEvent attached = new ManualResetEvent(false);

                OnAttached onAttached = (link, attach) =>
                {
                    recoveredSource = (Source) attach.Source;
                    attached.Set();
                };

                ReceiverLink receiver = new ReceiverLink(session, DEFAULT_SUBSCRIPTION_NAME, (Source) null, onAttached);                

                attached.WaitOne(10000);
                if (recoveredSource == null)
                {
                    // The remote had no subscription matching what we asked for.
                    throw new AmqpException(new Error());
                }
                else
                {
                    Console.WriteLine("  Receovered subscription for address: " + recoveredSource.Address);
                    Console.WriteLine("  Recovered Source Expiry Policy = " + recoveredSource.ExpiryPolicy);
                    Console.WriteLine("  Recovered Source Durability = " + recoveredSource.Durable);
                    Console.WriteLine("  Recovered Source Distribution Mode = " + recoveredSource.DistributionMode);
                }

                // Closing the Receiver vs. detaching it will unsubscribe
                receiver.Close();

                session.Close();
            }
            finally
            {
                connection.Close();
            }
        }

        // Creates a basic Source type that contains common attributes needed
        // to describe to the remote peer the features and expectations of the
        // Source of the Receiver link.
        private static Source CreateBasicSource()
        {
            Source source = new Source();

            // These are the outcomes this link will accept.
            Symbol[] outcomes = new Symbol[] {new Symbol("amqp:accepted:list"), 
                                              new Symbol("amqp:rejected:list"),
                                              new Symbol("amqp:released:list"),
                                              new Symbol("amqp:modified:list") };

            // Default Outcome for deliveries not settled on this link
            Modified defaultOutcome = new Modified();
            defaultOutcome.DeliveryFailed = true;
            defaultOutcome.UndeliverableHere = false;

            // Configure Source.
            source.DefaultOutcome = defaultOutcome;
            source.Outcomes = outcomes;            

            return source;
        }
    }
}
