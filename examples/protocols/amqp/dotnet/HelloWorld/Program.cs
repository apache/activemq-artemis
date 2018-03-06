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
using Amqp;
namespace ConsoleApplication
{
    public class Program
    {
        public static void Main(string[] args)
        {
            string address = "amqp://a:a@localhost:5672";

            Connection connection = new Connection(new Address(address));
            Session session = new Session(connection);
            SenderLink sender = new SenderLink(session, "test-sender", "q1");

            Message message1 = new Message("Hello AMQP!");
            sender.Send(message1);


            Console.WriteLine("Message sent into queue q1");
        }
    }
}
