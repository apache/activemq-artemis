/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */


/*
    Jiira Issue: https://issues.apache.org/jira/browse/ARTEMIS-1542
    Modified example (source was hello.cpp).
    This example produces an exception in ActiveMQ Artemis 2.4.0 in a HA setup.
    Prerequisites: Two brokers are running in a HA cluster.
    This example sends a message to broker1 and tries to read from broker 2.

    compile it using: g++ src/main/cpp/send-test.cpp  -o send-test -l qpidmessaging -l qpidtypes

*/


#include <qpid/messaging/Connection.h>
#include <qpid/messaging/Message.h>
#include <qpid/messaging/Receiver.h>
#include <qpid/messaging/Sender.h>
#include <qpid/messaging/Session.h>

#include <iostream>

using namespace qpid::messaging;

int main(int argc, char** argv) {
    std::string broker = argc > 1 ? argv[1] : "localhost:61616";
    std::string broker2 = argc > 1 ? argv[2] : "localhost:61617";
    std::string address = argc > 2 ? argv[3] : "test.queue";
    std::string exampleQueue = argc > 2 ? argv[3] : "exampleQueue";

    // Connection options documented at http://qpid.apache.org/releases/qpid-0.30/programming/book/connections.html#connection-options
    std::string connectionOptions = argc > 3 ? argv[3] : "{protocol:amqp1.0}";

    try {
         // Step 3. Create an amqp qpid 1.0 connection
        Connection connection(broker, connectionOptions);
        connection.open();

         // Step 4. Create a session
        Session session = connection.createSession();

         // Step 5. Create a sender
        Sender sender = session.createSender(address);

        Connection connection2(broker2, connectionOptions);
        connection2.open();
         // Step 4. Create a session
        Session session2 = connection2.createSession();
        //create a receiver
        Receiver receiver = session2.createReceiver(address);

        Message message;
        message.getContentObject() = "Hello world!";

        message.getContentObject().setEncoding("utf8");
        message.setContentType("text/plain");

        for (int i = 0; i < 10; i++) {
            sender.send(message);
        }

        // receive the simple message
        message = receiver.fetch(Duration::SECOND * 1);
        std::cout << "Received a message with this following content \"" << message.getContent() << "\"" << std::endl;

        // acknowledge the message
        session.acknowledge();


        // Create a sender towards the example, so the java class will give up waiting
        Sender senderExample = session.createSender(exampleQueue);

        for (int i = 0; i < 10; i++) {
           senderExample.send(message);
        }



        // close the connection
        connection.close();
        return 0;
    } catch(const std::exception& error) {
        std::cerr << error.what() << std::endl;
        return 1;
    }
}
