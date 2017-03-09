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


// Step 2. Make the proper C++ imports
#include <qpid/messaging/Connection.h>
#include <qpid/messaging/Message.h>
#include <qpid/messaging/Receiver.h>
#include <qpid/messaging/Sender.h>
#include <qpid/messaging/Session.h>

#include <iostream>

using namespace qpid::messaging;

int main(int argc, char** argv) {
    std::string broker = argc > 1 ? argv[1] : "localhost:61616";
    std::string address = argc > 2 ? argv[2] : "exampleQueue";

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

        //create a receiver
        Receiver receiver = session.createReceiver(address);


        for (int i = 0; i < 10; i++) {
            Message message;
            message.getContentObject() = "Hello world!";

            message.getContentObject().setEncoding("utf8");
            message.setContentType("text/plain");

            sender.send(message);

            // receive the simple message
            message = receiver.fetch(Duration::SECOND * 1);
            std::cout << "Received a message with this following content \"" << message.getContent() << "\"" << std::endl;

            // acknowledge the message
            session.acknowledge();
        }

        // close the connection
        connection.close();
        return 0;
    } catch(const std::exception& error) {
        std::cerr << error.what() << std::endl;
        return 1;
    }
}
