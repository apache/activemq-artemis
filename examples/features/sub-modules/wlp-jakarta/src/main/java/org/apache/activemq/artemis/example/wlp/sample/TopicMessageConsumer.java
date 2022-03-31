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
package org.apache.activemq.artemis.example.wlp.sample;

import jakarta.ejb.MessageDriven;
import jakarta.jms.*;

@MessageDriven(name = "TopicMessageConsumer")
public class TopicMessageConsumer implements MessageListener {

    @Override
    public void onMessage(Message message) {
        System.out.println("TopicMessageConsumer: " + message);
        if (message instanceof BytesMessage) {
            try {
                byte[] byteData = new byte[(int) ((BytesMessage) message).getBodyLength()];
                ((BytesMessage) message).readBytes(byteData);
            } catch (JMSException e) {
                e.printStackTrace();
            }
        } else if (message instanceof TextMessage) {
            try {
                System.out.println(((TextMessage) message).getText());
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }
    }

}
