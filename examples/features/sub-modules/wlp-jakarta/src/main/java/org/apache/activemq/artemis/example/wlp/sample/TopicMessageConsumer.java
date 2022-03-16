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
