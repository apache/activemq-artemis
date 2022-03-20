package org.apache.activemq.artemis.example.wlp.sample;

import javax.annotation.Resource;
import javax.ejb.MessageDriven;
import javax.ejb.MessageDrivenContext;
import javax.jms.*;

@MessageDriven(name = "QueueMessageConsumer")
public class QueueMessageConsumer implements MessageListener {

    @Resource
    private MessageDrivenContext mdcContext;

    @Resource(name = "jms/sampleConnectionFactory")
    private ConnectionFactory connectionFactory;

    @Resource(name = "jms/sampleTopic")
    private Topic topic;

    @Override
    public void onMessage(Message message) {
        System.out.println("QueueMessageConsumer: " + message);
        Connection con = null;
        try {
            con = connectionFactory.createConnection();
            Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(topic);
            producer.send(message);
            con.close();
        } catch (Exception e) {
            e.printStackTrace();
            mdcContext.setRollbackOnly();
        } finally {
            if (con != null) {
                try {
                    con.close();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
