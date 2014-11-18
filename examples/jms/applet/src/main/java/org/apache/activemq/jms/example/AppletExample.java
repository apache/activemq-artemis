/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.activemq.jms.example;

import java.awt.Color;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.swing.BorderFactory;
import javax.swing.JApplet;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.SwingConstants;
import javax.swing.SwingUtilities;

import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.api.jms.ActiveMQJMSClient;
import org.apache.activemq.api.jms.JMSFactoryType;
import org.apache.activemq.core.remoting.impl.netty.NettyConnectorFactory;

/**
 * A AppletExample
 *
 * @author <a href="mailto:jmesnil@redaht.com>Jeff Mesnil</a>
 *
 *
 */
public class AppletExample extends JApplet implements ActionListener
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   /**
    *
    */
   private static final long serialVersionUID = -2129589098734805722L;

   private Destination destination;

   private Connection connection;

   private MessageProducer producer;

   private MessageConsumer consumer;

   private JTextArea display;

   private JButton sendButton;

   private Session session;

   @Override
   public void init()
   {
      super.init();

      try
      {
         SwingUtilities.invokeAndWait(new Runnable()
         {
            public void run()
            {
               createGUI();
            }
         });
      }
      catch (Exception e)
      {
         System.err.println("createGUI didn't successfully complete");
      }

      Map<String, Object> params = new HashMap<String, Object>();
      TransportConfiguration connector = new TransportConfiguration(NettyConnectorFactory.class.getName(), params);
      ConnectionFactory cf = (ConnectionFactory)ActiveMQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, connector);
      destination = ActiveMQJMSClient.createTopic("exampleTopic");

      try
      {
         connection = cf.createConnection();
         session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         producer = session.createProducer(destination);
         consumer = session.createConsumer(destination);
         consumer.setMessageListener(new MessageListener()
         {
            public void onMessage(final Message msg)
            {
               try
               {
                  SwingUtilities.invokeAndWait(new Runnable()
                  {
                     public void run()
                     {
                        try
                        {
                           display.setText(display.getText() + "\n" + ((TextMessage)msg).getText());
                        }
                        catch (JMSException e)
                        {
                           e.printStackTrace();
                        }
                     }
                  });
               }
               catch (Exception e)
               {
                  e.printStackTrace();
               }
            }
         });
      }
      catch (JMSException e)
      {
         e.printStackTrace();
      }
   }

   @Override
   public void start()
   {
      super.start();

      try
      {
         connection.start();
      }
      catch (JMSException e)
      {
         e.printStackTrace();
      }
   }

   @Override
   public void stop()
   {
      System.out.println("close connection");
      if (connection != null)
      {
         try
         {
            connection.close();
         }
         catch (JMSException e)
         {
            e.printStackTrace();
         }
      }

      super.stop();

   }

   // Public --------------------------------------------------------

   public static void main(final String[] args) throws Exception
   {

      final AppletExample applet = new AppletExample();
      applet.init();

      JFrame frame = new JFrame("Applet Example");
      frame.getContentPane().add(applet);

      frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
      frame.pack();
      frame.setVisible(true);

      applet.start();
      System.out.println("open up the applet.html file in a browser, press enter to stop the example");
      System.in.read();
      Runtime.getRuntime().addShutdownHook(new Thread()
      {
         @Override
         public void run()
         {
            applet.stop();
         }
      });
   }

   public void actionPerformed(final ActionEvent e)
   {
      try
      {
         producer.send(session.createTextMessage(new Date().toString()));
      }
      catch (JMSException e1)
      {
         e1.printStackTrace();
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void createGUI()
   {
      JPanel contentPane = new JPanel(new GridBagLayout());
      GridBagConstraints c = new GridBagConstraints();
      int numColumns = 3;

      JLabel l1 = new JLabel("Received Messages:", SwingConstants.CENTER);
      c.gridx = 0;
      c.gridy = 0;
      c.anchor = GridBagConstraints.SOUTH;
      c.gridwidth = numColumns;
      contentPane.add(l1, c);

      display = new JTextArea(5, 20);
      JScrollPane scrollPane = new JScrollPane(display);
      display.setEditable(false);
      display.setForeground(Color.gray);
      c.gridy = 1;
      c.gridwidth = numColumns;
      c.anchor = GridBagConstraints.CENTER;
      c.weighty = 1.0;
      c.fill = GridBagConstraints.BOTH;
      contentPane.add(scrollPane, c);

      sendButton = new JButton("Send");
      c.gridy = 2;
      c.gridwidth = 1;
      c.anchor = GridBagConstraints.SOUTH;
      c.weighty = 0.0;
      c.fill = GridBagConstraints.NONE;
      contentPane.add(sendButton, c);

      sendButton.addActionListener(this);

      JButton clearButton = new JButton("Clear");
      c.gridx = 2;
      c.weightx = 0.0;
      c.anchor = GridBagConstraints.SOUTHEAST;
      c.fill = GridBagConstraints.NONE;
      contentPane.add(clearButton, c);

      clearButton.addActionListener(new ActionListener()
      {
         public void actionPerformed(final ActionEvent e)
         {
            display.setText("");
         }
      });

      // Finish setting up the content pane and its border.
      contentPane.setBorder(BorderFactory.createCompoundBorder(BorderFactory.createLineBorder(Color.black),
                                                               BorderFactory.createEmptyBorder(5, 20, 5, 10)));
      setContentPane(contentPane);
   }

   // Inner classes -------------------------------------------------

}
