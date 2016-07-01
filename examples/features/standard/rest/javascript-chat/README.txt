System Requirements:
You will need JDK 1.8 and Maven to run this example.  This example has been tested with Maven 3.3.3.  It may or may not work
with earlier or later versions of Maven.


This is an example of producing and consuming messages through a topic.  The client is Javascript code within your browser.
The example is a very simple chat application between two browser windows.

Step 1:
$ mvn jetty:run

This will bring up ActiveMQ Artemis and the ActiveMQ Artemis REST Interface.

Step 2:
Bring up two browsers and point them to http://localhost:8080.  In the textbox type a message you want to send.  Click
the "Click to send message" button and you'll see the message show up in both browser windows.