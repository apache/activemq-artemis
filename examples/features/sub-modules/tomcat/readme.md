# Apache ActiveMQ Artemis Tomcat Integration JNDI Resources Sample

This is a Sample Tomcat application showing JNDI resource in tomcat context.xml for ConnectionFactory 
and Destination for Apache ActiveMQ Artemis.

The sample context.xml used by the tomcat in this example can be found under:
/src/tomcat7-maven-plugin/resources/context.xml

To run 

start Apache ActiveMQ Artemis on port 61616
 
then

mvn clean install tomcat7:exec-war

then to send message 

http://localhost:8080/tomcat-sample/send

this will cause a "hello world" message to send, and the consumer should consume and output it to sysout.