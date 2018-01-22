# JMS Interceptor Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example shows you how to implement and configure a simple incoming, server-side interceptor with ActiveMQ Artemis.

ActiveMQ Artemis allows an application to use an interceptor to hook into the messaging system. To intercept "core" packets all that needs to be done is to implement the `org.apache.activemq.artemis.api.core.Interceptor` interface.

Once you have your own interceptor class, add it to the broker.xml, as follows:

    <core>
       ...
       <remoting-incoming-interceptors>
          <class-name>org.apache.activemq.artemis.jms.example.SimpleInterceptor</class-name>
       </remoting-incoming-interceptors>
       ...
    </core>

With interceptors, you can handle various events in message processing. In this example, a simple interceptor, SimpleInterceptor, is implemented and configured. When the example is running the interceptor will print out each events that are passed in the interceptor. And it will add a string property to the message being delivered. You can see that after the message is received, there will be a new string property appears in the received message.

With our interceptor we always return `true` from the `intercept` method. If we were to return `false` that signifies that no more interceptors are to run or the target is not to be called. Return `false` to abort processing of the packet.