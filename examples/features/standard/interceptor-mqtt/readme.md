# MQTT Interceptor Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example shows you how to implement and configure a simple incoming, server-side MQTT interceptor with ActiveMQ Artemis.

ActiveMQ Artemis allows an application to use an interceptor to hook into the messaging system. To intercept MQTT packets all that needs to be done is to implement the `org.apache.activemq.artemis.core.protocol.mqtt.MQTTInterceptor` interface.

Once you have your own interceptor class add it to the broker.xml as follows:

    <core>
       ...
       <remoting-incoming-interceptors>
          <class-name>org.apache.activemq.artemis.mqtt.example.SimpleMQTTInterceptor</class-name>
       </remoting-incoming-interceptors>
        ...
    </core>

With an interceptor you can handle various events in message processing. In this example, a simple interceptor, SimpleMQTTInterceptor, is implemented and configured. When the example is running, the interceptor will modify the payload of a sample MQTT message.

With our interceptor we always return `true` from the `intercept` method. If we were to return `false` that signifies that no more interceptors are to run. Throw an exception to abort processing of the packet.