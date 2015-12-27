# Artemis on Apache Karaf

Apache ActiveMQ Artemis can be installed on Apache Karaf using the following commands from the Karaf shell:

    feature:repo-add mvn:org.apache.activemq/artemis-features/1.2.1-SNAPSHOT/xml
    feature:install artemis-core

And the various protocols can be installed using:

    feature:install artemis-hornetq
    feature:install artemis-stomp
    feature:install artemis-mqtt
    feature:install artemis-amqp
