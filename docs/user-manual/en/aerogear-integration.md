# AeroGear Integration

AeroGears push technology provides support for different push
notification technologies like Google Cloud Messaging, Apple's APNs or
Mozilla's SimplePush. ActiveMQ allows you to configure a Connector
Service that will consume messages from a queue and forward them to an
AeroGear push server and subsequently sent as notifications to mobile
devices.

## Configuring an AeroGear Connector Service

AeroGear Connector services are configured in the connector-services
configuration:

        <connector-service name="aerogear-connector">
        <factory-class>org.apache.activemq.integration.aerogear.AeroGearConnectorServiceFactory</factory-class>
        <param key="endpoint" value="endpoint"/>
        <param key="queue" value="jms.queue.aerogearQueue"/>
        <param key="application-id" value="an applicationid"/>
        <param key="master-secret" value="a mastersecret"/>
        </connector-service>
        <address-setting match="jms.queue.lastValueQueue">
        <last-value-queue>true</last-value-queue>
        </address-setting>
        

Shown are the required params for the connector service and are:

-   `endpoint`. The endpoint or URL of you AeroGear application.

-   `queue`. The name of the queue to consume from.

-   `application-id`. The application id of your mobile application in
    AeroGear.

-   `master-secret`. The secret of your mobile application in AeroGear.

As well as these required paramaters there are the following optional
parameters

-   `ttl`. The time to live for the message once AeroGear receives it.

-   `badge`. The badge the mobile app should use for the notification.

-   `sound`. The sound the mobile app should use for the notification.

-   `filter`. A message filter(selector) to use on the connector.

-   `retry-interval`. If an error occurs on send, how long before we try
    again to connect.

-   `retry-attempts`. How many times we should try to reconnect after an
    error.

-   `variants`. A comma separated list of variants that should get the
    message.

-   `aliases`. A list of aliases that should get the message.

-   `device-types`. A list of device types that should get the messag.

More in depth explanations of the AeroGear related parameters can be
found in the [AeroGear Push docs](http://aerogear.org/push/)

## How to send a message for AeroGear

To send a message intended for AeroGear simply send a JMS Message and
set the appropriate headers, like so

``` java
Message message = session.createMessage();

message.setStringProperty("AEROGEAR_ALERT", "Hello this is a notification from ActiveMQ");

producer.send(message);
```
            

The 'AEROGEAR_ALERT' property will be the alert sent to the mobile
device.

> **Note**
>
> If the message does not contain this property then it will be simply
> ignored and left on the queue

Its also possible to override any of the other AeroGear parameters by
simply setting them on the message, for instance if you wanted to set
ttl of a message you would:

``` java
message.setIntProperty("AEROGEAR_TTL", 1234);
```
            

or if you wanted to set the list of variants you would use:

``` java
message.setStringProperty("AEROGEAR_VARIANTS", "variant1,variant2,variant3");
```            
```            

Again refer to the AeroGear documentation for a more in depth view on
how to use these settings
