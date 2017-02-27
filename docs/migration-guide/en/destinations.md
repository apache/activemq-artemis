Destinations
=====================================

We already talked about addressing differences between ActiveMQ and Artemis in the [introduction](README.md). Now let's dig into the details and see how to configure JMS queues and topics. It's important to note here that both brokers are configured by default to *auto-create* destinations requested by clients, which is preferred behavior for many use cases. This is configured using authorization security policies, so we will cover this topic in the later sections of this manual. For now, let's see how you can predefine JMS queues and topics in both brokers.
 
In ActiveMQ, destinations are pre-defined in the `<destinations>` section of the `conf/activemq.xml` configuration file.

```xml
<destinations>
     <queue physicalName="my-queue" />
     <topic physicalName="my-topic" />
</destinations>	
```

Things looks a bit different in Artemis. We already explained that queues are `anycast` addresses and topics are `muticast` ones. We're not gonna go deep into the address settings details here and you're advised to look at the user manual for that. Let's just see what we need to do in order to replicate ActiveMQ configuration. 

Addresses are defined in `<addresses>` section of the `etc/broker.xml` configuration file. So the corresponding Artemis configuration for the ActiveMQ example above, looks like this:

```xml
<addresses>    
    <address name="my-queue">
        <anycast>
            <queue name="my-queue"/>
        </anycast>
    </address>

    <address name="my-topic">
        <multicast></multicast>
    </address>
</adresses>
```
    
After this step we have our destinations ready in the new broker.
