<h3 class="about-header">About Apache ActiveMQ Artemis</h3>

<div id="content">
    <div class="wrapper">
        <p>Apache ActiveMQ Artemis is an open source project to build a multi-protocol, embeddable, very high performance, clustered, asynchronous messaging system.</p>
        <p>Apache ActiveMQ Artemis has a proven non blocking architecture. It delivers outstanding performance. </p>
        <p>A full guide on features and usage can be found in the <a href="#/help">User Manual</a></p>
        <p/>{{branding.appName}} is powered by <img class='no-shadow' ng-src='img/logo-16px.png'><a href="http://hawt.io/">hawtio</a><p/>
        <h2 id = "Features">Features</h2>
        <ul>
            <li>AMQP protocol support</li>
            <li>OpenWire support for ActiveMQ 5 clients</li>
            <li>MQTT support</li>
            <li>STOMP protocol support</li>
            <li>HornetQ Core protocol support for HornetQ 2.4,2.5 clients</li>
            <li>JMS 2.0 and 1.1 support</li>
            <li>High availability with shared store and non shared store (replication)</li>
            <li>Flexible Clustering</li>
            <li>High performance journal for message persistence</li>
            <li>Queue memory limitation</li>
            <li>SSL support</li>
            <li>Management over JMX, JMS and core protocol</li>
            <li>Large message support</li>
            <li>Topic hierarchies</li>
            <li>Producer flow control</li>
            <li>Consumer flow control</li>
            <li>Diverts</li>
            <li>Last value queue</li>
            <li>Message Groups</li>
            <li>OSGi support</li>
        </ul>
        <h2 id = "Links">Links</h2>
        <ul>
            <li><a target="_blank" href="#/help">User Manual</a></li>
            <li><a href="https://activemq.apache.org/artemis/download.html">Download</a></li>
            <li><a href="https://activemq.apache.org/artemis/migration.html">Migration</a></li>
            <li><a href="https://activemq.apache.org/artemis/community.html">Community</a></li>
        </ul>
    </div>
</div>


<h4>Versions</h4>

  **artemis** version: ${project.version}

  **hawtio** version: {{hawtioVersion}}

  **jolokia** version: {{jolokiaVersion}}

<div ng-show="serverVendor">
  <strong>server</strong> version: {{serverVendor}} {{serverProduct}} {{serverVersion}}
</div>