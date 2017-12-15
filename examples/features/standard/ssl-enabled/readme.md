# JMS SSL Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example shows you how to configure SSL with ActiveMQ Artemis to send and receive message.

Using SSL can make your messaging applications interact with ActiveMQ Artemis securely. An application can be secured transparently without extra coding effort. To secure your messaging application with SSL, you need to configure connector and acceptor as follows:

    <acceptor name="netty-ssl-acceptor">tcp://localhost:5500?sslEnabled=true;keyStorePath=activemq.example.keystore;keyStorePassword=activemqexample</acceptor>

In the configuration, the `activemq.example.keystore` is the key store file holding the server's certificate. The `activemq.example.truststore` is the file holding the certificates which the client trusts (i.e. the server's certificate exported from activemq.example.keystore). They are generated via the following commands:

* `keytool -genkey -keystore activemq.example.keystore -storepass activemqexample -keypass activemqexample -dname "CN=ActiveMQ Artemis Server, OU=Artemis, O=ActiveMQ, L=AMQ, S=AMQ, C=AMQ" -keyalg RSA`
* `keytool -export -keystore activemq.example.keystore -file server-side-cert.cer -storepass activemqexample`
* `keytool -import -keystore activemq.example.truststore -file server-side-cert.cer -storepass activemqexample -keypass activemqexample -noprompt`