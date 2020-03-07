# JMS OpenSSL Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example shows you how to configure Netty OpenSSL with ActiveMQ Artemis to send and receive message.

Using SSL can make your messaging applications interact with ActiveMQ Artemis securely. An application can be secured transparently without extra coding effort.
Beside using JDK's implementation, Artemis also supports using native OpenSSL provided by Netty.
To secure your messaging application with Netty's OpenSSL, you need to configure connector and acceptor as follows:

    <acceptor name="netty-ssl-acceptor">tcp://localhost:5500?sslEnabled=true;sslProvider=OPENSSL;keyStorePath=activemq.example.keystore;keyStorePassword=secureexample</acceptor>

In the configuration, the `activemq.example.keystore` is the key store file holding the server's certificate. The `activemq.example.truststore` is the file holding the certificates which the client trusts (i.e. the server's certificate exported from activemq.example.keystore). They are generated via the following commands:

* `keytool -genkey -keystore activemq.example.keystore -storepass secureexample -keypass secureexample -dname "CN=localhost, OU=Artemis, O=ActiveMQ, L=AMQ, S=AMQ, C=AMQ" -keyalg EC -sigalg SHA256withECDSA`
* `keytool -export -keystore activemq.example.keystore -file activemq-jks.cer -storepass secureexample`
* `keytool -import -keystore activemq.example.truststore -file activemq-jks.cer -storepass secureexample -keypass secureexample -noprompt`
