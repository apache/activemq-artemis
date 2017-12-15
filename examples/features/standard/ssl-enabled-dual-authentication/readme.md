# JMS SSL Dual Authentication Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example shows you how to configure 2-way SSL along with 2 different authentications mechanisms so that SSL and non-SSL clients can send and consume messages to/from ActiveMQ Artemis. The non-SSL authentication mechanism simply uses username and password. The SSL authentication mechanism uses the client's certificate.

To configure 2-way SSL you need to configure the acceptor as follows:

    <acceptor name="netty-ssl-acceptor">tcp://localhost:5500?sslEnabled=true;needClientAuth=true;keyStorePath=server-side-keystore.jks;keyStorePassword=secureexample;trustStorePath=server-side-truststore.jks;trustStorePassword=secureexample</acceptor>

In the server-side URL, the `server-side-keystore.jks` is the key store file holding the server's certificate. The `server-side-truststore.jks` is the file holding the certificates which the broker trusts. Notice also the `sslEnabled` and `needClientAuth` parameters which enable SSL and require clients to present their own certificate respectively.

Here's the URL the client uses to connect over SSL:

    tcp://localhost:5500?sslEnabled=true&trustStorePath=activemq/server0/client-side-truststore.jks&trustStorePassword=secureexample&keyStorePath=activemq/server0/client-side-keystore.jks&keyStorePassword=secureexample

In the client-side URL, the `client-side-keystore.jks` is the key store file holding the client's certificate. The `client-side-truststore.jks` is the file holding the certificates which the client trusts. The `sslEnabled` parameter is present here as well just as it is on the server.

The various keystore files are generated using the following commands:

* `keytool -genkey -keystore server-side-keystore.jks -storepass secureexample -keypass secureexample -dname "CN=ActiveMQ Artemis Server, OU=Artemis, O=ActiveMQ, L=AMQ, S=AMQ, C=AMQ" -keyalg RSA`
* `keytool -export -keystore server-side-keystore.jks -file server-side-cert.cer -storepass secureexample`
* `keytool -import -keystore client-side-truststore.jks -file server-side-cert.cer -storepass secureexample -keypass secureexample -noprompt`
* `keytool -genkey -keystore client-side-keystore.jks -storepass secureexample -keypass secureexample -dname "CN=ActiveMQ Artemis Client, OU=Artemis, O=ActiveMQ, L=AMQ, S=AMQ, C=AMQ" -keyalg RSA`
* `keytool -export -keystore client-side-keystore.jks -file client-side-cert.cer -storepass secureexample`
* `keytool -import -keystore server-side-truststore.jks -file client-side-cert.cer -storepass secureexample -keypass secureexample -noprompt` 
