# Stomp Dual Authentication Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example shows you how to configure 2-way SSL along with 2 different authentications mechanisms so that SSL and non-SSL clients can send and consume messages to/from ActiveMQ Artemis. The non-SSL authentication mechanism simply uses username and password. The SSL authentication mechanism uses the client's certificate. The Stomp client uses SSL socket directly to send a message. Then a JMS client will use a non-SSL connection to consume it.

The various keystore files are generated using the following commands:

* `keytool -genkey -keystore server-side-keystore.jks -storepass secureexample -keypass secureexample -dname "CN=ActiveMQ Artemis Server, OU=Artemis, O=ActiveMQ, L=AMQ, S=AMQ, C=AMQ" -keyalg RSA`
* `keytool -export -keystore server-side-keystore.jks -file server-side-cert.cer -storepass secureexample`
* `keytool -import -keystore client-side-truststore.jks -file server-side-cert.cer -storepass secureexample -keypass secureexample -noprompt`
* `keytool -genkey -keystore client-side-keystore.jks -storepass secureexample -keypass secureexample -dname "CN=ActiveMQ Artemis Client, OU=Artemis, O=ActiveMQ, L=AMQ, S=AMQ, C=AMQ" -keyalg RSA`
* `keytool -export -keystore client-side-keystore.jks -file client-side-cert.cer -storepass secureexample`
* `keytool -import -keystore server-side-truststore.jks -file client-side-cert.cer -storepass secureexample -keypass secureexample -noprompt`