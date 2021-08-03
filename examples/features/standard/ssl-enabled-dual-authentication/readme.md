# JMS SSL Dual Authentication Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example shows you how to configure 2-way SSL along with 2 different authentications mechanisms so that SSL and non-SSL clients can send and consume messages to/from ActiveMQ Artemis. The non-SSL authentication mechanism simply uses username and password. The SSL authentication mechanism uses the client's certificate.

To configure 2-way SSL you need to configure the acceptor as follows:

    <acceptor name="netty-ssl-acceptor">tcp://localhost:5500?sslEnabled=true;needClientAuth=true;keyStorePath=server-keystore.jks;keyStorePassword=securepass;trustStorePath=client-ca-truststore.jks;trustStorePassword=securepass</acceptor>

In the server-side URL, the `server-keystore.jks` is the key store file holding the server's certificate. The `client-ca-truststore.jks` is the file holding the certificates which the broker trusts. Notice also the `sslEnabled` and `needClientAuth` parameters which enable SSL and require clients to present their own certificate respectively.

Here's the URL the client uses to connect over SSL:

    tcp://localhost:5500?sslEnabled=true&trustStorePath=server-ca-truststore.jks&trustStorePassword=securepass&keyStorePath=client-keystore.jks&keyStorePassword=securepass

In the client-side URL, the `client-keystore.jks` is the key store file holding the client's certificate. The `server-ca-truststore.jks` is the file holding the certificates which the client trusts. The `sslEnabled` parameter is present here as well just as it is on the server.

The various keystore files are generated using the following commands:

```shell
#!/bin/bash
set -e

KEY_PASS=securepass
STORE_PASS=securepass
CA_VALIDITY=365000
VALIDITY=36500

# Create a key and self-signed certificate for the CA, to sign server certificate requests and use for trust:
# -----------------------------------------------------------------------------------------------------------
keytool -storetype pkcs12 -keystore server-ca-keystore.p12 -storepass $STORE_PASS -keypass $KEY_PASS -alias server-ca -genkey -keyalg "RSA" -keysize 2048 -dname "CN=ActiveMQ Artemis Server Certification Authority, OU=Artemis, O=ActiveMQ" -validity $CA_VALIDITY -ext bc:c=ca:true
keytool -storetype pkcs12 -keystore server-ca-keystore.p12 -storepass $STORE_PASS -alias server-ca -exportcert -rfc > server-ca.crt

# Create trust store with the server CA cert:
# -------------------------------------------
keytool -keystore server-ca-truststore.p12 -storepass $STORE_PASS -keypass $KEY_PASS -importcert -alias server-ca -file server-ca.crt -noprompt

# Create a key pair for the server, and sign it with the CA:
# ----------------------------------------------------------
keytool -keystore server-keystore.jks -storepass $STORE_PASS -keypass $KEY_PASS -alias server -genkey -keyalg "RSA" -keysize 2048 -dname "CN=ActiveMQ Artemis Server, OU=Artemis, O=ActiveMQ, L=AMQ, S=AMQ, C=AMQ" -validity $VALIDITY -ext bc=ca:false -ext eku=sA -ext san=dns:localhost,ip:127.0.0.1

keytool -keystore server-keystore.jks -storepass $STORE_PASS -alias server -certreq -file server.csr
keytool -keystore server-ca-keystore.p12 -storepass $STORE_PASS -alias server-ca -gencert -rfc -infile server.csr -outfile server.crt -validity $VALIDITY -ext bc=ca:false -ext san=dns:localhost,ip:127.0.0.1

keytool -keystore server-keystore.jks -storepass $STORE_PASS -keypass $KEY_PASS -importcert -alias server-ca -file server-ca.crt -noprompt
keytool -keystore server-keystore.jks -storepass $STORE_PASS -keypass $KEY_PASS -importcert -alias server -file server.crt

# Create a key and self-signed certificate for the CA, to sign client certificate requests and use for trust:
# -----------------------------------------------------------------------------------------------------------
keytool -keystore client-ca-keystore.jks -storepass $STORE_PASS -keypass $KEY_PASS -alias client-ca -genkey -keyalg "RSA" -keysize 2048 -dname "CN=ActiveMQ Artemis Client Certification Authority, OU=Artemis, O=ActiveMQ" -validity $CA_VALIDITY -ext bc:c=ca:true
keytool -keystore client-ca-keystore.jks -storepass $STORE_PASS -alias client-ca -exportcert -rfc > client-ca.crt

# Create trust store with the client CA cert:
# -------------------------------------------
keytool -storetype pkcs12 -keystore client-ca-keystore.p12 -storepass $STORE_PASS -keypass $KEY_PASS -alias client-ca -genkey -keyalg "RSA" -keysize 2048 -dname "CN=ActiveMQ Artemis Client Certification Authority, OU=Artemis, O=ActiveMQ" -validity $CA_VALIDITY -ext bc:c=ca:true
keytool -storetype pkcs12 -keystore client-ca-keystore.p12 -storepass $STORE_PASS -alias client-ca -exportcert -rfc > client-ca.crt

# Create a key pair for the client, and sign it with the CA:
# ----------------------------------------------------------
keytool -keystore client-keystore.jks -storepass $STORE_PASS -keypass $KEY_PASS -alias client -genkey -keyalg "RSA" -keysize 2048 -dname "CN=ActiveMQ Artemis Client, OU=Artemis, O=ActiveMQ, L=AMQ, S=AMQ, C=AMQ" -validity $VALIDITY -ext bc=ca:false -ext eku=cA -ext san=dns:localhost,ip:127.0.0.1

keytool -keystore client-keystore.jks -storepass $STORE_PASS -alias client -certreq -file client.csr
keytool -keystore client-ca-keystore.p12 -storepass $STORE_PASS -alias client-ca -gencert -rfc -infile client.csr -outfile client.crt -validity $VALIDITY -ext bc=ca:false -ext eku=cA -ext san=dns:localhost,ip:127.0.0.1

keytool -keystore client-keystore.jks -storepass $STORE_PASS -keypass $KEY_PASS -importcert -alias client-ca -file client-ca.crt -noprompt
keytool -keystore client-keystore.jks -storepass $STORE_PASS -keypass $KEY_PASS -importcert -alias client -file client.crt
```
