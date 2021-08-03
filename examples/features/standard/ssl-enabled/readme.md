# JMS SSL Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example shows you how to configure SSL with ActiveMQ Artemis to send and receive message.

Using SSL can make your messaging applications interact with ActiveMQ Artemis securely. An application can be secured transparently without extra coding effort. To secure your messaging application with SSL, you need to configure connector and acceptor as follows:

    <acceptor name="netty-ssl-acceptor">tcp://localhost:5500?sslEnabled=true;keyStorePath=server-keystore.jks;keyStorePassword=securepass</acceptor>

In the configuration, the `activemq.example.keystore` is the key store file holding the server's certificate. The `activemq.example.truststore` is the file holding the certificates which the client trusts (i.e. the server's certificate exported from activemq.example.keystore). They are generated via the following commands:

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
```
