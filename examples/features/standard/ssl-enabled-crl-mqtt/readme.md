# ActiveMQ Artemis MQTT CRL Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the server manually.

This example shows you how to configure 2-way SSL with CRL along with 2 different connections, one with a valid certificate and another with a revoked certificate.

To configure 2-way SSL with CRL you need to configure the acceptor as follows:

```
<acceptor name="mqtt">tcp://0.0.0.0:1883?protocols=MQTT;sslEnabled=true;keyStorePath=server-keystore.jks;keyStorePassword=securepass;trustStorePath=client-ca-truststore.jks;keyStorePassword=securepass;crlPath=other-client-crl.pem;needClientAuth=true</acceptor>
```

In the server-side URL, the `server-keystore.jks` is the key store file holding the server's key certificate. The `client-ca-truststore.jks` is the file holding the certificates which the server trusts. The `other-client-crl.pem` is the file holding the revoked certificates. Notice also the `sslEnabled` and `needClientAuth` parameters which enable SSL and require clients to present their own certificate respectively.

The various keystore files are generated using the following commands. Keep in mind that each common name should be different and the passwords should be `securepass`.


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

# Create a key pair for the other client, and sign it with the CA:
# ----------------------------------------------------------------
keytool -keystore other-client-keystore.jks -storepass $STORE_PASS -keypass $KEY_PASS -alias other-client -genkey -keyalg "RSA" -keysize 2048 -dname "CN=ActiveMQ Artemis Other Client, OU=Artemis, O=ActiveMQ, L=AMQ, S=AMQ, C=AMQ" -validity $VALIDITY -ext bc=ca:false -ext eku=cA -ext san=dns:localhost,ip:127.0.0.1

keytool -keystore other-client-keystore.jks -storepass $STORE_PASS -alias other-client -certreq -file other-client.csr
keytool -keystore client-ca-keystore.p12 -storepass $STORE_PASS -alias client-ca -gencert -rfc -infile other-client.csr -outfile other-client.crt -validity $VALIDITY -ext bc=ca:false -ext eku=cA -ext san=dns:localhost,ip:127.0.0.1

keytool -keystore other-client-keystore.jks -storepass $STORE_PASS -keypass $KEY_PASS -importcert -alias client-ca -file client-ca.crt -noprompt
keytool -keystore other-client-keystore.jks -storepass $STORE_PASS -keypass $KEY_PASS -importcert -alias other-client -file other-client.crt
```
## Create the ca.conf file:

```
[ ca ]
default_ca      = CA_default

[ CA_default ]
dir             = ./
database        = $dir/openssl-database
crlnumber       = $dir/openssl-crlnumber
default_md      = default
```

## Continue with the following commands:

```shell
# Export the key of the server CA:
# ----------------------------------------------------------------------------------------------------
openssl pkcs12 -in client-ca-keystore.p12 -nodes -nocerts -out client-ca.pem -password pass:$STORE_PASS

# Create crl with the other client cert:
# -------------------------------------------------------
> openssl-database
echo 00 > openssl-crlnumber
openssl ca -config openssl.conf -revoke other-client.crt -keyfile client-ca.pem -cert client-ca.crt
openssl ca -config openssl.conf -gencrl -keyfile client-ca.pem -cert client-ca.crt -out other-client-crl.pem -crldays $VALIDITY
```