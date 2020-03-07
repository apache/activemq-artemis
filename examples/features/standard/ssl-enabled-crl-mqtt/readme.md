# ActiveMQ Artemis MQTT CRL Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the server manually.

This example shows you how to configure 2-way SSL with CRL along with 2 different connections, one with a valid certificate and another with a revoked certificate.

To configure 2-way SSL with CRL you need to configure the acceptor as follows:

```
<acceptor name="mqtt">tcp://0.0.0.0:1883?tcpSendBufferSize=1048576;tcpReceiveBufferSize=1048576;protocols=MQTT;useEpoll=true;sslEnabled=true;keyStorePath=${data.dir}/../etc/keystore1.jks;keyStorePassword=changeit;trustStorePath=${data.dir}/../etc/truststore.jks;keyStorePassword=changeit;crlPath=${data.dir}/../etc/root.crl.pem;needClientAuth=true</acceptor>`
```

In the server-side URL, the `keystore1.jks` is the key store file holding the server's key certificate. The `truststore.jks` is the file holding the certificates which the server trusts. The `root.crl.pem` is the file holding the revoked certificates. Notice also the `sslEnabled` and `needClientAuth` parameters which enable SSL and require clients to present their own certificate respectively.

The various keystore files are generated using the following commands. Keep in mind that each common name should be different and the passwords should be `changeit`.

```
openssl genrsa -out ca.key 2048
openssl req -new -x509 -days 1826 -key ca.key -out ca.crt
touch certindex
echo 01 > certserial
echo 01 > crlnumber
```

## Create the ca.conf file:

```
[ ca ]
default_ca = myca

[ crl_ext ]
# issuerAltName=issuer:copy #this would copy the issuer name to altname
authorityKeyIdentifier=keyid:always

[ myca ]
dir = ./
new_certs_dir = $dir
unique_subject = no
certificate = $dir/ca.crt
database = $dir/certindex
private_key = $dir/ca.key
serial = $dir/certserial
default_days = 730
default_md = sha1
policy = myca_policy
x509_extensions = myca_extensions
crlnumber = $dir/crlnumber
default_crl_days = 730

[ myca_policy ]
commonName = supplied
stateOrProvinceName = supplied
countryName = optional
emailAddress = optional
organizationName = supplied
organizationalUnitName = optional

[ myca_extensions ]
basicConstraints = CA:false
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid:always
keyUsage = digitalSignature,keyEncipherment
extendedKeyUsage = serverAuth, clientAuth
crlDistributionPoints = URI:http://example.com/root.crl
subjectAltName = @alt_names

[alt_names]
DNS.1 = example.com
DNS.2 = *.example.com`
```

## Continue with the following commands:

```
openssl genrsa -out keystore1.key 2048
openssl req -new -key keystore1.key -out keystore1.csr
openssl ca -batch -config ca.conf -notext -in keystore1.csr -out keystore1.crt
openssl genrsa -out client_revoked.key 2048
openssl req -new -key client_revoked.key -out client_revoked.csr
openssl ca -batch -config ca.conf -notext -in client_revoked.csr -out client_revoked.crt
openssl genrsa -out client_not_revoked.key 2048
openssl req -new -key client_not_revoked.key -out client_not_revoked.csr
openssl ca -batch -config ca.conf -notext -in client_not_revoked.csr -out client_not_revoked.crt
openssl ca -config ca.conf -gencrl -keyfile ca.key -cert ca.crt -out root.crl.pem
openssl ca -config ca.conf -revoke client_revoked.crt -keyfile ca.key -cert ca.crt
openssl ca -config ca.conf -gencrl -keyfile ca.key -cert ca.crt -out root.crl.pem

openssl pkcs12 -export -name client_revoked -in client_revoked.crt -inkey client_revoked.key -out client_revoked.p12
keytool -importkeystore -destkeystore client_revoked.jks -srckeystore client_revoked.p12 -srcstoretype pkcs12 -alias client_revoked

openssl pkcs12 -export -name client_not_revoked -in client_not_revoked.crt -inkey client_not_revoked.key -out client_not_revoked.p12
keytool -importkeystore -destkeystore client_not_revoked.jks -srckeystore client_not_revoked.p12 -srcstoretype pkcs12 -alias client_not_revoked

openssl pkcs12 -export -name keystore1 -in keystore1.crt -inkey keystore1.key -out keystore1.p12
keytool -importkeystore -destkeystore keystore1.jks -srckeystore keystore1.p12 -srcstoretype pkcs12 -alias keystore1

keytool -import -trustcacerts -alias trust_key -file ca.crt -keystore truststore.jks
```