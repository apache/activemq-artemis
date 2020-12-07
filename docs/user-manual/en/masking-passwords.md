# Masking Passwords

By default all passwords in Apache ActiveMQ Artemis server's configuration
files are in plain text form. This usually poses no security issues as those
files should be well protected from unauthorized accessing. However, in some
circumstances a user doesn't want to expose its passwords to more eyes than
necessary.

Apache ActiveMQ Artemis can be configured to use 'masked' passwords in its
configuration files. A masked password is an obscure string representation of a
real password. To mask a password a user will use an 'codec'. The codec
takes in the real password and outputs the masked version. A user can then
replace the real password in the configuration files with the new masked
password. When Apache ActiveMQ Artemis loads a masked password it uses the
codec to decode it back into the real password.

Apache ActiveMQ Artemis provides a default codec. Optionally users can use
or implement their own codec for masking the passwords.

In general, a masked password can be identified using one of two ways. The
first one is the `ENC()` syntax, i.e. any string value wrapped in `ENC()` is to
be treated as a masked password. For example

`ENC(xyz)`

The above indicates that the password is masked and the masked value is `xyz`.

The `ENC()` syntax is the **preferred way** of masking a password and is
universally supported in every password configuration in Artemis.

The other, legacy way is to use a `mask-password` attribute to tell that a 
password in a configuration file should be treated as 'masked'. For example:

```xml
<mask-password>true</mask-password>
<cluster-password>xyz</cluster-password>
```

This method is now **deprecated** and exists only to maintain
backward-compatibility.  Newer configurations may not support it.

## Generating a Masked Password

To get a mask for a password using the broker's default codec run the
`mask` command from your Artemis *instance*. This command will not work
from the Artemis home:

```sh
./artemis mask <plaintextPassword>
```

You'll get something like

```
result: 32c6f67dae6cd61b0a7ad1702033aa81e6b2a760123f4360
```

Just copy `32c6f67dae6cd61b0a7ad1702033aa81e6b2a760123f4360` and replace your
plaintext password with it using the `ENC()` syntax, e.g. 
`ENC(32c6f67dae6cd61b0a7ad1702033aa81e6b2a760123f4360)`.

This process works for passwords in:

 - `broker.xml`
 - `login.config`
 - `bootstrap.xml`
 - `management.xml`

This process does **not** work for passwords in:

 - `artemis-users.properties`

Masked passwords for `artemis-users.properties` *can* be generated using the
`mask` command using the `--hash` command-line option. However, this is also
possible using the set of tools provided by the `user` command described below.

## Masking Configuration

Besides supporting the `ENC()` syntax, the server configuration file (i.e.
broker.xml) has a property that defines the default masking behaviors over the
entire file scope.

`mask-password`: this boolean type property indicates if a password should be
masked or not. Set it to "true" if you want your passwords masked. The default
value is "false". As noted above, this configuration parameter is deprecated.

`password-codec`: this string type property identifies the name of the class
which will be used to decode the masked password within the broker. If not
specified then the default
`org.apache.activemq.artemis.utils.DefaultSensitiveStringCodec` will be used.

### artemis-users.properties

Apache ActiveMQ Artemis's built-in security manager uses plain properties files
where the user passwords are specified in a hashed form by default. Note, the
passwords are technically *hashed* rather than masked in this context. The
default `PropertiesLoginModule` will not decode the passwords in
`artemis-users.properties` but will instead hash the input and compare the two
hashed values for password verification.

Use the following command from the CLI of the Artemis *instance* you wish to
add the user/password to. This command will not work from the Artemis home
used to create the instance, and it will also not work unless the broker has
been started. For example:

```sh
./artemis user add --user-command-user guest --user-command-password guest --role admin
```

This will use the default codec to perform a "one-way" hash of the password
and alter both the `artemis-users.properties` and `artemis-roles.properties`
files with the specified values.

Passwords in `artemis-users.properties` are automatically detected as hashed or
not by looking for the syntax `ENC(<hash>)`. The `mask-password` parameter does
not need to be `true` to use hashed passwords here.

> **Warning**
>
> Management and CLI operations to manipulate user & role data are only available
> when using the `PropertiesLoginModule`.
>
> In general, using properties files and broker-centric user management for
> anything other than very basic use-cases is not recommended. The broker is
> designed to deal with messages. It's not in the business of managing users,
> although that functionality is provided at a limited level for convenience. LDAP
> is recommended for enterprise level production use-cases.

### cluster-password

If it is specified in `ENC()` syntax it will be treated as masked, or
if `mask-password` is `true` the `cluster-password` will be treated as masked.

### Connectors & Acceptors

In broker.xml `connector` and `acceptor` configurations sometimes needs to
specify passwords. For example, if a user wants to use an `acceptor` with
`sslEnabled=true` it can specify `keyStorePassword` and `trustStorePassword`.
Because Acceptors and Connectors are pluggable implementations, each transport
will have different password masking needs.

The preferred way is simply to use the `ENC()` syntax.

If using the legacy `mask-password` and `password-codec` values then when a
`connector` or `acceptor` is initialised, Apache ActiveMQ Artemis will add
these values to the parameters using the keys `activemq.usemaskedpassword`
and `activemq.passwordcodec` respectively. The Netty and InVM implementations
will use these as needed and any other implementations will have access to
these to use if they so wish.

### Core Bridges

Core Bridges are configured in the server configuration file and so the masking
of its `password` properties follows the same rules as that of
`cluster-password`. It supports `ENC()` syntax.

For using `mask-password` property, the following table summarizes the
relations among the above-mentioned properties

mask-password | cluster-password | acceptor/connector passwords | bridge password
--- | --- | --- | ---
absent|plain text|plain text|plain text
false|plain text|plain text|plain text
true|masked|masked|masked

It is recommended that you use the `ENC()` syntax for new applications/deployments.

#### Examples

**Note:** In the following examples if related attributed or properties are
absent, it means they are not specified in the configure file.

- Unmasked

  ```xml
  <cluster-password>bbc</cluster-password>
  ```

  This indicates the cluster password is a plain text value `bbc`.

- Masked 1

  ```xml
  <cluster-password>ENC(80cf731af62c290)</cluster-password>
  ```

  This indicates the cluster password is a masked value `80cf731af62c290`.

- Masked 2

  ```xml
  <mask-password>true</mask-password>
  <cluster-password>80cf731af62c290</cluster-password>
  ```

  This indicates the cluster password is a masked value and Apache ActiveMQ
  Artemis will use its built-in codec to decode it. All other passwords in the
  configuration file, Connectors, Acceptors and Bridges, will also use masked
  passwords.

### bootstrap.xml

The broker embeds a web-server for hosting some web applications such as a
management console. It is configured in `bootstrap.xml` as a web component. The
web server can be secured using the `https` protocol, and it can be configured 
with a keystore password and/or truststore password which by default are 
specified in plain text forms.

To mask these passwords you need to use `ENC()` syntax. The `mask-password`
boolean is not supported here.

You can also set the `passwordCodec` attribute if you want to use a password
codec other than the default one. For example

```xml
<web bind="https://localhost:8443" path="web" 
     keyStorePassword="ENC(-5a2376c61c668aaf)"
     trustStorePassword="ENC(3d617352d12839eb71208edf41d66b34)">
    <app url="activemq-branding" war="activemq-branding.war"/>
</web>
```

### management.xml

The broker embeds a JMX connector which is used for management. The connector can
be secured using SSL and it can be configured with a keystore password and/or
truststore password which by default are specified in plain text forms.

To mask these passwords you need to use `ENC()` syntax. The `mask-password`
boolean is not supported here.

You can also set the `password-codec` attribute if you want to use a password
codec other than the default one. For example

```xml
<connector
      connector-port="1099"
      connector-host="localhost"
      secured="true"
      key-store-path="myKeystore.jks"
      key-store-password="ENC(3a34fd21b82bf2a822fa49a8d8fa115d"
      trust-store-path="myTruststore.jks"
      trust-store-password="ENC(3a34fd21b82bf2a822fa49a8d8fa115d)"/>
```

With this configuration, both passwords in ra.xml and all of its MDBs will have
to be in masked form.

### login.config

Artemis supports LDAP login modules to be configured in JAAS configuration file
(default name is `login.config`). When connecting to a LDAP server usually you
need to supply a connection password in the config file. By default this
password is in plain text form.

To mask it you need to configure the passwords in your login module using
`ENC()` syntax. To specify a codec using the following property:

`passwordCodec` - the password codec class name. (the default codec will be
used if it is absent)

For example:

```
LDAPLoginExternalPasswordCodec {
    org.apache.activemq.artemis.spi.core.security.jaas.LDAPLoginModule required
        debug=true
        initialContextFactory=com.sun.jndi.ldap.LdapCtxFactory
        connectionURL="ldap://localhost:1024"
        connectionUsername="uid=admin,ou=system"
        connectionPassword="ENC(-170b9ef34d79ed12)"
        passwordCodec="org.apache.activemq.artemis.utils.DefaultSensitiveStringCodec;key=helloworld"
        connectionProtocol=s
        authentication=simple
        userBase="ou=system"
        userSearchMatching="(uid={0})"
        userSearchSubtree=false
        roleBase="ou=system"
        roleName=dummyRoleName
        roleSearchMatching="(uid={1})"
        roleSearchSubtree=false
        ;
};
```

### JCA Resource Adapter

Both ra.xml and MDB activation configuration have a `password` property that
can be masked preferably using `ENC()` syntax.

Alternatively it can use an optional attribute in ra.xml to indicate that a
password is masked:

`UseMaskedPassword` -- If setting to "true" the passwords are masked.  Default
is false.

There is another property in ra.xml that can specify a codec:

`PasswordCodec` -- Class name and its parameters for the codec used to decode
the masked password. Ignored if UseMaskedPassword is false. The format of this
property is a full qualified class name optionally followed by key/value pairs.
It is the same format as that for JMS Bridges. Example:

Example 1 Using the `ENC()` syntax:

```xml
<config-property>
  <config-property-name>password</config-property-name>
  <config-property-type>String</config-property-type>
  <config-property-value>ENC(80cf731af62c290)</config-property-value>
</config-property>
<config-property>
  <config-property-name>PasswordCodec</config-property-name>
  <config-property-type>java.lang.String</config-property-type>
  <config-property-value>com.foo.ACodec;key=helloworld</config-property-value>
</config-property>
```

Example 2 Using the "UseMaskedPassword" property:

```xml
<config-property>
  <config-property-name>UseMaskedPassword</config-property-name>
  <config-property-type>boolean</config-property-type>
  <config-property-value>true</config-property-value>
</config-property>
<config-property>
  <config-property-name>password</config-property-name>
  <config-property-type>String</config-property-type>
  <config-property-value>80cf731af62c290</config-property-value>
</config-property>
<config-property>
  <config-property-name>PasswordCodec</config-property-name>
  <config-property-type>java.lang.String</config-property-type>
  <config-property-value>com.foo.ACodec;key=helloworld</config-property-value>
</config-property>
```

## Choosing a codec for password masking

As described in the previous sections, all password masking requires a codec.
A codec uses an algorithm to convert a masked password into its original
clear text form in order to be used in various security operations. The
algorithm used for decoding must match that for encoding. Otherwise the
decoding may not be successful.

For user's convenience Apache ActiveMQ Artemis provides a default codec.
However a user can implement their own if they wish.

### The Default Codec

Whenever no codec is specified in the configuration, the default codec
is used. The class name for the default codec is
`org.apache.activemq.artemis.utils.DefaultSensitiveStringCodec`. It has
hashing, encoding, and decoding capabilities. It uses `java.crypto.Cipher`
utilities to hash or encode a plaintext password and also to decode a masked
string using the same algorithm and key.

### Using a custom codec

It is possible to use a custom codec rather than the built-in one.  Simply
make sure the codec is in Apache ActiveMQ Artemis's classpath. The custom
codec can also be service loaded rather than class loaded, if the codec's
service provider is installed in the classpath.  Then configure the server to
use it as follows:

```xml
<password-codec>com.foo.SomeCodec;key1=value1;key2=value2</password-codec>
```

If your codec needs params passed to it you can do this via key/value pairs
when configuring. For instance if your codec needs say a "key-location"
parameter, you can define like so:

```xml
<password-codec>com.foo.NewCodec;key-location=/some/url/to/keyfile</password-codec>
```

Then configure your cluster-password like this:

```xml
<cluster-password>ENC(masked_password)</cluster-password>
```

When Apache ActiveMQ Artemis reads the cluster-password it will initialize the
`NewCodec` and use it to decode "mask\_password". It also process all passwords
using the new defined codec.

#### Implementing Custom Codecs

To use a different codec than the built-in one, you either pick one from
existing libraries or you implement it yourself. All codecs must implement
the `org.apache.activemq.artemis.utils.SensitiveDataCodec<T>` interface:

```java
public interface SensitiveDataCodec<T> {

   T decode(Object mask) throws Exception;

   T encode(Object secret) throws Exception;

   default void init(Map<String, String> params) throws Exception {
   };
}
```

This is a generic type interface but normally for a password you just need
String type. So a new codec would be defined like

```java
public class MyCodec implements SensitiveDataCodec<String> {
   @Override
   public String decode(Object mask) throws Exception {
      // Decode the mask into clear text password.
      return "the password";
   }
   
   @Override
   public String encode(Object secret) throws Exception {
      // Mask the clear text password.
      return "the masked password"";
   }

   @Override
   public void init(Map<String, String> params) {
      // Initialization done here. It is called right after the codec has been created.
   }
}
```

Last but not least, once you get your own codec please [add it to the
classpath](using-server.md#adding-runtime-dependencies) otherwise the broker
will fail to load it!
