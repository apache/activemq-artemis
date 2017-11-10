# Masking Passwords

By default all passwords in Apache ActiveMQ Artemis server's configuration files are in
plain text form. This usually poses no security issues as those files
should be well protected from unauthorized accessing. However, in some
circumstances a user doesn't want to expose its passwords to more eyes
than necessary.

Apache ActiveMQ Artemis can be configured to use 'masked' passwords in its
configuration files. A masked password is an obscure string
representation of a real password. To mask a password a user will use an
'encoder'. The encoder takes in the real password and outputs the masked
version. A user can then replace the real password in the configuration
files with the new masked password. When Apache ActiveMQ Artemis loads a masked
password, it uses a suitable 'decoder' to decode it into real password.

Apache ActiveMQ Artemis provides a default password encoder and decoder. Optionally
users can use or implement their own encoder and decoder for masking the
passwords.

### Password Masking in Server Configuration File

#### General Masking Configuration

The server configuration file (i.e. broker.xml )has a property that defines the
default masking behaviors over the entire file scope.

`mask-password`: this boolean type property indicates if a password
should be masked or not. Set it to "true" if you want your passwords
masked. The default value is "false".

`password-codec`: this string type property identifies the name of the class
which will be used to decode the masked password within the broker. If not
specified then the default `org.apache.activemq.artemis.utils.DefaultSensitiveStringCodec`
will be used.

#### Specific Masking Behaviors

##### cluster-password

If `mask-password` is `true` the `cluster-password` will be treated as masked.

##### Passwords in connectors and acceptors

In broker.xml `connector` and `acceptor` configurations sometimes needs to
specify passwords. For example, if a user wants to use an `acceptor` with
`sslEnabled=true` it can specify `keyStorePassword` and `trustStorePassword`.
Because Acceptors and Connectors are pluggable implementations, each transport
will have different password masking needs.

When a `connector` or `acceptor` is initialised, Apache ActiveMQ Artemis will
add the aforementioned `mask-password` and `password-codec` values to the
`connector` or `acceptor` parameters using the keys `activemq.usemaskedpassword`
and `activemq.passwordcodec` respectively. The Netty and InVM implementations
will use these as needed and any other implementations will have access to
these to use if they so wish.

##### Passwords in bridge configurations

Core Bridges are configured in the server configuration file and so the
masking of its `password` properties follows the same rules as that of
`cluster-password`.

#### Examples

The following table summarizes the relations among the above-mentioned
properties

  mask-password  | cluster-password  | acceptor/connector passwords |  bridge password
  :------------- | :---------------- | :--------------------------- | :---------------
  absent   |       plain text     |    plain text       |              plain text
  false    |       plain text     |    plain text       |              plain text
  true     |       masked         |    masked           |              masked

Examples

Note: In the following examples if related attributed or properties are
absent, it means they are not specified in the configure file.

example 1

```xml
<cluster-password>bbc</cluster-password>
```

This indicates the cluster password is a plain text value ("bbc").

example 2

```xml
<mask-password>true</mask-password>
<cluster-password>80cf731af62c290</cluster-password>
```

This indicates the cluster password is a masked value and Apache ActiveMQ Artemis will
use its built-in decoder to decode it. All other passwords in the
configuration file, Connectors, Acceptors and Bridges, will also use
masked passwords.

### Masking passwords in ActiveMQ Artemis JCA ResourceAdapter and MDB activation configurations

Both ra.xml and MDB activation configuration have a `password` property
that can be masked. They are controlled by the following two optional
Resource Adapter properties in ra.xml:

`UseMaskedPassword` -- If setting to "true" the passwords are masked.
Default is false.

`PasswordCodec` -- Class name and its parameters for the Decoder used to
decode the masked password. Ignored if UseMaskedPassword is false. The
format of this property is a full qualified class name optionally
followed by key/value pairs. It is the same format as that for JMS
Bridges. Example:

```xml
<config-property>
  <config-property-name>UseMaskedPassword</config-property-name>
  <config-property-type>boolean</config-property-type>
  <config-property-value>true</config-property-value>
</config-property>
<config-property>
  <config-property-name>PasswordCodec</config-property-name>
  <config-property-type>java.lang.String</config-property-type>
  <config-property-value>com.foo.ADecoder;key=helloworld</config-property-value>
</config-property>
```

With this configuration, both passwords in ra.xml and all of its MDBs
will have to be in masked form.

### Masking passwords in artemis-users.properties

Apache ActiveMQ Artemis's built-in security manager uses plain properties files
where the user passwords are specified in a hashed form by default. Note, the passwords
are technically *hashed* rather than masked in this context. The default `PropertiesLoginModule`
will not decode the passwords in `artemis-users.properties` but will instead hash the input
and compare the two hashed values for password verification.

Please use Artemis CLI command to add a password. For example:

```sh
    ./artemis user add --username guest --password guest --role admin
```

This will use the default `org.apache.activemq.artemis.utils.DefaultSensitiveStringCodec`
to perform a "one-way" hash of the password and alter both the `artemis-users.properties`
and `artemis-roles.properties` files with the specified values.

Passwords in `artemis-users.properties` are automatically detected as hashed or not
by looking for the syntax `ENC(<hash>)`. The `mask-password` parameter does not need
to be `true` to use hashed passwords here.

### Choosing a decoder for password masking

As described in the previous sections, all password masking requires a
decoder. A decoder uses an algorithm to convert a masked password into
its original clear text form in order to be used in various security
operations. The algorithm used for decoding must match that for
encoding. Otherwise the decoding may not be successful.

For user's convenience Apache ActiveMQ Artemis provides a default decoder.
However a user can implement their own if they wish.

#### The Default Decoder

Whenever no decoder is specified in the configuration file, the default
decoder is used. The class name for the default decoder is
`org.apache.activemq.artemis.utils.DefaultSensitiveStringCodec`. It has hashing,
encoding, and decoding capabilities. It uses `java.crypto.Cipher` utilities
to hash or encode a plaintext password and also to decode a masked string using
same algorithm and key. Using this decoder/encoder is pretty straightforward. To
get a mask for a password, just run the `mask` command:

```sh
./artemis mask <plaintextPassword>
```

You'll get something like

```
result: 32c6f67dae6cd61b0a7ad1702033aa81e6b2a760123f4360
```

Just copy `32c6f67dae6cd61b0a7ad1702033aa81e6b2a760123f4360` and replace your
plaintext password in broker.xml with it.

#### Using a custom decoder

It is possible to use a custom decoder rather than the built-in one.
Simply make sure the decoder is in Apache ActiveMQ Artemis's classpath. The custom decoder
can also be service loaded rather than class loaded, if the decoder's service provider is installed in the classpath.
Then configure the server to use it as follows:

```xml
    <password-codec>com.foo.SomeDecoder;key1=value1;key2=value2</password-codec>
```

If your decoder needs params passed to it you can do this via key/value
pairs when configuring. For instance if your decoder needs say a
"key-location" parameter, you can define like so:

```xml
    <password-codec>com.foo.NewDecoder;key-location=/some/url/to/keyfile</password-codec>
```

Then configure your cluster-password like this:

```xml
    <mask-password>true</mask-password>
    <cluster-password>masked_password</cluster-password>
```

When Apache ActiveMQ Artemis reads the cluster-password it will initialize the
NewDecoder and use it to decode "mask\_password". It also process all
passwords using the new defined decoder.

#### Implementing your own codecs

To use a different decoder than the built-in one, you either pick one
from existing libraries or you implement it yourself. All decoders must
implement the `org.apache.activemq.artemis.utils.SensitiveDataCodec<T>`
interface:

``` java
public interface SensitiveDataCodec<T>
{
   T decode(Object mask) throws Exception;

   void init(Map<String, String> params);
}
```

This is a generic type interface but normally for a password you just
need String type. So a new decoder would be defined like

```java
public class MyNewDecoder implements SensitiveDataCodec<String>
{
   public String decode(Object mask) throws Exception
   {
      //decode the mask into clear text password
      return "the password";
   }

   public void init(Map<String, String> params)
   {
      //initialization done here. It is called right after the decoder has been created.
   }
}
```

Last but not least, once you get your own decoder, please add it to the
classpath by packaging it in a JAR file and putting the JAR file in the `lib`
directory. Otherwise Apache ActiveMQ Artemis will fail to load it!