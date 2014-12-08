Configuration Reference
=======================

This section is a quick index for looking up configuration. Click on the
element name to go to the specific chapter.

Server Configuration
====================

activemq-configuration.xml
--------------------------

This is the main core server configuration file.

activemq-jms.xml
----------------

This is the configuration file used by the server side JMS service to
load JMS Queues, Topics and Connection Factories.

Using Masked Passwords in Configuration Files
---------------------------------------------

By default all passwords in ActiveMQ server's configuration files are in
plain text form. This usually poses no security issues as those files
should be well protected from unauthorized accessing. However, in some
circumstances a user doesn't want to expose its passwords to more eyes
than necessary.

ActiveMQ can be configured to use 'masked' passwords in its
configuration files. A masked password is an obscure string
representation of a real password. To mask a password a user will use an
'encoder'. The encoder takes in the real password and outputs the masked
version. A user can then replace the real password in the configuration
files with the new masked password. When ActiveMQ loads a masked
password, it uses a suitable 'decoder' to decode it into real password.

ActiveMQ provides a default password encoder and decoder. Optionally
users can use or implement their own encoder and decoder for masking the
passwords.

### Password Masking in Server Configuration File

#### The password masking property

The server configuration file has a property that defines the default
masking behaviors over the entire file scope.

`mask-password`: this boolean type property indicates if a password
should be masked or not. Set it to "true" if you want your passwords
masked. The default value is "false".

#### Specific masking behaviors

##### cluster-password

The nature of the value of cluster-password is subject to the value of
property 'mask-password'. If it is true the cluster-password is masked.

##### Passwords in connectors and acceptors

In the server configuration, Connectors and Acceptors sometimes needs to
specify passwords. For example if a users wants to use an SSL-enabled
NettyAcceptor, it can specify a key-store-password and a
trust-store-password. Because Acceptors and Connectors are pluggable
implementations, each transport will have different password masking
needs.

When a Connector or Acceptor configuration is initialised, ActiveMQ will
add the "mask-password" and "password-codec" values to the Connector or
Acceptors params using the keys `activemq.usemaskedpassword` and
`activemq.passwordcodec` respectively. The Netty and InVM
implementations will use these as needed and any other implementations
will have access to these to use if they so wish.

##### Passwords in Core Bridge configurations

Core Bridges are configured in the server configuration file and so the
masking of its 'password' properties follows the same rules as that of
'cluster-password'.

#### Examples

The following table summarizes the relations among the above-mentioned
properties

  mask-password   cluster-password   acceptor/connector passwords   bridge password
  --------------- ------------------ ------------------------------ -----------------
  absent          plain text         plain text                     plain text
  false           plain text         plain text                     plain text
  true            masked             masked                         masked

Examples

Note: In the following examples if related attributed or properties are
absent, it means they are not specified in the configure file.

example 1

    <cluster-password>bbc</cluster-password>

This indicates the cluster password is a plain text value ("bbc").

example 2

    <mask-password>true</mask-password>
    <cluster-password>80cf731af62c290</cluster-password>

This indicates the cluster password is a masked value and ActiveMQ will
use its built-in decoder to decode it. All other passwords in the
configuration file, Connectors, Acceptors and Bridges, will also use
masked passwords.

### JMS Bridge password masking

The JMS Bridges are configured and deployed as separate beans so they
need separate configuration to control the password masking. A JMS
Bridge has two password parameters in its constructor, SourcePassword
and TargetPassword. It uses the following two optional properties to
control their masking:

`useMaskedPassword` -- If set to "true" the passwords are masked.
Default is false.

`passwordCodec` -- Class name and its parameters for the Decoder used to
decode the masked password. Ignored if `useMaskedPassword` is false. The
format of this property is a full qualified class name optionally
followed by key/value pairs, separated by semi-colons. For example:

\<property name="useMaskedPassword"\>true\</property\>
\<property
name="passwordCodec"\>com.foo.FooDecoder;key=value\</property\>
ActiveMQ will load this property and initialize the class with a
parameter map containing the "key"-\>"value" pair. If `passwordCodec` is
not specified, the built-in decoder is used.

### Masking passwords in ActiveMQ ResourceAdapters and MDB activation configurations

Both ra.xml and MDB activation configuration have a 'password' property
that can be masked. They are controlled by the following two optional
Resource Adapter properties in ra.xml:

`UseMaskedPassword` -- If setting to "true" the passwords are masked.
Default is false.

`PasswordCodec` -- Class name and its parameters for the Decoder used to
decode the masked password. Ignored if UseMaskedPassword is false. The
format of this property is a full qualified class name optionally
followed by key/value pairs. It is the same format as that for JMS
Bridges. Example:

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

With this configuration, both passwords in ra.xml and all of its MDBs
will have to be in masked form.

### Masking passwords in activemq-users.xml

ActiveMQ's built-in security manager uses plain configuration files
where the user passwords are specified in plaintext forms by default. To
mask those parameters the following two properties are needed:

`mask-password` -- If set to "true" all the passwords are masked.
Default is false.

`password-codec` -- Class name and its parameters for the Decoder used
to decode the masked password. Ignored if `mask-password` is false. The
format of this property is a full qualified class name optionally
followed by key/value pairs. It is the same format as that for JMS
Bridges. Example:

    <mask-password>true</mask-password>
    <password-codec>org.apache.activemq.utils.DefaultSensitiveStringCodec;key=hello world</password-codec>

When so configured, the ActiveMQ security manager will initialize a
DefaultSensitiveStringCodec with the parameters "key"-\>"hello world",
then use it to decode all the masked passwords in this configuration
file.

### Choosing a decoder for password masking

As described in the previous sections, all password masking requires a
decoder. A decoder uses an algorithm to convert a masked password into
its original clear text form in order to be used in various security
operations. The algorithm used for decoding must match that for
encoding. Otherwise the decoding may not be successful.

For user's convenience ActiveMQ provides a default built-in Decoder.
However a user can if they so wish implement their own.

#### The built-in Decoder

Whenever no decoder is specified in the configuration file, the built-in
decoder is used. The class name for the built-in decoder is
org.apache.activemq.utils.DefaultSensitiveStringCodec. It has both
encoding and decoding capabilities. It uses java.crypto.Cipher utilities
to encrypt (encode) a plaintext password and decrypt a mask string using
same algorithm. Using this decoder/encoder is pretty straightforward. To
get a mask for a password, just run the following in command line:

    java org.apache.activemq.utils.DefaultSensitiveStringCodec "your plaintext password"

Make sure the classpath is correct. You'll get something like

    Encoded password: 80cf731af62c290

Just copy "80cf731af62c290" and replace your plaintext password with it.

#### Using a different decoder

It is possible to use a different decoder rather than the built-in one.
Simply make sure the decoder is in ActiveMQ's classpath and configure
the server to use it as follows:

    <password-codec>com.foo.SomeDecoder;key1=value1;key2=value2</password-codec>

If your decoder needs params passed to it you can do this via key/value
pairs when configuring. For instance if your decoder needs say a
"key-location" parameter, you can define like so:

    <password-codec>com.foo.NewDecoder;key-location=/some/url/to/keyfile</password-codec>

Then configure your cluster-password like this:

    <mask-password>true</mask-password>
    <cluster-password>masked_password</cluster-password>

When ActiveMQ reads the cluster-password it will initialize the
NewDecoder and use it to decode "mask\_password". It also process all
passwords using the new defined decoder.

#### Implementing your own codecs

To use a different decoder than the built-in one, you either pick one
from existing libraries or you implement it yourself. All decoders must
implement the `org.apache.activemq.utils.SensitiveDataCodec<T>`
interface:

    public interface SensitiveDataCodec<T>
    {
       T decode(Object mask) throws Exception;

       void init(Map<String, String> params);
    }

This is a generic type interface but normally for a password you just
need String type. So a new decoder would be defined like

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

Last but not least, once you get your own decoder, please add it to the
classpath. Otherwise ActiveMQ will fail to load it!
