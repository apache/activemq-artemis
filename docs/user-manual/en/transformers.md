# Transformers

A transformer, as the name suggests, is a component which transforms a message.
For example, a transformer could modify the body of a message or add or remove
properties. Both [diverts](diverts.md) and [core bridges](core-bridges.md)
support.

A transformer is simply a class which implements the interface
`org.apache.activemq.artemis.core.server.transformer.Transformer`:

```java
public interface Transformer {

   default void init(Map<String, String> properties) { }

   Message transform(Message message);
}
```

The `init` method is called immediately after the broker instantiates the class.
There is a default method implementation so implementing `init` is optional.
However, if the transformer needs any configuration properties it should
implement `init` and the broker will pass the configured key/value pairs to the
transformer using a `java.util.Map`.

## Configuration

The most basic configuration requires only specifying the transformer's class
name, e.g.:

```xml
<transformer-class-name>
   org.foo.MyTransformer
</transformer-class-name>
```

However, if the transformer needs any configuration properties those can be
specified using a slightly different syntax, e.g.:

```xml
<transformer>
   <class-name>org.foo.MyTransformerWithProperties</class-name>
   <property key="transformerKey1" value="transformerValue1"/>
   <property key="transformerKey2" value="transformerValue2"/>
</transformer>
```

Any transformer implementation needs to be added to the broker's classpath. See
the documentation on [adding runtime dependencies](using-server.md#adding-runtime-dependencies)
to understand how to make your transformer available to the broker.
