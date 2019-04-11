# Prometheus Support

Apache ActiveMQ Artemis supports exporting metrics for consumption by
[Prometheus](https://prometheus.io/). Prometheus

## Configuration

Prometheus support is provided by 2 main components:

- A servlet - This servlet is configured in `bootstrap.xml` as part of the
  embedded Jetty HTTP server. By default it is enabled and is available at
  "metrics" (e.g. http://localhost:8161/metrics). See relevant configuration
  options in the [Web Server](web-server.md) chapter.

- JMX exporter -  This is based on the
  [Prometheus JMX Exporter](https://github.com/prometheus/jmx_exporter) and has
  very similar configuration, but instead of running as a Java Agent it runs
  within the broker and is configured via `broker.xml`. By default, **no
  configuration of the JMX exporter is required to export metrics for the
  broker, addresses, and queues**. See more about the default configuration
  below.

> **Note:**
>
> Since the exporter works on *JMX* metrics the `<jmx-management-enabled>`
> element must be set to `true`. To be clear, this is the default value.

### JMX Exporter Configuration

Here's an example configuration of the Prometheus JMS exporter from
`broker.xml`:

```xml
<prometheus-jmx-exporter>
   <lowercase-output-name>true</lowercase-output-name>
   <lowercase-output-label-names>true</lowercase-output-label-names>
   <whitelist>
      <object-name>org.apache.activemq.artemis:*</object-name>
   </whitelist>
   <blacklist>
      <object-name>java.nio:*</object-name>
   </blacklist>
   <rules>
      <rule>
         <pattern>pattern1</pattern>
         <name>myMetric</name>
         <attr-name-snake-case>true</attr-name-snake-case>
         <type>COUNTER</type>
         <help>ruleHelp</help>
         <value>ruleValue</value>
         <value-factor>2.0</value-factor>
         <labels>
            <label name="labelName" value="labelValue"/>
         </labels>
      </rule>
   </rules>
</prometheus-jmx-exporter>
```

#### `prometheus-jmx-exporter`

- `lowercaseOutputName` - Lowercase the output metric name. Applies to default
  format and `name`. Defaults to `true`.
- `lowercaseOutputLabelNames` - Lowercase the output metric label names.
  Applies to default format and `labels`. Defaults to `true`.
- `whitelistObjectNames` - A list of [ObjectNames](http://docs.oracle.com/javase/6/docs/api/javax/management/ObjectName.html)
  to query. Defaults to all mBeans.
- `blacklistObjectNames` - A list of [ObjectNames](http://docs.oracle.com/javase/6/docs/api/javax/management/ObjectName.html)
  to not query. Takes precedence over `whitelistObjectNames`. Defaults to none.
- `rules` - A list of `rule` elements to apply in order. Processing stops at
  the first matching rule. Attributes that aren't matched aren't collected. If
  not specified, defaults to collecting everything in the default format.

#### `rule`

- `pattern` - Regex pattern to match against each bean attribute. The pattern
  is not anchored. Capture groups can be used in other options. Defaults to
  matching everything.
- `attrNameSnakeCase` - Converts the attribute name to snake case. This is seen
  in the names matched by the pattern and the default format. For example,
  `anAttrName` to `an_attr_name`. Defaults to `false`.
- `name` - The metric name to set. Capture groups from the `pattern` can be
  used. If not specified, the default format will be used. If it evaluates to
  empty, processing of this attribute stops with no output.
- `value` - Value for the metric. Static values and capture groups from the
  `pattern` can be used. If not specified the scraped mBean value will be used.
- `valueFactor` - Optional number that `value` (or the scraped mBean value if
  `value` is not specified) is multiplied by, mainly used to convert mBean
  values from milliseconds to seconds.
- `labels` - A list of `label` elements.
- `help` - Help text for the metric. Capture groups from `pattern` can be used.
  `name` must be set to use this. Defaults to the mBean attribute description
  and the full name of the attribute.
- `type` - The type of the metric, can be `GAUGE`, `COUNTER` or `UNTYPED`.
  `name` must be set to use this. Defaults to `UNTYPED`.


#### `label`

Capture groups from `pattern` can be used in each. `name` must be set to use
this. Empty names and values are ignored. If not specified and the default
format is not being used, no labels are set.

- `name`
- `value`

#### Default Configuration

By default no configuration is necessary to export metrics for the broker,
addresses, and queues. The following rules are embedded in the code and used
when no other rules are specified. If any rules are specified explicitly via
`broker.xml` then *all* of the following rules are removed. These rules were
adapted from [these](https://github.com/prometheus/jmx_exporter/blob/master/example_configs/artemis-2.yml)
available from the Prometheus JMX Exporter GitHub project. The various
`pattern` elements have been changed slightly to be properly escaped for
HTML rather than YAML.

```xml
<prometheus-jmx-exporter>
   <rules>
      <rule>
         <pattern>^org.apache.activemq.artemis&lt;broker=&quot;([^&quot;]*)&quot;>&lt;>([^:]*):\s(.*)</pattern>
         <name>artemis_$2</name>
         <attr-name-snake-case>true</attr-name-snake-case>
         <type>COUNTER</type>
      </rule>
      <rule>
         <pattern>^org.apache.activemq.artemis&lt;broker=&quot;([^&quot;]*)&quot;,\s*component=addresses,\s*address=&quot;([^&quot;]*)&quot;>&lt;>([^:]*):\s(.*)</pattern>
         <name>artemis_$3</name>
         <attr-name-snake-case>true</attr-name-snake-case>
         <type>COUNTER</type>
         <labels>
            <label name="address" value="$2"/>
         </labels>
      </rule>
      <rule>
         <pattern>^org.apache.activemq.artemis&lt;broker=&quot;([^&quot;]*)&quot;,\s*component=addresses,\s*address=&quot;([^&quot;]*)&quot;,\s*subcomponent=queues,\s*routing-type=&quot;([^&quot;]*)&quot;,\s*(queue|topic)=&quot;([^&quot;]*)&quot;>&lt;>([^: ]*):\s(.*)</pattern>
         <name>artemis_$6</name>
         <attr-name-snake-case>true</attr-name-snake-case>
         <type>COUNTER</type>
         <labels>
            <label name="address" value="$2"/>
            <label name="queue" value="$5"/>
         </labels>
      </rule>
   </rules>
</prometheus-jmx-exporter>
```