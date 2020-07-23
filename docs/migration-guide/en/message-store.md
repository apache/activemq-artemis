#Message Store Migration

## ActiveMQ 5 KahaDB or mKahaDB

ActiveMQ Artemis supports an XML format for message store exchange. An existing store may be exported from a broker using the command line tools and subsequently imported to another broker.

The [Apache ActiveMQ Command Line Tools](https://github.com/apache/activemq-cli-tools) project provides an command line export tool for ActiveMQ 5.x that will export a KahaDB (or mKahaDB) message store into the ActiveMQ Artemis XML format, for subsequent import by ActiveMQ Artemis.

The export tool supports selective export using filters, useful if only some of your data needs to be migrated. From version 0.2.0, the export tool has support for virtual topic consumer queue mapping, which will allow existing Openwire virtual topic consumers to resume on an ActiveMQ Artemis broker with no message loss. Note the OpenWire acceptor `virtualTopicConsumerWildcards` option from [virtual topics migration](VirtualTopics.md).

Full details of tool can be found on the project website: https://github.com/apache/activemq-cli-tools
