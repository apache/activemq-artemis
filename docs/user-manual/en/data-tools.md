# Data Tools

You can use the Artemis CLI to execute data maintenance tools:

The following sub-commands are available when running the CLI data
command from a particular broker instance that has already been
installed using the create command:

Name | Description
---|---
print | Prints a report about journal records of a non-running server
exp | Export the message data using a special and independent XML format
imp | Imports the journal to a running broker using the output from expt
encode | shows an internal format of the journal encoded to String
decode | imports the internal journal format from encode
compact | Compacts the journal of a non running server
recover | Recover (undelete) messages from an existing journal and create a new one.

You can use the CLI help for more information on how to execute each of the tools. For example:

```
$ ./artemis help data print
NAME
        artemis data print - Print data records information (WARNING: don't use
        while a production server is running)

SYNOPSIS
        artemis data print [--bindings <binding>] [--broker <brokerConfig>]
                [--f] [--jdbc] [--jdbc-bindings-table-name <jdbcBindings>]
                [--jdbc-connection-url <jdbcURL>]
                [--jdbc-driver-class-name <jdbcClassName>]
                [--jdbc-large-message-table-name <jdbcLargeMessages>]
                [--jdbc-message-table-name <jdbcMessages>]
                [--jdbc-node-manager-table-name <jdbcNodeManager>]
                [--jdbc-page-store-table-name <jdbcPageStore>] [--journal <journal>]
                [--large-messages <largeMessges>] [--output <output>]
                [--paging <paging>] [--reclaimed] [--safe] [--verbose] [--]
                [<configuration>]

OPTIONS
        --bindings <binding>
            The folder used for bindings (default from broker.xml)

        --broker <brokerConfig>
            This would override the broker configuration from the bootstrap

        --f
            This will allow certain tools like print-data to be performed
            ignoring any running servers. WARNING: Changing data concurrently
            with a running broker may damage your data. Be careful with this
            option.

        --jdbc
            It will activate jdbc

        --jdbc-bindings-table-name <jdbcBindings>
            Name of the jdbc bindigns table

        --jdbc-connection-url <jdbcURL>
            The connection used for the database

        --jdbc-driver-class-name <jdbcClassName>
            JDBC driver classname

        --jdbc-large-message-table-name <jdbcLargeMessages>
            Name of the large messages table

        --jdbc-message-table-name <jdbcMessages>
            Name of the jdbc messages table

        --jdbc-node-manager-table-name <jdbcNodeManager>
            Name of the jdbc node manager table

        --jdbc-page-store-table-name <jdbcPageStore>
            Name of the page store messages table

        --journal <journal>
            The folder used for messages journal (default from broker.xml)

        --large-messages <largeMessges>
            The folder used for large-messages (default from broker.xml)

        --output <output>
            Output name for the file

        --paging <paging>
            The folder used for paging (default from broker.xml)

        --reclaimed
            This option will try to print as many records as possible from
            reclaimed files

        --safe
            It will print your data structure without showing your data

        --verbose
            Adds more information on the execution

        --
            This option can be used to separate command-line options from the
            list of argument, (useful when arguments might be mistaken for
            command-line options

        <configuration>
            Broker Configuration URI, default
            'xml:${ARTEMIS_INSTANCE}/etc/bootstrap.xml'
```


For a full list of data tools commands available use:

```
$ ./artemis help data
NAME
        artemis data - data tools group
        (print|imp|exp|encode|decode|compact|recover) (example ./artemis data
        print)

SYNOPSIS
        artemis data
        artemis data compact [--journal <journal>]
                [--large-messages <largeMessges>] [--paging <paging>]
                [--broker <brokerConfig>] [--bindings <binding>] [--verbose]
        artemis data decode [--journal <journal>]
                [--large-messages <largeMessges>] [--file-size <size>]
                [--paging <paging>] [--prefix <prefix>] [--suffix <suffix>]
                [--broker <brokerConfig>] [--directory <directory>]
                [--bindings <binding>] [--verbose] --input <input>
        artemis data encode [--journal <journal>]
                [--large-messages <largeMessges>] [--file-size <size>]
                [--paging <paging>] [--prefix <prefix>] [--suffix <suffix>]
                [--broker <brokerConfig>] [--bindings <binding>] [--verbose]
                [--directory <directory>]
        artemis data exp [--jdbc-driver-class-name <jdbcClassName>]
                [--journal <journal>] [--jdbc-connection-url <jdbcURL>]
                [--large-messages <largeMessges>]
                [--jdbc-bindings-table-name <jdbcBindings>] [--paging <paging>] [--f]
                [--jdbc-large-message-table-name <jdbcLargeMessages>]
                [--broker <brokerConfig>] [--jdbc-page-store-table-name <jdbcPageStore>]
                [--bindings <binding>] [--jdbc] [--verbose]
                [--jdbc-message-table-name <jdbcMessages>]
                [--jdbc-node-manager-table-name <jdbcNodeManager>] [--output <output>]
        artemis data imp [--legacy-prefixes] [--password <password>]
                [--transaction] [--verbose] [--port <port>] [--user <user>] [--sort]
                --input <input> [--host <host>]
        artemis data print [--reclaimed]
                [--jdbc-driver-class-name <jdbcClassName>] [--journal <journal>]
                [--jdbc-connection-url <jdbcURL>] [--large-messages <largeMessges>]
                [--jdbc-bindings-table-name <jdbcBindings>] [--paging <paging>] [--f]
                [--jdbc-large-message-table-name <jdbcLargeMessages>] [--safe]
                [--broker <brokerConfig>] [--jdbc-page-store-table-name <jdbcPageStore>]
                [--bindings <binding>] [--jdbc] [--verbose]
                [--jdbc-message-table-name <jdbcMessages>]
                [--jdbc-node-manager-table-name <jdbcNodeManager>] [--output <output>]
        artemis data recover [--jdbc-driver-class-name <jdbcClassName>]
                [--journal <journal>] [--jdbc-connection-url <jdbcURL>]
                [--large-messages <largeMessges>] [--reclaimed] --target <outputJournal>
                [--jdbc-bindings-table-name <jdbcBindings>] [--paging <paging>] [--f]
                [--jdbc-large-message-table-name <jdbcLargeMessages>]
                [--broker <brokerConfig>] [--jdbc-page-store-table-name <jdbcPageStore>]
                [--bindings <binding>] [--jdbc] [--verbose]
                [--jdbc-message-table-name <jdbcMessages>]
                [--jdbc-node-manager-table-name <jdbcNodeManager>] [--output <output>]

COMMANDS
        With no arguments, Display help information

        recover
            Recover (undelete) every message on the journal by creating a new
            output journal. Rolled backed and acked messages will be sent out to
            the output as much as possible.

            With --jdbc-driver-class-name option, JDBC driver classname

            With --journal option, The folder used for messages journal (default
            from broker.xml)

            With --jdbc-connection-url option, The connection used for the
            database

            With --large-messages option, The folder used for large-messages
            (default from broker.xml)

            With --reclaimed option, This option will try to recover as many
            records as possible from reclaimed files

            With --target option, Output folder container the new journal with
            all the generated messages

            With --jdbc-bindings-table-name option, Name of the jdbc bindigns
            table

            With --paging option, The folder used for paging (default from
            broker.xml)

            With --f option, This will allow certain tools like print-data to be
            performed ignoring any running servers. WARNING: Changing data
            concurrently with a running broker may damage your data. Be careful
            with this option.

            With --jdbc-large-message-table-name option, Name of the large
            messages table

            With --broker option, This would override the broker configuration
            from the bootstrap

            With --jdbc-page-store-table-name option, Name of the page store
            messages table

            With --bindings option, The folder used for bindings (default from
            broker.xml)

            With --jdbc option, It will activate jdbc

            With --verbose option, Adds more information on the execution

            With --jdbc-message-table-name option, Name of the jdbc messages
            table

            With --jdbc-node-manager-table-name option, Name of the jdbc node
            manager table

            With --output option, Output name for the file

        print
            Print data records information (WARNING: don't use while a
            production server is running)

            With --reclaimed option, This option will try to print as many
            records as possible from reclaimed files

            With --jdbc-driver-class-name option, JDBC driver classname

            With --journal option, The folder used for messages journal (default
            from broker.xml)

            With --jdbc-connection-url option, The connection used for the
            database

            With --large-messages option, The folder used for large-messages
            (default from broker.xml)

            With --jdbc-bindings-table-name option, Name of the jdbc bindigns
            table

            With --paging option, The folder used for paging (default from
            broker.xml)

            With --f option, This will allow certain tools like print-data to be
            performed ignoring any running servers. WARNING: Changing data
            concurrently with a running broker may damage your data. Be careful
            with this option.

            With --jdbc-large-message-table-name option, Name of the large
            messages table

            With --safe option, It will print your data structure without
            showing your data

            With --broker option, This would override the broker configuration
            from the bootstrap

            With --jdbc-page-store-table-name option, Name of the page store
            messages table

            With --bindings option, The folder used for bindings (default from
            broker.xml)

            With --jdbc option, It will activate jdbc

            With --verbose option, Adds more information on the execution

            With --jdbc-message-table-name option, Name of the jdbc messages
            table

            With --jdbc-node-manager-table-name option, Name of the jdbc node
            manager table

            With --output option, Output name for the file

        exp
            Export all message-data using an XML that could be interpreted by
            any system.

            With --jdbc-driver-class-name option, JDBC driver classname

            With --journal option, The folder used for messages journal (default
            from broker.xml)

            With --jdbc-connection-url option, The connection used for the
            database

            With --large-messages option, The folder used for large-messages
            (default from broker.xml)

            With --jdbc-bindings-table-name option, Name of the jdbc bindigns
            table

            With --paging option, The folder used for paging (default from
            broker.xml)

            With --f option, This will allow certain tools like print-data to be
            performed ignoring any running servers. WARNING: Changing data
            concurrently with a running broker may damage your data. Be careful
            with this option.

            With --jdbc-large-message-table-name option, Name of the large
            messages table

            With --broker option, This would override the broker configuration
            from the bootstrap

            With --jdbc-page-store-table-name option, Name of the page store
            messages table

            With --bindings option, The folder used for bindings (default from
            broker.xml)

            With --jdbc option, It will activate jdbc

            With --verbose option, Adds more information on the execution

            With --jdbc-message-table-name option, Name of the jdbc messages
            table

            With --jdbc-node-manager-table-name option, Name of the jdbc node
            manager table

            With --output option, Output name for the file

        imp
            Import all message-data using an XML that could be interpreted by
            any system.

            With --legacy-prefixes option, Do not remove prefixes from legacy
            imports

            With --password option, User name used to import the data. (default
            null)

            With --transaction option, If this is set to true you will need a
            whole transaction to commit at the end. (default false)

            With --verbose option, Adds more information on the execution

            With --port option, The port used to import the data (default 61616)

            With --user option, User name used to import the data. (default
            null)

            With --sort option, Sort the messages from the input (used for older
            versions that won't sort messages)

            With --input option, The input file name (default=exp.dmp)

            With --host option, The host used to import the data (default
            localhost)

        decode
            Decode a journal's internal format into a new journal set of files

            With --journal option, The folder used for messages journal (default
            from broker.xml)

            With --large-messages option, The folder used for large-messages
            (default from broker.xml)

            With --file-size option, The journal size (default 10485760)

            With --paging option, The folder used for paging (default from
            broker.xml)

            With --prefix option, The journal prefix (default activemq-data)

            With --suffix option, The journal suffix (default amq)

            With --broker option, This would override the broker configuration
            from the bootstrap

            With --directory option, The journal folder (default journal folder
            from broker.xml)

            With --bindings option, The folder used for bindings (default from
            broker.xml)

            With --verbose option, Adds more information on the execution

            With --input option, The input file name (default=exp.dmp)

        encode
            Encode a set of journal files into an internal encoded data format

            With --journal option, The folder used for messages journal (default
            from broker.xml)

            With --large-messages option, The folder used for large-messages
            (default from broker.xml)

            With --file-size option, The journal size (default 10485760)

            With --paging option, The folder used for paging (default from
            broker.xml)

            With --prefix option, The journal prefix (default activemq-data)

            With --suffix option, The journal suffix (default amq)

            With --broker option, This would override the broker configuration
            from the bootstrap

            With --bindings option, The folder used for bindings (default from
            broker.xml)

            With --verbose option, Adds more information on the execution

            With --directory option, The journal folder (default the journal
            folder from broker.xml)

        compact
            Compacts the journal of a non running server

            With --journal option, The folder used for messages journal (default
            from broker.xml)

            With --large-messages option, The folder used for large-messages
            (default from broker.xml)

            With --paging option, The folder used for paging (default from
            broker.xml)

            With --broker option, This would override the broker configuration
            from the bootstrap

            With --bindings option, The folder used for bindings (default from
            broker.xml)

            With --verbose option, Adds more information on the execution

```
