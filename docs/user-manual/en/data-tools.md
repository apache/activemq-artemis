# Data Tools

You can use the Artemis CLI to execute data maintenance tools:

This is a list of sub-commands available

Name | Description
---|---
exp | Export the message data using a special and independent XML format
imp | Imports the journal to a running broker using the output from expt
data | Prints a report about journal records and summary of existent records, as well a report on paging
encode | shows an internal format of the journal encoded to String
decode | imports the internal journal format from encode

You can use the help at the tool for more information on how to execute each of the tools. For example:

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
                [--jdbc-page-store-table-name <jdbcPageStore>] [--journal <journal>]
                [--large-messages <largeMessges>] [--output <output>]
                [--paging <paging>] [--safe] [--verbose] [--] [<configuration>]

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

        --jdbc-page-store-table-name <jdbcPageStore>
            Name of the page sotre messages table

        --journal <journal>
            The folder used for messages journal (default from broker.xml)

        --large-messages <largeMessges>
            The folder used for large-messages (default from broker.xml)

        --output <output>
            Output name for the file

        --paging <paging>
            The folder used for paging (default from broker.xml)

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
        artemis data - data tools group (print|imp|exp|encode|decode|compact)
        (example ./artemis data print)

SYNOPSIS
        artemis data
        artemis data compact [--verbose] [--paging <paging>]
                [--journal <journal>] [--large-messages <largeMessges>]
                [--broker <brokerConfig>] [--bindings <binding>]
        artemis data decode [--verbose] [--suffix <suffix>] [--paging <paging>]
                [--prefix <prefix>] [--file-size <size>] --input <input>
                [--journal <journal>] [--directory <directory>]
                [--large-messages <largeMessges>] [--broker <brokerConfig>]
                [--bindings <binding>]
        artemis data encode [--verbose] [--directory <directory>]
                [--suffix <suffix>] [--paging <paging>] [--prefix <prefix>]
                [--file-size <size>] [--journal <journal>]
                [--large-messages <largeMessges>] [--broker <brokerConfig>]
                [--bindings <binding>]
        artemis data exp [--jdbc-bindings-table-name <jdbcBindings>]
                [--jdbc-message-table-name <jdbcMessages>] [--paging <paging>]
                [--jdbc-connection-url <jdbcURL>]
                [--jdbc-large-message-table-name <jdbcLargeMessages>] [--f]
                [--large-messages <largeMessges>] [--broker <brokerConfig>]
                [--jdbc-page-store-table-name <jdbcPageStore>]
                [--jdbc-driver-class-name <jdbcClassName>] [--jdbc] [--verbose]
                [--journal <journal>] [--output <output>] [--bindings <binding>]
        artemis data imp [--user <user>] [--legacy-prefixes] [--verbose]
                [--host <host>] [--port <port>] [--transaction] --input <input>
                [--password <password>] [--sort]
        artemis data print [--jdbc-bindings-table-name <jdbcBindings>]
                [--jdbc-message-table-name <jdbcMessages>] [--paging <paging>]
                [--jdbc-connection-url <jdbcURL>]
                [--jdbc-large-message-table-name <jdbcLargeMessages>] [--f]
                [--large-messages <largeMessges>] [--broker <brokerConfig>]
                [--jdbc-page-store-table-name <jdbcPageStore>]
                [--jdbc-driver-class-name <jdbcClassName>] [--safe] [--jdbc] [--verbose]
                [--journal <journal>] [--output <output>] [--bindings <binding>]

COMMANDS
        With no arguments, Display help information

        print
            Print data records information (WARNING: don't use while a
            production server is running)

            With --jdbc-bindings-table-name option, Name of the jdbc bindigns
            table

            With --jdbc-message-table-name option, Name of the jdbc messages
            table

            With --paging option, The folder used for paging (default from
            broker.xml)

            With --jdbc-connection-url option, The connection used for the
            database

            With --jdbc-large-message-table-name option, Name of the large
            messages table

            With --f option, This will allow certain tools like print-data to be
            performed ignoring any running servers. WARNING: Changing data
            concurrently with a running broker may damage your data. Be careful
            with this option.

            With --large-messages option, The folder used for large-messages
            (default from broker.xml)

            With --broker option, This would override the broker configuration
            from the bootstrap

            With --jdbc-page-store-table-name option, Name of the page sotre
            messages table

            With --jdbc-driver-class-name option, JDBC driver classname

            With --safe option, It will print your data structure without
            showing your data

            With --jdbc option, It will activate jdbc

            With --verbose option, Adds more information on the execution

            With --journal option, The folder used for messages journal (default
            from broker.xml)

            With --output option, Output name for the file

            With --bindings option, The folder used for bindings (default from
            broker.xml)

        exp
            Export all message-data using an XML that could be interpreted by
            any system.

            With --jdbc-bindings-table-name option, Name of the jdbc bindigns
            table

            With --jdbc-message-table-name option, Name of the jdbc messages
            table

            With --paging option, The folder used for paging (default from
            broker.xml)

            With --jdbc-connection-url option, The connection used for the
            database

            With --jdbc-large-message-table-name option, Name of the large
            messages table

            With --f option, This will allow certain tools like print-data to be
            performed ignoring any running servers. WARNING: Changing data
            concurrently with a running broker may damage your data. Be careful
            with this option.

            With --large-messages option, The folder used for large-messages
            (default from broker.xml)

            With --broker option, This would override the broker configuration
            from the bootstrap

            With --jdbc-page-store-table-name option, Name of the page sotre
            messages table

            With --jdbc-driver-class-name option, JDBC driver classname

            With --jdbc option, It will activate jdbc

            With --verbose option, Adds more information on the execution

            With --journal option, The folder used for messages journal (default
            from broker.xml)

            With --output option, Output name for the file

            With --bindings option, The folder used for bindings (default from
            broker.xml)

        imp
            Import all message-data using an XML that could be interpreted by
            any system.

            With --user option, User name used to import the data. (default
            null)

            With --legacy-prefixes option, Do not remove prefixes from legacy
            imports

            With --verbose option, Adds more information on the execution

            With --host option, The host used to import the data (default
            localhost)

            With --port option, The port used to import the data (default 61616)

            With --transaction option, If this is set to true you will need a
            whole transaction to commit at the end. (default false)

            With --input option, The input file name (default=exp.dmp)

            With --password option, User name used to import the data. (default
            null)

            With --sort option, Sort the messages from the input (used for older
            versions that won't sort messages)

        decode
            Decode a journal's internal format into a new journal set of files

            With --verbose option, Adds more information on the execution

            With --suffix option, The journal suffix (default amq)

            With --paging option, The folder used for paging (default from
            broker.xml)

            With --prefix option, The journal prefix (default activemq-data)

            With --file-size option, The journal size (default 10485760)

            With --input option, The input file name (default=exp.dmp)

            With --journal option, The folder used for messages journal (default
            from broker.xml)

            With --directory option, The journal folder (default journal folder
            from broker.xml)

            With --large-messages option, The folder used for large-messages
            (default from broker.xml)

            With --broker option, This would override the broker configuration
            from the bootstrap

            With --bindings option, The folder used for bindings (default from
            broker.xml)

        encode
            Encode a set of journal files into an internal encoded data format

            With --verbose option, Adds more information on the execution

            With --directory option, The journal folder (default the journal
            folder from broker.xml)

            With --suffix option, The journal suffix (default amq)

            With --paging option, The folder used for paging (default from
            broker.xml)

            With --prefix option, The journal prefix (default activemq-data)

            With --file-size option, The journal size (default 10485760)

            With --journal option, The folder used for messages journal (default
            from broker.xml)

            With --large-messages option, The folder used for large-messages
            (default from broker.xml)

            With --broker option, This would override the broker configuration
            from the bootstrap

            With --bindings option, The folder used for bindings (default from
            broker.xml)

        compact
            Compacts the journal of a non running server

            With --verbose option, Adds more information on the execution

            With --paging option, The folder used for paging (default from
            broker.xml)

            With --journal option, The folder used for messages journal (default
            from broker.xml)

            With --large-messages option, The folder used for large-messages
            (default from broker.xml)

            With --broker option, This would override the broker configuration
            from the bootstrap

            With --bindings option, The folder used for bindings (default from
            broker.xml)

```