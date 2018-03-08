# Tools

You can use the artemis cli interface to execute data maintenance tools:

This is a list of sub-commands available

Name | Description
:--- | :---
exp     | Export the message data using a special and independent XML format
imp  | Imports the journal to a running broker using the output from expt
data     | Prints a report about journal records and summary of existent records, as well a report on paging
encode | shows an internal format of the journal encoded to String
decode | imports the internal journal format from encode

You can use the help at the tool for more information on how to execute each of the tools. For example:

```
$ ./artemis help data print
NAME
        artemis data print - Print data records information (WARNING: don't use
        while a production server is running)

SYNOPSIS
        artemis data print [--bindings <binding>] [--journal <journal>]
                [--paging <paging>]

OPTIONS
        --bindings <binding>
            The folder used for bindings (default ../data/bindings)

        --journal <journal>
            The folder used for messages journal (default ../data/journal)

        --paging <paging>
            The folder used for paging (default ../data/paging)


```


For a full list of data tools commands available use:

```
NAME
        artemis data - data tools group
        (print|imp|exp|encode|decode|compact) (example ./artemis data print)

SYNOPSIS
        artemis data
        artemis data compact [--broker <brokerConfig>] [--verbose]
                [--paging <paging>] [--journal <journal>]
                [--large-messages <largeMessges>] [--bindings <binding>]
        artemis data decode [--broker <brokerConfig>] [--suffix <suffix>]
                [--verbose] [--paging <paging>] [--prefix <prefix>] [--file-size <size>]
                [--directory <directory>] --input <input> [--journal <journal>]
                [--large-messages <largeMessges>] [--bindings <binding>]
        artemis data encode [--directory <directory>] [--broker <brokerConfig>]
                [--suffix <suffix>] [--verbose] [--paging <paging>] [--prefix <prefix>]
                [--file-size <size>] [--journal <journal>]
                [--large-messages <largeMessges>] [--bindings <binding>]
        artemis data exp [--broker <brokerConfig>] [--verbose]
                [--paging <paging>] [--journal <journal>]
                [--large-messages <largeMessges>] [--bindings <binding>]
        artemis data imp [--host <host>] [--verbose] [--port <port>]
                [--password <password>] [--transaction] --input <input> [--user <user>]
        artemis data print [--broker <brokerConfig>] [--verbose]
                [--paging <paging>] [--journal <journal>]
                [--large-messages <largeMessges>] [--bindings <binding>]

COMMANDS
        With no arguments, Display help information

        print
            Print data records information (WARNING: don't use while a
            production server is running)

            With --broker option, This would override the broker configuration
            from the bootstrap

            With --verbose option, Adds more information on the execution

            With --paging option, The folder used for paging (default from
            broker.xml)

            With --journal option, The folder used for messages journal (default
            from broker.xml)

            With --large-messages option, The folder used for large-messages
            (default from broker.xml)

            With --bindings option, The folder used for bindings (default from
            broker.xml)

        exp
            Export all message-data using an XML that could be interpreted by
            any system.

            With --broker option, This would override the broker configuration
            from the bootstrap

            With --verbose option, Adds more information on the execution

            With --paging option, The folder used for paging (default from
            broker.xml)

            With --journal option, The folder used for messages journal (default
            from broker.xml)

            With --large-messages option, The folder used for large-messages
            (default from broker.xml)

            With --bindings option, The folder used for bindings (default from
            broker.xml)

        imp
            Import all message-data using an XML that could be interpreted by
            any system.

            With --host option, The host used to import the data (default
            localhost)

            With --verbose option, Adds more information on the execution

            With --port option, The port used to import the data (default 61616)

            With --password option, User name used to import the data. (default
            null)

            With --transaction option, If this is set to true you will need a
            whole transaction to commit at the end. (default false)

            With --input option, The input file name (default=exp.dmp)

            With --user option, User name used to import the data. (default
            null)

        decode
            Decode a journal's internal format into a new journal set of files

            With --broker option, This would override the broker configuration
            from the bootstrap

            With --suffix option, The journal suffix (default amq)

            With --verbose option, Adds more information on the execution

            With --paging option, The folder used for paging (default from
            broker.xml)

            With --prefix option, The journal prefix (default activemq-data)

            With --file-size option, The journal size (default 10485760)

            With --directory option, The journal folder (default journal folder
            from broker.xml)

            With --input option, The input file name (default=exp.dmp)

            With --journal option, The folder used for messages journal (default
            from broker.xml)

            With --large-messages option, The folder used for large-messages
            (default from broker.xml)

            With --bindings option, The folder used for bindings (default from
            broker.xml)

        encode
            Encode a set of journal files into an internal encoded data format

            With --directory option, The journal folder (default the journal
            folder from broker.xml)

            With --broker option, This would override the broker configuration
            from the bootstrap

            With --suffix option, The journal suffix (default amq)

            With --verbose option, Adds more information on the execution

            With --paging option, The folder used for paging (default from
            broker.xml)

            With --prefix option, The journal prefix (default activemq-data)

            With --file-size option, The journal size (default 10485760)

            With --journal option, The folder used for messages journal (default
            from broker.xml)

            With --large-messages option, The folder used for large-messages
            (default from broker.xml)

            With --bindings option, The folder used for bindings (default from
            broker.xml)

        compact
            Compacts the journal of a non running server

            With --broker option, This would override the broker configuration
            from the bootstrap

            With --verbose option, Adds more information on the execution

            With --paging option, The folder used for paging (default from
            broker.xml)

            With --journal option, The folder used for messages journal (default
            from broker.xml)

            With --large-messages option, The folder used for large-messages
            (default from broker.xml)

            With --bindings option, The folder used for bindings (default from
            broker.xml)
```