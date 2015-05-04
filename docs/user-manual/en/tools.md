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
$ ./artemis help data
NAME
        artemis data - data tools like (print|exp|imp|exp|encode|decode)
        (example ./artemis data print)

SYNOPSIS
        artemis data
        artemis data decode [--prefix <prefix>] [--directory <directory>]
                [--suffix <suffix>] [--file-size <size>]
        artemis data encode [--prefix <prefix>] [--directory <directory>]
                [--suffix <suffix>] [--file-size <size>]
        artemis data exp [--bindings <binding>]
                [--large-messages <largeMessges>] [--paging <paging>]
                [--journal <journal>]
        artemis data imp [--password <password>] [--port <port>] [--host <host>]
                [--user <user>] [--transaction]
        artemis data print [--bindings <binding>] [--paging <paging>]
                [--journal <journal>]

COMMANDS
        With no arguments, Display help information

        print
            Print data records information (WARNING: don't use while a
            production server is running)

            With --bindings option, The folder used for bindings (default
            ../data/bindings)

            With --paging option, The folder used for paging (default
            ../data/paging)

            With --journal option, The folder used for messages journal (default
            ../data/journal)

        exp
            Export all message-data using an XML that could be interpreted by
            any system.

            With --bindings option, The folder used for bindings (default
            ../data/bindings)

            With --large-messages option, The folder used for large-messages
            (default ../data/largemessages)

            With --paging option, The folder used for paging (default
            ../data/paging)

            With --journal option, The folder used for messages journal (default
            ../data/journal)

        imp
            Import all message-data using an XML that could be interpreted by
            any system.

            With --password option, User name used to import the data. (default
            null)

            With --port option, The port used to import the data (default 61616)

            With --host option, The host used to import the data (default
            localhost)

            With --user option, User name used to import the data. (default
            null)

            With --transaction option, If this is set to true you will need a
            whole transaction to commit at the end. (default false)

        decode
            Decode a journal's internal format into a new journal set of files

            With --prefix option, The journal prefix (default activemq-datal)

            With --directory option, The journal folder (default
            ../data/journal)

            With --suffix option, The journal suffix (default amq)

            With --file-size option, The journal size (default 10485760)

        encode
            Encode a set of journal files into an internal encoded data format

            With --prefix option, The journal prefix (default activemq-datal)

            With --directory option, The journal folder (default
            ../data/journal)

            With --suffix option, The journal suffix (default amq)

            With --file-size option, The journal size (default 10485760)



```