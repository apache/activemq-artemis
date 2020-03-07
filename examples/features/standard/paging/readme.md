# Paging Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example shows how ActiveMQ Artemis would avoid running out of memory resources by paging messages.

A maximum size can be specified per address via the address settings in the configuration file (broker.xml).

When messages routed to an address exceed the specified max-size-bytes the broker will begin to write messages to the file system, this is called paging. This will continue to occur until messages have been delivered to consumers and subsequently acknowledged freeing up memory. Messages will then be read from the file system , i.e. depaged, and routed as normal.

Acknowledgement plays an important factor on paging as messages will stay on the file system until the memory is released so it is important to make sure that the client acknowledges its messages.