Installation
============

This section describes how to install ActiveMQ Artemis.

Prerequisites
=============

> **Note**
>
> ActiveMQ Artemis only runs on Java 7 or later.

By default, ActiveMQ Artemis server runs with 1GiB of memory. If your computer
has less memory, or you want to run it with more available RAM, modify
the value in `bin/run.sh` accordingly.

For persistence, ActiveMQ Artemis uses its own fast journal, which you can
configure to use libaio (which is the default when running on Linux) or
Java NIO. In order to use the libaio module on Linux, you'll need to
install libaio, if it's not already installed.

If you're not running on Linux then you don't need to worry about this.

You can install libaio using the following steps as the root user:

Using yum, (e.g. on Fedora or Red Hat Enterprise Linux):

    yum install libaio

Using aptitude, (e.g. on Ubuntu or Debian system):

    apt-get install libaio

Installing the Standalone Distribution
======================================

After downloading the distribution, unzip it into your chosen directory.
At this point it should be possible to [run straight out of the
box](#running.standalone), the following describes the directory
structure:

             |___ bin
             |
             |___ docs
             |      |___ api
             |      |___ quickstart-guide
             |      |___ user-manual
             |
             |___ examples
             |      |___ core
             |      |___ javaee
             |      |___ jms
             |
             |___ lib
             |
             |___ licenses
             |
             |___ schemas
          

-   `bin` -- binaries and scripts needed to run ActiveMQ Artemis.

-   `docs` -- guides and javadocs for ActiveMQ

-   `examples` -- JMS and Java EE examples. Please refer to the 'running
    examples' chapter for details on how to run them.

-   `lib` -- jars and libraries needed to run ActiveMQ

-   `licenses` -- licenses for ActiveMQ

-   `schemas` -- XML Schemas used to validate ActiveMQ Artemis configuration
    files
