# Libaio Native Libraries

ActiveMQ distributes a native library, used as a bridge between ActiveMQ
and Linux libaio.

`libaio` is a library, developed as part of the Linux kernel project.
With `libaio` we submit writes to the operating system where they are
processed asynchronously. Some time later the OS will call our code back
when they have been processed.

We use this in our high performance journal if configured to do so,
please see [Persistence](persistence.md).

These are the native libraries distributed by ActiveMQ:

-   libActiveMQAIO32.so - x86 32 bits

-   libActiveMQAIO64.so - x86 64 bits

When using libaio, ActiveMQ will always try loading these files as long
as they are on the [library path](#using-server.library.path).

## Compiling the native libraries

In the case that you are using Linux on a platform other than x86\_32 or
x86\_64 (for example Itanium 64 bits or IBM Power) you may need to
compile the native library, since we do not distribute binaries for
those platforms with the release.

## Install requirements

> **Note**
>
> At the moment the native layer is only available on Linux. If you are
> in a platform other than Linux the native compilation will not work

The native library uses
[autoconf](http://en.wikipedia.org/wiki/Autoconf) what makes the
compilation process easy, however you need to install extra packages as
a requirement for compilation:

-   gcc - C Compiler

-   gcc-c++ or g++ - Extension to gcc with support for C++

-   autoconf - Tool for automating native build process

-   make - Plain old make

-   automake - Tool for automating make generation

-   libtool - Tool for link editing native libraries

-   libaio - library to disk asynchronous IO kernel functions

-   libaio-dev - Compilation support for libaio

-   A full JDK installed with the environment variable JAVA\_HOME set to
    its location

To perform this installation on RHEL or Fedora, you can simply type this
at a command line:

    sudo yum install automake libtool autoconf gcc-c++ gcc libaio libaio-devel make

Or on Debian systems:

    sudo apt-get install automake libtool autoconf gcc-g++ gcc libaio libaio-dev make

> **Note**
>
> You could find a slight variation of the package names depending on
> the version and Linux distribution. (for example gcc-c++ on Fedora
> versus g++ on Debian systems)

## Invoking the compilation

In the distribution, in the `native-src` directory, execute the shell
script `bootstrap`. This script will invoke `automake` and `make` what
will create all the make files and the native library.

    someUser@someBox:/messaging-distribution/native-src$ ./bootstrap
    checking for a BSD-compatible install... /usr/bin/install -c
    checking whether build environment is sane... yes
    checking for a thread-safe mkdir -p... /bin/mkdir -p

    ...

    configure: creating ./config.status
    config.status: creating Makefile
    config.status: creating ./src/Makefile
    config.status: creating config.h
    config.status: config.h is unchanged
    config.status: executing depfiles commands
    config.status: executing libtool commands

    ...

The produced library will be at
`./native-src/src/.libs/libActiveMQAIO.so`. Simply move that file over
`bin` on the distribution or the place you have chosen on the [library
path](#using-server.library.path).

If you want to perform changes on the ActiveMQ libaio code, you could
just call make directly at the `native-src` directory.
