# Libaio Native Libraries

Apache ActiveMQ Artemis distributes a native library, used as a bridge for its fast journal, between Apache ActiveMQ Artemis
and Linux libaio.

`libaio` is a library, developed as part of the Linux kernel project.
With `libaio` we submit writes to the operating system where they are
processed asynchronously. Some time later the OS will call our code back
when they have been processed.

We use this in our high performance journal if configured to do so,
please see [Persistence](persistence.md).

These are the native libraries distributed by Apache ActiveMQ Artemis:

- libartemis-native-64.so - x86 64 bits
- We distributed a 32-bit version until early 2017. While it's not available on the distribution any longer it should still be possible to compile to a 32-bit environment if needed.

When using libaio, Apache ActiveMQ Artemis will always try loading these files as long
as they are on the [library path](using-server.md#library-path)


## Runtime dependencies

If you just want to use the provided native binaries you need to install the required libaio dependency.

You can install libaio using the following steps as the root user:

Using yum, (e.g. on Fedora or Red Hat Enterprise Linux):

```
yum install libaio
```

Using aptitude, (e.g. on Ubuntu or Debian system):

```
apt-get install libaio1
```

## Compiling the native libraries

In the case that you are using Linux on a platform other than x86\_32 or
x86\_64 (for example Itanium 64 bits or IBM Power) you may need to
compile the native library, since we do not distribute binaries for
those platforms with the release.

## Compilation dependencies

> **Note:**
>
> The native layer is only available on Linux. If you are
> in a platform other than Linux the native compilation will not work

These are the required linux packages to be installed for the compilation to work:

- gcc - C Compiler

- gcc-c++ or g++ - Extension to gcc with support for C++

- libtool - Tool for link editing native libraries

- libaio - library to disk asynchronous IO kernel functions

- libaio-dev - Compilation support for libaio

- cmake

- A full JDK installed with the environment variable JAVA\_HOME set to
    its location

To perform this installation on RHEL or Fedora, you can simply type this at a command line:

    sudo yum install libtool gcc-c++ gcc libaio libaio-devel cmake

Or on Debian systems:

    sudo apt-get install libtool gcc-g++ gcc libaio libaio- cmake

> **Note:**
>
> You could find a slight variation of the package names depending on
> the version and Linux distribution. (for example gcc-c++ on Fedora
> versus g++ on Debian systems)

## Invoking the compilation

In the source distribution or git clone, in the `artemis-native` directory, execute the shell
script `compile-native.sh`. This script will invoke the proper commands to perform the native build.

If you want more information refer to the [cmake web pages](https://cmake.org).

