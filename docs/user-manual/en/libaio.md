# Libaio Native Libraries

Apache ActiveMQ Artemis distributes a native library, used as a bridge between Apache ActiveMQ Artemis
and Linux libaio.

`libaio` is a library, developed as part of the Linux kernel project.
With `libaio` we submit writes to the operating system where they are
processed asynchronously. Some time later the OS will call our code back
when they have been processed.

We use this in our high performance journal if configured to do so,
please see [Persistence](persistence.md).

These are the native libraries distributed by Apache ActiveMQ Artemis:

-   libActiveMQAIO32.so - x86 32 bits

-   libActiveMQAIO64.so - x86 64 bits

When using libaio, Apache ActiveMQ Artemis will always try loading these files as long
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

These are the required linux packages to be installed for the compilation to work:

-   gcc - C Compiler

-   gcc-c++ or g++ - Extension to gcc with support for C++

-   libtool - Tool for link editing native libraries

-   libaio - library to disk asynchronous IO kernel functions

-   libaio-dev - Compilation support for libaio

-   A full JDK installed with the environment variable JAVA\_HOME set to
    its location

To perform this installation on RHEL or Fedora, you can simply type this at a command line:

    sudo yum install libtool gcc-c++ gcc libaio libaio-devel make

Or on Debian systems:

    sudo apt-get install libtool gcc-g++ gcc libaio libaio-dev make

> **Note**
>
> You could find a slight variation of the package names depending on
> the version and Linux distribution. (for example gcc-c++ on Fedora
> versus g++ on Debian systems)

## Invoking the compilation

In the source distribution or git clone, in the `artemis-native` directory, execute the shell
script `compile-native.sh`. This script will invoke the proper maven profile to perform the native build.

    someUser@someBox:/checkout-dir/artemis-native$ ./compile-native.sh
    [INFO] Scanning for projects...
    [INFO]
    [INFO] ------------------------------------------------------------------------
    [INFO] Building ActiveMQ Artemis Native POM 6.0.0
    [INFO] ------------------------------------------------------------------------
    [INFO]
    [INFO] --- nar-maven-plugin:3.0.0:nar-validate (default-nar-validate) @ artemis-native ---
    [INFO] Using AOL: amd64-Linux-gpp
    [INFO]
    [INFO] --- maven-enforcer-plugin:1.4:enforce (enforce-java) @ artemis-native ---
    ...

The produced library will be at
`./target/nar/artemis-native-RELEASE-amd64-Linux-gpp-jni/lib/amd64-Linux-gpp/jni/
libartemis-native-RELEASE.so`. Simply move that file over
`bin` with the proper rename [library
path](#using-server.library.path).

If you want to perform changes on the Apache ActiveMQ Artemis libaio code, you could
just call make directly at the `native-src` directory.
