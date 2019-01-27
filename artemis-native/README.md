# Introduction


This is a simple tutorial about building and packaging the libartemis-native library. The libartemis-native is a thin
layer library that interface with Linux' lib AIO library as part of the journaling feature of the broker when operating
with AIO journal.

The lib AIO is a Linux-specific dependency, therefore having a relatively modern Linux operating system is assumed for
the purpose of this documentation.

## Dependencies

In order to build the package, make sure you install these packages:

- The GNU compiler library container both the C and C++ compiler
- The GNU C library
- The respective libaio package for your Linux distribution
- JDK (full JDK)


For example, on Fedora Linux, compilation of the library requires the following specific packages:

- glibc-devel
- libaio-devel
- gcc
- gcc-g++
- java-1.8.0-openjdk-devel

### Cross compilation

Using a 64-bit Linux OS, it is possible to cross-compile the 32-bit version of the library. For this, the 32-bits
version of the GNU C Library and lib AIO should be installed.

Once again using Fedora Linux as an example, it would mean that the following packages need to be installed:

- glibc-devel.i686
- libaio-devel.i686

## Lib AIO Information

The Lib AIO is the Linux' Kernel Asynchronous I/O Support Library. It is part of the kernel project. The library makes
system calls on the kernel layer.

This is the project information:

Git Repository:  git://git.kernel.org/pub/scm/libs/libaio/libaio.git
Mailing List:    linux-aio@kvack.org


## Steps to build

1. Make sure you have JAVA_HOME defined, and pointing to the root of your JDK:

Example:

```export JAVA_HOME=/usr/share/jdk1.7```


2. Call compile-native.sh. Bootstrap will call all the initial scripts you need
 $>  ./compile-native.sh

if you are missing any dependencies, autoconf would tell you what you're missing.


### Compiled File

The produced file will be under the ./target/nar (example: ./target/nar/artemis-native-1.0.0-amd64-Linux-gpp-jni/lib/amd64-Linux-gpp/jni/libartemis-native-1.0.0.so)
and you will have to rename it manually under ./bin following the appropriate pattern.

### Advanced Compilation Methods and Developer-specific Documentation

Passing additional options to the compiler:
```cmake -DCMAKE_USER_C_FLAGS="-fomit-frame-pointer" -DCMAKE_VERBOSE_MAKEFILE=On .```

Compiling with debug options:
```cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_VERBOSE_MAKEFILE=On .```

Cross-compilation:
```cmake -DCMAKE_VERBOSE_MAKEFILE=On -DCMAKE_USER_C_FLAGS="-m32" -DARTEMIS_CROSS_COMPILE=On -DARTEMIS_CROSS_COMPILE_ROOT_PATH=/usr/lib .```

Cross-compilation with debugging symbols:
```cmake -DCMAKE_VERBOSE_MAKEFILE=On -DCMAKE_USER_C_FLAGS="-m32" -DARTEMIS_CROSS_COMPILE=On -DARTEMIS_CROSS_COMPILE_ROOT_PATH=/usr/lib .```


## Lib AIO Documentation

The User Manual, chapter 38 (Libaio Native Libraries) will provide more details about our native libraries on libaio.
