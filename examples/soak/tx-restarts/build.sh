#!/bin/sh

OVERRIDE_ANT_HOME=../../tools/ant
export OVERRIDE_ANT_HOME

if [ -f "../../../src/bin/build.sh" ]; then
   # running from TRUNK
   ../../../src/bin/build.sh "$@"
else
   # running from the distro
   ../../bin/build.sh "$@"
fi



