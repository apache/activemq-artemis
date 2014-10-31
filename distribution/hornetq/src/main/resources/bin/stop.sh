#!/bin/sh

export HORNETQ_HOME=..
if [ a"$1" = a ]; then CONFIG_DIR=$HORNETQ_HOME/config/stand-alone/non-clustered; else CONFIG_DIR="$1"; fi
touch $CONFIG_DIR/STOP_ME;