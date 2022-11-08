# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

ARTEMIS_HOME='must-change'
ARTEMIS_INSTANCE='${project.basedir}/target/classes/servers/linuxUpgrade'
ARTEMIS_DATA_DIR='${project.basedir}/target/classes/servers/linuxUpgrade/data'
ARTEMIS_ETC_DIR='${project.basedir}/target/classes/servers/linuxUpgradeETC'
ARTEMIS_OOME_DUMP='${project.basedir}/target/classes/servers/linuxUpgrade/log/oom_dump.hprof'

# The logging config will need an URI
# this will be encoded in case you use spaces or special characters
# on your directory structure
ARTEMIS_INSTANCE_URI='file:${project.basedir}/target/classes/servers/linuxUpgrade/'
ARTEMIS_INSTANCE_ETC_URI='file:${project.basedir}/target/classes/servers/linuxUpgradeETC/'

# Cluster Properties: Used to pass arguments to ActiveMQ Artemis which can be referenced in broker.xml
#ARTEMIS_CLUSTER_PROPS="-Dactivemq.remoting.default.port=61617 -Dactivemq.remoting.amqp.port=5673 -Dactivemq.remoting.stomp.port=61614 -Dactivemq.remoting.hornetq.port=5446"

# Hawtio Properties
# HAWTIO_ROLE define the user role or roles required to be able to login to the console. Multiple roles to allow can
# be separated by a comma. Set to '*' or an empty value to disable role checking when Hawtio authenticates a user.
HAWTIO_ROLE='amq'

# Java Opts
if [ -z "$JAVA_ARGS" ]; then
    #### I am adding a customization to the JAVA-ARGS that would be replaced
    #### If this line is seen on the upgrade it means that things are not working
    JAVA_ARGS="-should-not-work -XX:+PrintClassHistogram -XX:+UseG1GC -XX:+UseStringDeduplication -Xms512M -Xmx1G -Dhawtio.disableProxy=true -Dhawtio.realm=activemq -Dhawtio.offline=true -Dhawtio.rolePrincipalClasses=org.apache.activemq.artemis.spi.core.security.jaas.RolePrincipal -Djolokia.policyLocation=${ARTEMIS_INSTANCE_ETC_URI}jolokia-access.xml "
fi

# Uncomment to enable logging for Safepoint JVM pauses
#
# In addition to the traditional GC logs you could enable some JVM flags to know any meaningful and "hidden" pause
# that could affect the latencies of the services delivered by the broker, including those that are not reported by
# the classic GC logs and dependent by JVM background work (eg method deoptimizations, lock unbiasing, JNI, counted
# loops and obviously GC activity).
#
# Replace "all_pauses.log" with the file name you want to log to.
# JAVA_ARGS="$JAVA_ARGS -XX:+PrintSafepointStatistics -XX:PrintSafepointStatisticsCount=1 -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCApplicationConcurrentTime -XX:+LogVMOutput -XX:LogFile=all_pauses.log"

# Uncomment to enable the dumping of the Java heap when a java.lang.OutOfMemoryError exception is thrown
# JAVA_ARGS="$JAVA_ARGS -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=${ARTEMIS_OOME_DUMP}"

# Only enable debug options for the 'run' command
if [ "$1" = "run" ]; then :
    # Uncomment to enable remote debugging
    # DEBUG_ARGS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005"

    # Uncomment for async profiler
    # DEBUG_ARGS="-XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints"
fi