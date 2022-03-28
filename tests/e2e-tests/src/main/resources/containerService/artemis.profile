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

# this is to replace the artemis.profile on an instance meant to be used as part of a docker image.

ARTEMIS_HOME='/opt/activemq-artemis'
ARTEMIS_INSTANCE='/var/lib/artemis-instance'
ARTEMIS_DATA_DIR='/var/lib/artemis-instance/data'
ARTEMIS_ETC_DIR='/var/lib/artemis-instance/etc'
ARTEMIS_OOME_DUMP='/var/lib/artemis-instance/log/oom_dump.hprof'

# The logging config will need an URI
# this will be encoded in case you use spaces or special characters
# on your directory structure
ARTEMIS_INSTANCE_URI='file:/var/lib/artemis-instance/./'
ARTEMIS_INSTANCE_ETC_URI='file:/var/lib/artemis-instance/./etc/'

# Hawtio Properties
HAWTIO_ROLE='amq'

# Java Opts
if [ -z "$JAVA_ARGS" ]; then
    JAVA_ARGS="-XX:+PrintClassHistogram -XX:+UseG1GC -XX:+UseStringDeduplication -Xms128M -Xmx512M -Dhawtio.disableProxy=true -Dhawtio.realm=activemq -Dhawtio.offline=true -Dhawtio.rolePrincipalClasses=org.apache.activemq.artemis.spi.core.security.jaas.RolePrincipal -Djolokia.policyLocation=${ARTEMIS_INSTANCE_ETC_URI}jolokia-access.xml -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=1099 -Dcom.sun.management.jmxremote.rmi.port=1099 -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"
fi