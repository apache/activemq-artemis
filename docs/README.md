<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

Release Notes - Apache ActiveMQ Artemis 1.2.0
---------------------------------------------

A complete list of JIRAs for the 1.2.0 release can be found at the [Apache ActiveMQ Artemis project
JIRA](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12315920&version=12333274)

Apache ActiveMQ Artemis 1.2.0 has several improvements around performance, support for OSGi, LDAP integration and better OpenWire support.

Release Notes - Apache ActiveMQ Artemis 1.1.0
---------------------------------------------

A complete list for JIRAs for the 1.1.0 release can be found at the
[Apache ActiveMQ Artemis project
JIRA](https://issues.apache.org/jira/secure/ReleaseNote.jspa?version=12332642&styleName=Html&projectId=12315920&Create=Create&atl_token=A5KQ-2QAV-T4JA-FDED%7C708a588702fdb54724fe817fd07ee4c5610de292%7Clin)


Overall Apache ActiveMQ Artemis 1.1.0 has several improvements and it's a nice polish on top of Apache ActiveMQ Artemis 1.0.0, things like better examples, better OpenWire support and the first release of MQTT.

We would like to highlight the following accomplishments on this release:

- [[ARTEMIS-154](https://issues.apache.org/jira/browse/ARTEMIS-154)]
This is our first implementation of the MQTT support.

- [ARTEMIS-178](https://issues.apache.org/jira/browse/ARTEMIS-178)
The examples are now using the CLI to create and start servers reflecting real cases used in production.
The documentation about the examples has been updated. Please refer to the documentation for more information about this.

- [[ARTEMIS-116](https://issues.apache.org/jira/browse/ARTEMIS-116)]
The CLI has been improved. There are new tools to compact the journal and we did a lot of polish around the CLI.


- **Improvements on OpenWire**
We fixed several issues around OpenWire


### Where we are still improving.

1.1.0 has improved a lot, and these are the areas we are still working on:

- This is the first release with MQTT. We would like to hear from you if you have any issues around MQTT as we are continuously improving the MQTT support

- Short list of what need to be implemented on OpenWire for future releases:
    - Reconnection
    - Better Flow Control on producers
    - Compressed messages
    - Optimized ACKs
    - Streamlet support




Release Notes - Apache ActiveMQ Artemis 1.0.0
---------------------------------------------

The Apache ActiveMQ Artemis 1.0.0 release notes can be found in the
[Apache ActiveMQ Artemis project
JIRA](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12315920&version=12328953).
For more info on what this release has to offer please refer to the
quick start guide or the user manual. Enjoy!!
