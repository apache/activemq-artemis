/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
var Artemis;
(function (Artemis) {
    Artemis._module
   .run(configureHelp)
   .name;

    function configureHelp(helpRegistry, $templateCache) {
        var path = 'plugin/help.md';
        helpRegistry.addUserDoc('artemis', path);
        //this is indented to render correctly as it is markdown
        $templateCache.put(path, `
 ### Artemis

 Click [Artemis](#/jmx/attributes?tab=artemis) in the top navigation bar to see the Artemis specific plugin. (The Artemis tab won't appear if there is no broker in this JVM).  The Artemis plugin works very much the same as the JMX plugin however with a focus on interacting with an Artemis broker.

 The tree view on the left-hand side shows the top level JMX tree of each broker instance running in the JVM.  Expanding the tree will show the various MBeans registered by Artemis that you can inspect via the **Attributes** tab.

 #### Creating a new Address

 To create a new address simply click on the broker or the address folder in the jmx tree and click on the create tab.

 Once you have created an address you should be able to **Send** to it by clicking on it in the jmx tree and clicking on the send tab.

 #### Creating a new Queue

 To create a new queue click on the address you want to bind the queue to and click on the create tab.

 Once you have created a queue you should be able to **Send** a message to it or **Browse** it or view the  **Attributes** or **Charts**. Simply click on the queue in th ejmx tree and click on the appropriate tab.

 You can also see a graphical view of all brokers, addresses, queues and their consumers using the **Diagram** tab.
             `);
    }
    configureHelp.$inject = ['helpRegistry', '$templateCache'];

})(Artemis || (Artemis = {}));