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
var ARTEMIS;
(function (ARTEMIS) {
    ARTEMIS.jmsHeaderSchema = {
        definitions: {
            headers: {
                properties: {
                    JMSCorrelationID: {
                        type: "java.lang.String"
                    },
                    JMSDeliveryMode: {
                        "type": "string",
                        "enum": [
                            "PERSISTENT",
                            "NON_PERSISTENT"
                        ]
                    },
                    JMSDestination: {
                        type: "javax.jms.Destination"
                    },
                    JMSExpiration: {
                        type: "long"
                    },
                    JMSPriority: {
                        type: "int"
                    },
                    JMSReplyTo: {
                        type: "javax.jms.Destination"
                    },
                    JMSType: {
                        type: "java.lang.String"
                    },
                    JMSXGroupId: {
                        type: "java.lang.String"
                    },
                    _AMQ_SCHED_DELIVERY: {
                        type: "java.lang.String"
                    }
                }
            },
            "javax.jms.Destination": {
                type: "java.lang.String"
            }
        }
    };
})(ARTEMIS || (ARTEMIS = {}));
//# sourceMappingURL=jmsHeaderSchema.js.map