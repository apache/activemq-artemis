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
 /// <reference path="tree.component.ts"/>
var Artemis;
(function (Artemis) {
    Artemis._module.component('artemisBrokerDiagram', {
        template:
            `<div class="container-topology">
                <h1>Broker Diagram
                    <button type="button" class="btn btn-link jvm-title-popover"
                              uib-popover-template="'diagram-instructions.html'" popover-placement="bottom-left"
                              popover-title="Instructions" popover-trigger="'outsideClick'">
                        <span class="pficon pficon-help"></span>
                    </button>
                </h1>
                <!-- Inhibit the context menu of pf-topology for the its items -->
                <style type="text/css">pf-topology .popup { visibility: hidden; }</style>
                <pf-topology items="$ctrl.data.items" relations="$ctrl.data.relations" kinds="$ctrl.kinds" icons="$ctrl.data.icons" nodes="$ctrl.nodes" item-selected="$ctrl.itemSelected(item)" search-text="searchText" show-labels="$ctrl.showLabels" tooltip-function="$ctrl.tooltip(node)" chart-rendered="$ctrl.chartRendered(vertices, added)">

                <label style="margin-right: 1em">Show labels:
                    <input type="checkbox" ng-model="$ctrl.showLabels">
                </label>
                 <label style="margin-right: 1em" ng-show="$ctrl.cntAddresses">Show addresses:
                    <input type="checkbox" ng-model="$ctrl.showAddresses">
                </label>
                <label style="margin-right: 1em" ng-show="$ctrl.cntQueues">Show queues:
                    <input type="checkbox" ng-model="$ctrl.showQueues">
                </label>
                <label style="margin-right: 1em" ng-show="$ctrl.cntInternalAddresses">Show internal addresses:
                    <input type="checkbox" ng-model="$ctrl.showInternalAddresses">
                </label>
                <label style="margin-right: 1em" ng-show="$ctrl.cntInternalQueues">Show internal queues:
                    <input type="checkbox" ng-model="$ctrl.showInternalQueues">
                </label>

                <label style="margin-right: 1em" ng-show="$ctrl.cntPrimaryBrokers && $ctrl.cntBackupBrokers">Show Primary Brokers:
                    <input type="checkbox" ng-model="$ctrl.showPrimaryBrokers">
                </label>
                <label style="margin-right: 1em" ng-show="$ctrl.cntPrimaryBrokers && $ctrl.cntBackupBrokers">Show Backup Brokers:
                    <input type="checkbox" ng-model="$ctrl.showBackupBrokers">
                </label>
                <label style="margin-right: 1em" ng-show="$ctrl.relations.length">Show Connectors:
                    <input type="checkbox" ng-model="$ctrl.showConnectors">
                </label>
                <button type="submit" class="btn btn-primary"
                    ng-click="$ctrl.refresh()">Refresh
                </button>
            </div>
            <div ng-show="$ctrl.showAttributes">
                <pf-table-view
                    config="$ctrl.config"
                    columns="$ctrl.tableColumns"
                    items="$ctrl.attributes">
                </pf-table-view>
            </div>
            <script type="text/ng-template" id="diagram-instructions.html">
            <div>
                <p>
                    This page is a graphical representation of the cluster topology. It will show all the brokers in the cluster
                    as well as well as any Adresses and Queues on the broker the console is connected to.
                </p>
                <p>
                    It is possible to view the attributes of the addresses, queues and connected broker by left clicking
                    on each node.
                </p>
                </div>
            </script>
        `,
        controller: BrokerDiagramController
    })
    .name;


    function BrokerDiagramController($scope, workspace, jolokia, localStorage, artemisMessage, $location, $timeout, $filter) {
        Artemis.log.debug("loaded browse " + Artemis.browseQueueModule);
        var ctrl = this;
        ctrl.index = 0;
        ctrl.showLabels = false;
        ctrl.showAddresses = true;
        ctrl.showQueues = true;
        ctrl.showInternalAddresses = false;
        ctrl.showInternalQueues = false;
        ctrl.showPrimaryBrokers = true;
        ctrl.showBackupBrokers = true;
        ctrl.showConnectors = true;
        ctrl.cntPrimaryBrokers = 0;
        ctrl.cntBackupBrokers = 0;
        ctrl.cntAddresses = 0;
        ctrl.cntInternalAddresses = 0;
        ctrl.cntQueues = 0;
        ctrl.cntInternalQueues = 0;

        function updateAddressKind() {
            if(ctrl.kinds.Address && !ctrl.showAddresses) {
               delete ctrl.kinds.Address;
            } else if (!ctrl.kinds.Address && ctrl.showAddresses) {
                ctrl.kinds.Address = true;
            }
        }
        $scope.$watch('$ctrl.showAddresses', function () {
            updateAddressKind();
        });
        function updateQueueKind() {
            if(ctrl.kinds.Queue && !ctrl.showQueues) {
               delete ctrl.kinds.Queue;
            } else if (!ctrl.kinds.Queue && ctrl.showQueues) {
                ctrl.kinds.Queue = true;
            }
        }
        $scope.$watch('$ctrl.showQueues', function () {
            updateQueueKind();
        });
        function updateInternalAddressKind() {
            if(ctrl.kinds.InternalAddress && !ctrl.showInternalAddresses) {
               delete ctrl.kinds.InternalAddress;
            } else if (!ctrl.kinds.InternalAddress && ctrl.showInternalAddresses) {
                ctrl.kinds.InternalAddress = true;
            }
        }
        $scope.$watch('$ctrl.showInternalAddresses', function () {
            updateInternalAddressKind();
        });
        function updateInternalQueueKind() {
            if(ctrl.kinds.InternalQueue && !ctrl.showInternalQueues) {
               delete ctrl.kinds.InternalQueue;
            } else if (!ctrl.kinds.InternalQueue && ctrl.showInternalQueues) {
                ctrl.kinds.InternalQueue = true;
            }
        }
        $scope.$watch('$ctrl.showInternalQueues', function () {
            updateInternalQueueKind();
        });
        function updateLiveBrokerKind() {
            if(ctrl.kinds.ThisPrimaryBroker && !ctrl.showPrimaryBrokers) {
               delete ctrl.kinds.ThisPrimaryBroker;
            } else if (!ctrl.kinds.ThisPrimaryBroker && ctrl.showPrimaryBrokers) {
                ctrl.kinds.ThisPrimaryBroker = true;
            }
            if(ctrl.kinds.PrimaryBroker && !ctrl.showPrimaryBrokers) {
               delete ctrl.kinds.PrimaryBroker;
            } else if (!ctrl.kinds.PrimaryBroker && ctrl.showPrimaryBrokers) {
                ctrl.kinds.PrimaryBroker = true;
            }
        }
        $scope.$watch('$ctrl.showPrimaryBrokers', function () {
            updateLiveBrokerKind();
        });
        function updateBackupBrokerKind() {
            if(ctrl.kinds.ThisBackupBroker && !ctrl.showBackupBrokers) {
               delete ctrl.kinds.ThisBackupBroker;
            } else if (!ctrl.kinds.ThisBackupBroker && ctrl.showBackupBrokers) {
                ctrl.kinds.ThisBackupBroker = true;
            }
            if(ctrl.kinds.BackupBroker && !ctrl.showBackupBrokers) {
               delete ctrl.kinds.BackupBroker;
            } else if (!ctrl.kinds.BackupBroker && ctrl.showBackupBrokers) {
                ctrl.kinds.BackupBroker = true;
            }
            if(ctrl.kinds.OtherBroker && !ctrl.showBackupBrokers) {
               delete ctrl.kinds.OtherBroker;
            } else if (!ctrl.kinds.OtherBroker && ctrl.showBackupBrokers) {
                ctrl.kinds.OtherBroker = true;
            }
        }
        $scope.$watch('$ctrl.showBackupBrokers', function () {
            updateBackupBrokerKind();
        });
        function updateConnectors() {
            if(!ctrl.showConnectors) {
                ctrl.data.relations = [];
            } else {
                ctrl.data.relations = ctrl.relations;
            }
        }
        $scope.$watch('$ctrl.showConnectors', function () {
            updateConnectors();
        });
        ctrl.datasets = [];
        //icons can be found at https://www.patternfly.org/v3/styles/icons/index.html
        ctrl.serverIcon = "\ue90d"; // pficon-server
        Artemis.log.debug(ctrl.serverIcon);
        ctrl.addressIcon = "";
        ctrl.queueIcon = "";
        ctrl.icons = {
            "ThisPrimaryBroker": {
              "type": "glyph",
              "icon": ctrl.serverIcon,
              "fontfamily": "PatternFlyIcons-webfont"
            },
            "PrimaryBroker": {
              "type": "glyph",
              "icon": ctrl.serverIcon,
              "fontfamily": "PatternFlyIcons-webfont"
            },
            "ThisBackupBroker": {
              "type": "glyph",
              "icon": ctrl.serverIcon,
              "fontfamily": "PatternFlyIcons-webfont"
            },
            "BackupBroker": {
              "type": "glyph",
              "icon": ctrl.serverIcon,
              "fontfamily": "PatternFlyIcons-webfont"
            },
            "OtherBroker": {
              "type": "glyph",
              "icon": ctrl.serverIcon,
              "fontfamily": "PatternFlyIcons-webfont"
            },
            "Address": {
                "type": "glyph",
                "icon": ctrl.addressIcon,
                "fontfamily": "PatternFlyIcons-webfont"
            },
            "InternalAddress": {
                "type": "glyph",
                "icon": ctrl.addressIcon,
                "fontfamily": "PatternFlyIcons-webfont"
            },
            "Queue": {
                "type": "glyph",
                "icon": ctrl.queueIcon,
                "fontfamily": "PatternFlyIcons-webfont"
            },
            "InternalQueue": {
                "type": "glyph",
                "icon": ctrl.queueIcon,
                "fontfamily": "PatternFlyIcons-webfont"
            }
        };

        load();
        function load() {
            ctrl.items = {};

            ctrl.relations = [];

            ctrl.datasets.push({
                "items": ctrl.items,
                "relations": ctrl.relations,
                "icons": ctrl.icons
            });

            Artemis.log.debug("index " + ctrl.index);

            ctrl.data = ctrl.datasets[ctrl.index];

            ctrl.data.url = "fooBar";

            ctrl.kinds = {
                "ThisPrimaryBroker": true,
                "PrimaryBroker": true,
                "ThisBackupBroker": true,
                "BackupBroker": true,
                "OtherBroker": true,
                "Address": true,
                "Queue": true
            };

            ctrl.icons = ctrl.data.icons;

            ctrl.nodes = {
                "ThisPrimaryBroker": {
                     "name": "ThisPrimaryBroker",
                     "enabled": true,
                     "radius": 28,
                     "textX": 0,
                     "textY": 5,
                     "height": 30,
                     "width": 30,
                     "icon": ctrl.icons["ThisPrimaryBroker"].icon,
                     "fontFamily": ctrl.icons["ThisPrimaryBroker"].fontfamily
                   },
                "PrimaryBroker": {
                    "name": "PrimaryBroker",
                    "enabled": true,
                    "radius": 28,
                    "textX": 0,
                    "textY": 5,
                    "height": 30,
                    "width": 30,
                    "icon": ctrl.icons["PrimaryBroker"].icon,
                    "fontFamily": ctrl.icons["PrimaryBroker"].fontfamily
                },
                "ThisBackupBroker": {
                    "name": "ThisBackupBroker",
                    "enabled": true,
                    "radius": 28,
                    "textX": 0,
                    "textY": 5,
                    "height": 30,
                    "width": 30,
                    "icon": ctrl.icons["ThisBackupBroker"].icon,
                    "fontFamily": ctrl.icons["ThisBackupBroker"].fontfamily
                },
                "BackupBroker": {
                    "name": "BackupBroker",
                    "enabled": true,
                    "radius": 28,
                    "textX": 0,
                    "textY": 5,
                    "height": 30,
                    "width": 30,
                    "icon": ctrl.icons["BackupBroker"].icon,
                    "fontFamily": ctrl.icons["BackupBroker"].fontfamily
                },
                "OtherBroker": {
                    "name": "OtherBroker",
                    "enabled": true,
                    "radius": 28,
                    "textX": 0,
                    "textY": 5,
                    "height": 30,
                    "width": 30,
                    "icon": ctrl.icons["OtherBroker"].icon,
                    "fontFamily": ctrl.icons["OtherBroker"].fontfamily
                },
                "Address": {
                    "name": "Address",
                    "enabled": ctrl.showDestinations,
                    "radius": 16,
                    "textX": 0,
                    "textY": 5,
                    "height": 18,
                    "width": 18,
                    "icon": ctrl.icons["Address"].icon,
                    "fontFamily": ctrl.icons["Address"].fontfamily
                },
                "Queue": {
                    "name": "Queue",
                    "enabled": ctrl.showDestinations,
                    "radius": 16,
                    "textX": 0,
                    "textY": 5,
                    "height": 18,
                    "width": 18,
                    "icon": ctrl.icons["Queue"].icon,
                    "fontFamily": ctrl.icons["Queue"].fontfamily
                }
            };

            ctrl.tableColumns = [
                { header: 'attribute', itemField: 'attribute' },
                { header: 'value', itemField: 'value' }
            ];
            ctrl.attributes = [];
            ctrl.config = {
                selectionMatchProp: 'attribute',
                showCheckboxes: false
            };
            ctrl.showAttributes = false;

            updateAddressKind();
            updateQueueKind();
            updateInternalAddressKind();
            updateInternalQueueKind();
            updateLiveBrokerKind();
            updateBackupBrokerKind();
            updateConnectors();

            loadThisBroker();
            Core.$apply($scope);
        }
        ctrl.itemSelected = function(item) {
            ctrl.showAttributes = false;
            ctrl.attributes = [];
            if (!item || !item.mbean) {
                Core.$apply($scope);
                return;
            }
            var atts = jolokia.request({ type: "read", mbean: item.mbean}, {method: "post"});
            var val = atts.value;
            if (val) {
                angular.forEach(val, function (value, key) {
                    attribute = {
                        "attribute": key,
                        "value": value
                    }
                   ctrl.attributes.push(attribute);
                });
            }
            ctrl.showAttributes = true;
            Core.$apply($scope);
        }
        ctrl.tooltip = function (node) {
            var status = [
                'Name: ' + node.item.name,
                'Type: ' + node.item.brokerKind
            ];
            return status;
        }
        ctrl.chartRendered = function (vertices, added) {
            // Inhibit the dblclick handler of pf-topology for the its items.
            added.each(function (d) { d.url = "javascript:void(0)"; });
        }
        ctrl.refresh = function () {
            ctrl.datasets = [];
            load();
        }

        function loadThisBroker() {
            var mBean = Artemis.getBrokerMBean(workspace, jolokia);
            var atts = jolokia.request({ type: "read", mbean: mBean}, {method: "post"});
            var val = atts.value;
            var details = Core.parseMBean(mBean);

            var cntPrimaryBrokers = 0;
            var cntBackupBrokers = 0;

            if (details) {
                var properties = details['attributes'];
                Artemis.log.debug("Got broker: " + mBean + " properties: " + angular.toJson(properties, true));
                if (properties) {
                    var brokerAddress = properties["broker"] || "unknown";
                    var brokerName = artemisJmxDomain + ":broker=" + brokerAddress;
                    var backupRes = jolokia.request({ type: "read", mbean: mBean, attribute: "Backup"}, {method: "get"});

                    var isBackup = backupRes.value;
                    var nodeId = val["NodeID"];
                    var response = jolokia.request({ type: 'exec', mbean: mBean, operation: 'listNetworkTopology()' }, Core.onSuccess(null));
                    var responseValue = response.value;
                    var remoteBrokers = angular.fromJson(responseValue);
                    var thisBroker = remoteBrokers.find(broker => broker.nodeID == nodeId);
                    if(!thisBroker) {
                        // use the broker-name when nothing else is available
                        thisBroker = {
                            backup: isBackup ? properties.broker.replace(/["]+/g, "") : undefined,
                            primary: isBackup ? undefined : properties.broker.replace(/["]+/g, "")
                        };
                        // prevent confusion between this thisBroker and one of the brokers
                        // listed in the connectors-list that we expand below
                        val.Connectors = [];
                    }
                    if (thisBroker.primary) {
                        ctrl.items[thisBroker.primary] = {
                            "name": thisBroker.primary.replace(/:6161[67]$/, ""),
                            "kind": isBackup ? "PrimaryBroker" : "ThisPrimaryBroker",
                            "brokerKind": "primary",
                            "status": "broker",
                            "display_kind": "Server",
                            "mbean": isBackup ? undefined : mBean
                        }
                        cntPrimaryBrokers += 1;
                    }
                    if (thisBroker.backup) {
                        ctrl.items[thisBroker.backup] = {
                            "name": thisBroker.backup.replace(/:6161[67]$/, ""),
                            "kind": isBackup ? "ThisBackupBroker" : "BackupBroker",
                            "brokerKind": "backup",
                            "status": "broker",
                            "display_kind": "Server",
                            "mbean": isBackup ? mBean : undefined
                        };
                        cntBackupBrokers += 1;
                    }
                    if (thisBroker.primary && thisBroker.backup) {
                        ctrl.relations.push({
                            "source": thisBroker.primary,
                            "target": thisBroker.backup
                        });
                    }
                    createAddresses(mBean, thisBroker.primary)
                }

                angular.forEach(remoteBrokers, function (remoteBroker) {
                    if (nodeId != remoteBroker.nodeID) {
                       if (remoteBroker.primary) {
                          ctrl.items[remoteBroker.primary] = {
                              "name": remoteBroker.primary.replace(/:6161[67]$/, ""),
                              "kind": "PrimaryBroker",
                              "brokerKind": "primary",
                              "status": "broker",
                              "display_kind": "Server"
                          };
                          cntPrimaryBrokers += 1;
                          //if we arent a backup then connect to it as we are in the cluster
                          if(!isBackup) {}
                              ctrl.relations.push({
                                  "source": thisBroker.primary,
                                  "target": remoteBroker.primary
                              });
                          }
                          if (remoteBroker.backup) {
                              ctrl.items[remoteBroker.backup] = {
                                  "name": remoteBroker.backup.replace(/:6161[67]$/, ""),
                                  "kind": "BackupBroker",
                                  "brokerKind": "backup",
                                  "status": "broker",
                                  "display_kind": "Server"
                              };
                              cntBackupBrokers += 1;
                              ctrl.relations.push({
                                 "source": remoteBroker.backup,
                                 "target": remoteBroker.primary
                              });
                          }
                    }
                });

                angular.forEach(val.Connectors, function (connector) {
                    // each connector entry is like: [connectorname, connectorfactoryclassname, properties]
                    var nodeId = connector[2].host + ":" + connector[2].port;
                    if (ctrl.items[nodeId]) {
                       // already connected to this one
                       return;
                    }
                    ctrl.items[nodeId] = {
                        "name": nodeId.replace(/:6161[67]$/, ""),
                        "kind": "OtherBroker",
                        "brokerKind": "backup",
                        "status": "broker",
                        "display_kind": "Server"
                      };
                    cntBackupBrokers += 1;
                });
            }

            // reduce the checkbox-list by updating their visibility
            ctrl.cntPrimaryBrokers = cntPrimaryBrokers;
            ctrl.cntBackupBrokers = cntBackupBrokers;
            ctrl.cntAddresses = val.AddressNames.filter(name => !isInternalName(name, 0)).length;
            ctrl.cntInternalAddresses = val.AddressNames.filter(name => isInternalName(name, 0)).length;
            ctrl.cntQueues = val.QueueNames.filter(name => !isInternalName(name, 0)).length;
            ctrl.cntInternalQueues = val.QueueNames.filter(name => isInternalName(name, 0)).length;
        }

        function isInternalName(name, start=1) {
            // starts at position 1 when the name is surrounded with quotes
            return name.startsWith("$", start) || name.startsWith("notif", start);
        }

        function createAddresses(brokerMBean, brokerId) {
           jolokia.search(brokerMBean + ",component=addresses,*", Core.onSuccess(function (response) {
              angular.forEach(response, function (objectName) {
                 var details = Core.parseMBean(objectName);
                 if (details) {
                    var properties = details['attributes'];
                    if (properties) {
                        if (!properties.subcomponent) {

                           Artemis.log.debug("Got Address: " + objectName + " properties: " + angular.toJson(properties, true));
                           addressKind = isInternalName(properties.address) ? "InternalAddress" : "Address";
                           ctrl.items[properties.address] = {
                               "name": properties.address.replace(/["]+/g, ""),
                               "kind": addressKind,
                               "brokerKind": "address",
                               "status": "Valid",
                               "display_kind": "Server",
                               "mbean": objectName
                           }
                           ctrl.relations.push({
                               "source": brokerId,
                               "target": properties.address
                           });
                        }
                        if (properties.queue) {
                            Artemis.log.debug("Got Queue: " + objectName + " properties: " + angular.toJson(properties, true));
                            queueKind = isInternalName(properties.queue) ? "InternalQueue" : "Queue";
                            ctrl.items["queue." + properties.queue] = {
                               "name": properties.queue.replace(/["]+/g, ""),
                               "kind": queueKind,
                               "brokerKind": "queue",
                               "status": "Valid",
                               "display_kind": "Service",
                               "mbean": objectName
                           }
                           ctrl.relations.push({
                               "source": properties.address,
                               "target": "queue." + properties.queue
                           });
                        }
                    }
                 }
              });
           }));
        }
    }
    BrokerDiagramController.$inject = ['$scope', 'workspace', 'jolokia', 'localStorage', 'artemisMessage', '$location', '$timeout', '$filter'];

})(Artemis || (Artemis = {}));
