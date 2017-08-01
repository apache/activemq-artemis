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
/**
 * @module ARTEMIS
 */
var ARTEMIS = (function(ARTEMIS) {
   ARTEMIS.BrokerDiagramController = function ($scope, $compile, $location, localStorage, ARTEMISService, jolokia, workspace, $routeParams) {

      Fabric.initScope($scope, $location, jolokia, workspace);
      var artemisJmxDomain = localStorage['artemisJmxDomain'] || "org.apache.activemq.artemis";

      $scope.selectedNode = null;
      var defaultFlags = {
         panel: true,
         popup: false,
         label: true,
         group: false,
         profile: false,
         slave: false,
         broker: true,
         network: true,
         container: false,
         address: true,
         queue: true,
         consumer: true,
         producer: true
      };
      $scope.viewSettings = {};
      $scope.shapeSize = {
         broker: 20,
         queue: 14,
         address: 14
      };
      var redrawGraph = Core.throttled(doRedrawGraph, 1000);
      var graphBuilder = new ForceGraph.GraphBuilder();
      Core.bindModelToSearchParam($scope, $location, "searchFilter", "q", "");
      angular.forEach(defaultFlags, function (defaultValue, key) {
         var modelName = "viewSettings." + key;
         // bind model values to search params...
         function currentValue() {
            var answer = $location.search()[paramName] || defaultValue;
            return answer === "false" ? false : answer;
         }

         var paramName = key;
         var value = currentValue();
         Core.pathSet($scope, modelName, value);
         $scope.$watch(modelName, function () {
            var current = Core.pathGet($scope, modelName);
            var old = currentValue();
            if (current !== old) {
               var defaultValue = defaultFlags[key];
               if (current !== defaultValue) {
                  if (!current) {
                     current = "false";
                  }
                  $location.search(paramName, current);
               }
               else {
                  $location.search(paramName, null);
               }
            }
            redrawGraph();
         });
      });
      $scope.connectToBroker = function () {
         var selectedNode = $scope.selectedNode;
         if (selectedNode) {
            var container = selectedNode["brokerContainer"] || selectedNode;
            connectToBroker(container, selectedNode["brokerName"]);
         }
      };
      function connectToBroker(container, brokerName, postfix) {
         if (postfix === void 0) {
            postfix = null;
         }

         var view = "/jmx/attributes?tab=artemis";
         if (!postfix) {
            if (brokerName) {
               // lets default to the broker view
               postfix = "nid=root-" + artemisJmxDomain + "-Broker-" + brokerName;
            }
         }
         if (postfix) {
            view += "&" + postfix;
         }
         var path = Core.url("/#" + view);
         window.open(path, '_destination');
         window.focus();
      }

      $scope.connectToDestination = function () {
         var selectedNode = $scope.selectedNode;
         if (selectedNode) {
            var container = selectedNode["brokerContainer"] || selectedNode;
            var brokerName = selectedNode["brokerName"];
            var destinationType = selectedNode["destinationType"] || selectedNode["typeLabel"];
            var destinationName = selectedNode["destinationName"];
            var postfix = null;
            if (brokerName && destinationType && destinationName) {
               postfix = "nid=root-" + artemisJmxDomain + "-Broker-" + brokerName + "-" + destinationType + "-" + destinationName;
            }
            connectToBroker(container, brokerName, postfix);
         }
      };
      $scope.$on('$destroy', function (event) {
         stopOldJolokia();
      });
      function stopOldJolokia() {
         var oldJolokia = $scope.selectedNodeJolokia;
         if (oldJolokia && oldJolokia !== jolokia) {
            oldJolokia.stop();
         }
      }

      $scope.$watch("selectedNode", function (newValue, oldValue) {
         // lets cancel any previously registered thingy
         if ($scope.unregisterFn) {
            $scope.unregisterFn();
            $scope.unregisterFn = null;
         }
         var node = $scope.selectedNode;
         if (node) {
            var mbean = node.objectName;
            var brokerContainer = node.brokerContainer || {};
            var nodeJolokia = node.jolokia || brokerContainer.jolokia || jolokia;
            if (nodeJolokia !== $scope.selectedNodeJolokia) {
               stopOldJolokia();
               $scope.selectedNodeJolokia = nodeJolokia;
               if (nodeJolokia !== jolokia) {
                  var rate = Core.parseIntValue(localStorage['updateRate'] || "2000", "update rate");
                  if (rate) {
                     nodeJolokia.start(rate);
                  }
               }
            }
            var dummyResponse = {value: node.panelProperties || {}};
            if (mbean && nodeJolokia) {
               ARTEMIS.log.debug("reading ", mbean, " on remote container");
               $scope.unregisterFn = Core.register(nodeJolokia, $scope, {
                  type: 'read',
                  mbean: mbean
               }, onSuccess(renderNodeAttributes, {
                  error: function (response) {
                     // probably we've got a wrong mbean name?
                     // so lets render at least
                     renderNodeAttributes(dummyResponse);
                     Core.defaultJolokiaErrorHandler(response);
                  }
               }));
            }
            else {
               ARTEMIS.log.debug("no mbean or jolokia available, using dummy response");
               renderNodeAttributes(dummyResponse);
            }
         }
      });
      function getDestinationTypeName(attributes) {
         var prefix = attributes["DestinationTemporary"] ? "Temporary " : "";
         return prefix + (attributes["DestinationTopic"] ? "Topic" : "Queue");
      }

      var ignoreNodeAttributes = ["Broker", "BrokerId", "BrokerName", "Connection", "DestinationName", "DestinationQueue", "DestinationTemporary", "DestinationTopic",];
      var ignoreNodeAttributesByType = {
         producer: ["Producer", "ProducerId"],
         queue: ["Name", "MessageGroups", "MessageGroupType", "Subscriptions"],
         topic: ["Name", "Subscriptions"],
         broker: ["DataDirectory", "DurableTopicSubscriptions", "DynamicDestinationProducers", "InactiveDurableToppicSubscribers"]
      };
      var brokerShowProperties = ["Version", "Started"];
      var onlyShowAttributesByType = {
         broker: brokerShowProperties,
         brokerSlave: brokerShowProperties
      };

      function renderNodeAttributes(response) {
         var properties = [];
         if (response) {
            var value = response.value || {};
            $scope.selectedNodeAttributes = value;
            var selectedNode = $scope.selectedNode || {};
            var brokerContainer = selectedNode['brokerContainer'] || {};
            var nodeType = selectedNode["type"];
            var brokerName = selectedNode["brokerName"];
            var containerId = selectedNode["container"] || brokerContainer["container"];
            var group = selectedNode["group"] || brokerContainer["group"];
            var jolokiaUrl = selectedNode["jolokiaUrl"] || brokerContainer["jolokiaUrl"];
            var profile = selectedNode["profile"] || brokerContainer["profile"];
            var version = selectedNode["version"] || brokerContainer["version"];
            var isBroker = nodeType && nodeType.startsWith("broker");
            var ignoreKeys = ignoreNodeAttributes.concat(ignoreNodeAttributesByType[nodeType] || []);
            var onlyShowKeys = onlyShowAttributesByType[nodeType];
            angular.forEach(value, function (v, k) {
               if (onlyShowKeys ? onlyShowKeys.indexOf(k) >= 0 : ignoreKeys.indexOf(k) < 0) {
                  var formattedValue = Core.humanizeValueHtml(v);
                  properties.push({key: Core.humanizeValue(k), value: formattedValue});
               }
            });
            properties = properties.sortBy("key");
            var brokerProperty = null;
            if (brokerName) {
               var brokerHtml = '<a target="broker" ng-click="connectToBroker()">' + '<img title="Apache Artemis" src="img/icons/messagebroker.svg"> ' + brokerName + '</a>';
               if (version && profile) {
                  var brokerLink = Fabric.brokerConfigLink(workspace, jolokia, localStorage, version, profile, brokerName);
                  if (brokerLink) {
                     brokerHtml += ' <a title="configuration settings" target="brokerConfig" href="' + brokerLink + '"><i class="icon-tasks"></i></a>';
                  }
               }
               var html = $compile(brokerHtml)($scope);
               brokerProperty = {key: "Broker", value: html};
               if (!isBroker) {
                  properties.splice(0, 0, brokerProperty);
               }
            }
            if (containerId) {
               //var containerModel = "selectedNode" + (selectedNode['brokerContainer'] ? ".brokerContainer" : "");
               properties.splice(0, 0, {
                  key: "Container",
                  value: $compile('<div fabric-container-link="' + selectedNode['container'] + '"></div>')($scope)
               });
            }
            var destinationName = value["DestinationName"] || selectedNode["destinationName"];
            if (destinationName && (nodeType !== "queue" && nodeType !== "topic")) {
               var destinationTypeName = getDestinationTypeName(value);
               var html = createDestinationLink(destinationName, destinationTypeName);
               properties.splice(0, 0, {key: destinationTypeName, value: html});
            }
            var typeLabel = selectedNode["typeLabel"];
            var name = selectedNode["name"] || selectedNode["id"] || selectedNode['objectName'];
            if (typeLabel) {
               var html = name;
               if (nodeType === "queue" || nodeType === "topic") {
                  html = createDestinationLink(name, nodeType);
               }
               var typeProperty = {key: typeLabel, value: html};
               if (isBroker && brokerProperty) {
                  typeProperty = brokerProperty;
               }
               properties.splice(0, 0, typeProperty);
            }
         }
         $scope.selectedNodeProperties = properties;
         Core.$apply($scope);
      }

      /**
       * Generates the HTML for a link to the destination
       */
      function createDestinationLink(destinationName, destinationType) {
         if (destinationType === void 0) {
            destinationType = "queue";
         }
         return $compile('<a target="destination" title="' + destinationName + '" ng-click="connectToDestination()">' + destinationName + '</a>')($scope);
      }

      $scope.$watch("searchFilter", function (newValue, oldValue) {
         redrawGraph();
      });
      // lets just use the current stuff from the workspace
      $scope.$watch('workspace.tree', function () {
         redrawGraph();
      });
      $scope.$on('jmxTreeUpdated', function () {
         redrawGraph();
      });

      function onBrokerData(response) {
         if (response) {
            var responseJson = angular.toJson(response.value);
            if ($scope.responseJson === responseJson) {
               return;
            }
            $scope.responseJson = responseJson;
            $scope.brokers = response.value;
            doRedrawGraph();
         }
      }

      function redrawLocalBroker() {
         var container = {
            jolokia: jolokia
         };
         var containerId = "local";
         $scope.activeContainers = {
            containerId: container
         };
         var brokers = [];
         jolokia.search(artemisJmxDomain + ":broker=*", onSuccess(function (response) {
            angular.forEach(response, function (objectName) {
               var atts = ARTEMISService.artemisConsole.getServerAttributes(jolokia, objectName);
               var val = atts.value;
               var details = Core.parseMBean(objectName);
               if (details) {
                  var properties = details['attributes'];
                  ARTEMIS.log.info("Got broker: " + objectName + " on container: " + containerId + " properties: " + angular.toJson(properties, true));
                  if (properties) {
                     var master = true;
                     var brokerId = properties["broker"] || "unknown";
                     var nodeId = val["NodeID"];
                     var theBroker = {
                        brokerId: brokerId,
                        nodeId: nodeId
                     };
                     brokers.push(theBroker);
                     if ($scope.viewSettings.broker) {
                        var broker = getOrAddBroker(master, brokerId, nodeId, containerId, container, properties);
                     }
                  }
               }
            });

            redrawActiveContainers(brokers);
         }));
      }

      function redrawActiveContainers(brokers) {
         // TODO delete any nodes from dead containers in containersToDelete
         angular.forEach($scope.activeContainers, function (container, id) {
            var containerJolokia = container.jolokia;
            if (containerJolokia) {
               onContainerJolokia(containerJolokia, container, id, brokers);
            }
            else {
               Fabric.containerJolokia(jolokia, id, function (containerJolokia) {
                  return onContainerJolokia(containerJolokia, container, id, brokers);
               });
            }
         });
         $scope.graph = graphBuilder.buildGraph();
         Core.$apply($scope);
      }

      function doRedrawGraph() {
         graphBuilder = new ForceGraph.GraphBuilder();
         redrawLocalBroker();
      }

      function brokerNameMarkup(brokerName) {
         return brokerName ? "<p></p>broker: " + brokerName + "</p>" : "";
      }

      function onContainerJolokia(containerJolokia, container, id, brokers) {
         function createQueues(brokers) {
            if ($scope.viewSettings.queue) {
               containerJolokia.search(artemisJmxDomain + ":*,subcomponent=queues", onSuccess(function (response) {
                  angular.forEach(response, function (objectName) {
                     var details = Core.parseMBean(objectName);
                     if (details) {
                        var properties = details['attributes'];
                        if (properties) {
                           configureDestinationProperties(properties);
                           var brokerName = properties.broker;
                           var addressName = properties.address;
                           var typeName = "queue";
                           var queueName = properties.queue;
                           var routingType = properties["routing-type"];
                           var destination = getOrAddQueue(properties, typeName, routingType, queueName, addressName, brokerName);
                        }
                     }
                  });
                  graphModelUpdated();
                  createConsumersAndNetwork(brokers);
               }));
            } else {
               createConsumersAndNetwork(brokers);
            }
         }

         function createAddresses(brokers) {
            if ($scope.viewSettings.address) {
               containerJolokia.search(artemisJmxDomain + ":*,component=addresses", onSuccess(function (response) {
                  angular.forEach(response, function (objectName) {
                     var details = Core.parseMBean(objectName);
                     if (details) {
                        var properties = details['attributes'];
                        if (properties) {
                           var brokerName = properties.broker;
                           var typeName = "address";
                           var addressName = properties.address;
                           var destination = getOrAddAddress(properties, typeName, addressName, brokerName);
                        }
                     }
                  });
                  createQueues(brokers);
                  graphModelUpdated();
               }));
            } else {
               createQueues(brokers);
            }
         }

         function createConsumersAndNetwork(brokers) {
            angular.forEach(brokers, function (broker) {
               mBean = artemisJmxDomain + ":broker=" + broker.brokerId;
               // find consumers
               if ($scope.viewSettings.consumer) {
                  ARTEMISService.artemisConsole.getConsumers(mBean, containerJolokia, onSuccess(function (properties) {
                     consumers = properties.value;
                     ARTEMIS.log.info(consumers);
                     angular.forEach(angular.fromJson(consumers), function (consumer) {
                        if (consumer) {

                           configureDestinationProperties(consumer);
                           var consumerId = consumer.sessionID + "-" + consumer.consumerID;
                           if (consumerId) {
                              var queueName = consumer.queueName;
                              var consumerNode = getOrAddNode("consumer", consumerId, consumer, function () {
                                 return {
                                    typeLabel: "Consumer",
                                    brokerContainer: container,
                                    //objectName: "null",
                                    jolokia: containerJolokia,
                                    popup: {
                                       title: "Consumer: " + consumerId,
                                       content: "<p>client: " + (consumer.connectionID || "") + "</p> " + brokerNameMarkup(broker.brokerId)
                                    }
                                 };
                              });
                              addLinkIds("queue:\"" + queueName + "\"", consumerNode["id"], "consumer");
                           }
                        }
                     });
                     graphModelUpdated();
                  }));
               }


               // find networks of brokers
               if ($scope.viewSettings.network && $scope.viewSettings.broker) {

                  ARTEMISService.artemisConsole.getRemoteBrokers(mBean, containerJolokia, onSuccess(function (properties) {
                     remoteBrokers = properties.value;

                     ARTEMIS.log.info("remoteBrokers=" + angular.toJson(remoteBrokers))
                     angular.forEach(angular.fromJson(remoteBrokers), function (remoteBroker) {
                        if (remoteBroker) {
                           ARTEMIS.log.info("remote=" + angular.toJson(remoteBroker))
                           if (broker.nodeId != remoteBroker.nodeID) {
                              getOrAddBroker(true, "\"" + remoteBroker.live + "\"", remoteBroker.nodeID, "remote", null, properties);
                              addLinkIds("broker:" + broker.brokerId, "broker:" + "\"" + remoteBroker.live + "\"", "network");

                              var backup = remoteBroker.backup;
                              if (backup) {
                                 getOrAddBroker(false, "\"" + backup + "\"", remoteBroker.nodeID, "remote", null, properties);
                                 addLinkIds("broker:" + "\"" + remoteBroker.live + "\"", "broker:" + "\"" + backup + "\"", "network");
                              }
                           }
                           else {
                              var backup = remoteBroker.backup;
                              if (backup) {
                                 getOrAddBroker(false, "\"" + remoteBroker.backup + "\"", remoteBroker.nodeID, "remote", null, properties);
                                 addLinkIds("broker:" + broker.brokerId, "broker:" + "\"" + remoteBroker.backup + "\"", "network");
                              }
                           }
                        }
                     });
                     graphModelUpdated();
                  }));
               }
            });
         }

         if (containerJolokia) {
            container.jolokia = containerJolokia;
            function getOrAddQueue(properties, typeName, routingType, queueName, addressName, brokerName) {
               var queue = getOrAddNode(typeName.toLowerCase(), queueName, properties, function () {
                  var objectName = "";
                  if (addressName) {
                     objectName = artemisJmxDomain + ":broker=" + brokerName + ",component=addresses,address=" + addressName + ",subcomponent=queues,routing-type=" + routingType + ",queue=" + queueName;
                     
                  }
                  ARTEMIS.log.info(objectName);
                  var answer = {
                     typeLabel: typeName,
                     brokerContainer: container,
                     objectName: objectName,
                     jolokia: containerJolokia,
                     popup: {
                        title: "queue: " + queueName,
                        content: "address:" + addressName
                     }
                  };
                  if (!addressName) {
                     containerJolokia.search(artemisJmxDomain + ":broker=" + brokerName + ",component=addresses,address=" + addressName + ",subcomponent=queues,routing-type=" + routingType + ",queue=" + queueName + ",*", onSuccess(function (response) {
                        if (response && response.length) {
                           answer.objectName = response[0];
                        }
                     }));
                  }
                  return answer;
               });
               if (queue && $scope.viewSettings.broker && addressName) {
                  addLinkIds("address:" + addressName, queue["id"], "queue");
               }
               return queue;
            }

            function getOrAddAddress(properties, typeName, destinationName, brokerName) {
               var destination = getOrAddNode(typeName.toLowerCase(), destinationName, properties, function () {
                  var objectName = "";
                  if (brokerName) {
                     objectName = artemisJmxDomain + ":broker=" + brokerName + ",component=addresses,address=" + destinationName;
                  }
                  var answer = {
                     typeLabel: typeName,
                     brokerContainer: container,
                     objectName: objectName,
                     jolokia: containerJolokia,
                     popup: {
                        title: typeName + ": " + destinationName,
                        content: brokerNameMarkup(brokerName)
                     }
                  };
                  if (!brokerName) {
                     containerJolokia.search(artemisJmxDomain + ":broker=" + brokerName + ",component=addresses,address=" + destinationName + ",*", onSuccess(function (response) {
                        if (response && response.length) {
                           answer.objectName = response[0];
                        }
                     }));
                  }
                  return answer;
               });
               if (destination && $scope.viewSettings.broker && brokerName) {
                  addLinkIds(brokerNodeId(brokerName), destination["id"], "address");
               }
               return destination;
            }

            createAddresses(brokers);
         }
      }

      function graphModelUpdated() {
         $scope.graph = graphBuilder.buildGraph();
         Core.$apply($scope);
      }

      function getOrAddBroker(master, brokerId, nodeId, containerId, container, brokerStatus) {
         var broker = null;
         var brokerFlag = master ? $scope.viewSettings.broker : $scope.viewSettings.slave;
         if (brokerFlag) {
            broker = getOrAddNode("broker", brokerId, brokerStatus, function () {
               return {
                  type: master ? "broker" : "brokerSlave",
                  typeLabel: master ? "Broker" : "Slave Broker",
                  popup: {
                     title: (master ? "Master" : "Slave") + " Broker: " + brokerId,
                     content: "<p>Container: " + containerId + "</p> Node ID: " + nodeId
                  }
               };
            });
            if (!broker['objectName']) {
               // lets try guess the mbean name
               broker['objectName'] = artemisJmxDomain + ":broker=" + brokerId;
               ARTEMIS.log.debug("Guessed broker mbean: " + broker['objectName']);
            }
            if (!broker['brokerContainer'] && container) {
               broker['brokerContainer'] = container;
            }
            if (!broker['nodeID']) {
               broker['nodeID'] = nodeId;
            }
         }
         return broker;
      }

      function getOrAddNode(typeName, id, properties, createFn) {
         var node = null;
         if (id) {
            var nodeId = typeName + ":" + id;
            node = graphBuilder.getNode(nodeId);
            if (!node) {
               var nodeValues = createFn();
               node = angular.copy(properties);

               angular.forEach(nodeValues, function (value, key) {
                  return node[key] = value;
               });
               node['id'] = nodeId;
               if (!node['type']) {
                  node['type'] = typeName;
               }
               if (!node['name']) {
                  node['name'] = id;
               }
               if (node) {
                  var size = $scope.shapeSize[typeName];
                  if (size && !node['size']) {
                     node['size'] = size;
                  }
                  if (!node['summary']) {
                     node['summary'] = node['popup'] || "";
                  }
                  if (!$scope.viewSettings.popup) {
                     delete node['popup'];
                  }
                  if (!$scope.viewSettings.label) {
                     delete node['name'];
                  }
                  // lets not add nodes which are defined as being disabled
                  var enabled = $scope.viewSettings[typeName];
                  if (enabled || !angular.isDefined(enabled)) {
                     graphBuilder.addNode(node);
                  }
                  else {
                  }
               }
            }
         }
         return node;
      }

      function addLink(object1, object2, linkType) {
         if (object1 && object2) {
            addLinkIds(object1.id, object2.id, linkType);
         }
      }

      function addLinkIds(id1, id2, linkType) {
         ARTEMIS.log.info("adding " + id1 + " to " + id2 + " " + linkType)
         if (id1 && id2) {
            graphBuilder.addLink(id1, id2, linkType);
         }
      }

      function brokerNodeId(brokerId) {
         return brokerId ? "broker:" + brokerId : null;
      }

      /**
       * Avoid the JMX type property clashing with the ForceGraph type property; used for associating css classes with nodes on the graph
       *
       * @param properties
       */
      function renameTypeProperty(properties) {
         properties.mbeanType = properties['type'];
         delete properties['type'];
      }

      function configureDestinationProperties(properties) {
         renameTypeProperty(properties);
         var destinationType = properties.destinationType || "Queue";
         var typeName = destinationType.toLowerCase();
         properties.isQueue = !typeName.startsWith("t");
         properties['destType'] = typeName;
      }
   };

   return ARTEMIS;
} (ARTEMIS || {}));