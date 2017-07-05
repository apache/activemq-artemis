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
/// <reference path="artemisPlugin.ts"/>
var ARTEMIS;
(function (ARTEMIS) {
    ARTEMIS.module.controller("ARTEMIS.TreeHeaderController", ["$scope", function ($scope) {
        $scope.expandAll = function () {
            Tree.expandAll("#artemistree");
        };
        $scope.contractAll = function () {
            Tree.contractAll("#artemistree");
        };
    }]);
    ARTEMIS.module.controller("ARTEMIS.TreeController", ["$scope", "$location", "workspace", "localStorage", function ($scope, $location, workspace, localStorage) {
        var artemisJmxDomain = localStorage['artemisJmxDomain'] || "org.apache.activemq.artemis";
        ARTEMIS.log.info("init tree " + artemisJmxDomain);
        $scope.$on("$routeChangeSuccess", function (event, current, previous) {
            // lets do this asynchronously to avoid Error: $digest already in progress
            setTimeout(updateSelectionFromURL, 50);
        });
        $scope.$watch('workspace.tree', function () {
            reloadTree();
        });
        $scope.$on('jmxTreeUpdated', function () {
            reloadTree();
        });
        function reloadTree() {
            ARTEMIS.log.info("workspace tree has changed, lets reload the artemis tree");
            var children = [];
            var tree = workspace.tree;

            ARTEMIS.log.info("tree="+tree);
            if (tree) {
                var domainName = artemisJmxDomain;
                var folder = tree.get(domainName);

                ARTEMIS.log.info("folder="+folder);
                if (folder) {
                    children = folder.children;
                }
                var treeElement = $("#artemistree");
                Jmx.enableTree($scope, $location, workspace, treeElement, children, true);
                // lets do this asynchronously to avoid Error: $digest already in progress
                setTimeout(updateSelectionFromURL, 50);
            }
        }
        function updateSelectionFromURL() {
            Jmx.updateTreeSelectionFromURLAndAutoSelect($location, $("#artemistree"), function (first) {
                // use function to auto select the queue folder on the 1st broker
                var jms = first.getChildren()[0];
                ARTEMIS.log.info("%%%%%%" + jms);
                var queues = jms.getChildren()[0];
                if (queues && queues.data.title === 'Queue') {
                    first = queues;
                    first.expand(true);
                    return first;
                }
                return null;
            }, true);
        }
    }]);
})(ARTEMIS || (ARTEMIS = {}));