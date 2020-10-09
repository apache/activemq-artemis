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
    Artemis._module.component('artemisTreeHeader', {
            template:
                `<div class="tree-nav-sidebar-header">
                  <form role="form" class="search-pf has-button">
                    <div class="form-group has-clear">
                      <div class="search-pf-input-group">
                        <label for="input-search" class="sr-only">Search Tree:</label>
                        <input id="input-search" type="search" class="form-control" placeholder="Search tree:"
                          ng-model="$ctrl.filter">
                        <button type="button" class="clear" aria-hidden="true"
                          ng-hide="$ctrl.filter.length === 0"
                          ng-click="$ctrl.filter = ''">
                          <span class="pficon pficon-close"></span>
                        </button>
                      </div>
                    </div>
                    <div class="form-group tree-nav-buttons">
                      <span class="badge" ng-class="{positive: $ctrl.result.length > 0}"
                        ng-show="$ctrl.filter.length > 0">
                        {{$ctrl.result.length}}
                      </span>
                      <i class="fa fa-plus-square-o" title="Expand All" ng-click="$ctrl.expandAll()"></i>
                      <i class="fa fa-minus-square-o" title="Collapse All" ng-click="$ctrl.contractAll()"></i>
                    </div>
                  </form>
                </div>
            `,
            controller: TreeHeaderController
        })
        .component('artemisTree', {
              template:
              `<div class="tree-nav-sidebar-content">
                  <div id="artemistree" class="treeview-pf-hover treeview-pf-select"></div>
              </div>
              `,
              controller: TreeController
        })
        .name;
    treeElementId = '#artemistree';
    Artemis.log.debug("loaded tree" + Artemis.treeModule);


    function TreeHeaderController($scope, $element) {
        'ngInject';
        Artemis.log.debug("TreeHeaderController ");
        this.$scope = $scope;
        this.$element = $element;
        this.filter = '';
        this.result = [];
        // it's not possible to declare classes to the component host tag in AngularJS
        $element.addClass('tree-nav-sidebar-header');

        TreeHeaderController.prototype.$onInit = function () {
        Artemis.log.debug("TreeHeaderController init");
            var _this = this;
            this.$scope.$watch(angular.bind(this, function () { return _this.filter; }), function (filter, previous) {
                if (filter !== previous) {
                    _this.search(filter);
                }
            });
        };

        TreeHeaderController.prototype.search = function (filter) {
                Artemis.log.debug("TreeHeaderController search");
            var _this = this;
            var doSearch = function (filter) {
                var result = _this.tree().search(filter, {
                    ignoreCase: true,
                    exactMatch: false,
                    revealResults: true
                });
                _this.result.length = 0;
                (_a = _this.result).push.apply(_a, result);
                Core.$apply(_this.$scope);
                var _a;
            };
            _.debounce(doSearch, 300, { leading: false, trailing: true })(filter);
        };

        TreeHeaderController.prototype.tree = function () {
                Artemis.log.debug("TreeHeaderController tree");
            return $(treeElementId).treeview(true);
        };

        TreeHeaderController.prototype.expandAll = function () {
                Artemis.log.debug("TreeHeaderController expand");
            return this.tree()
                .expandNode(this.tree().getNodes(), { levels: HawtioTree.getMaxTreeLevel(this.tree()), silent: true });
        };

        TreeHeaderController.prototype.contractAll = function () {
                Artemis.log.debug("TreeHeaderController contract");
            return this.tree()
                .collapseNode(this.tree().getNodes(), { ignoreChildren: true, silent: true });
        };
    }
    TreeHeaderController.$inject = ['$scope', '$element'];

    function TreeController($scope, $location, workspace, $element) {
        'ngInject';
        this.$scope = $scope;
        this.$location = $location;
        this.workspace = workspace;
        this.$element = $element;
        // it's not possible to declare classes to the component host tag in AngularJS
        $element.addClass('tree-nav-sidebar-content');
        Artemis.log.debug("TreeController ");
        var artemisJmxDomain = localStorage['artemisJmxDomain'] || "org.apache.activemq.artemis";
        TreeController.prototype.$onInit = function () {
        Artemis.log.debug("TreeController onInit");
            var _this = this;
            this.$scope.$on('$destroy', function () { return _this.removeTree(); });
            this.$scope.$on('$routeChangeStart', function () { return Jmx.updateTreeSelectionFromURL(_this.$location, $(treeElementId)); });
            this.$scope.$on('jmxTreeUpdated', function () { return _this.populateTree(); });
            this.populateTree();
        };
        TreeController.prototype.updateSelectionFromURL = function () {
            Jmx.updateTreeSelectionFromURLAndAutoSelect(this.$location, $(treeElementId), function (first) {
                if (first.children == null) {
                    return null;
                }
                // use function to auto select the queue folder on the 1st broker
                var queues = first.children[0];
                if (queues && queues.text === 'Queue') {
                    return queues;
                }
                return null;
            }, true);
        };
        TreeController.prototype.populateTree = function () {
            var _this = this;
            var children = [];
            var tree = this.workspace.tree;
            Artemis.log.debug("tree= "+tree);
            if (tree) {
                var domainName = artemisJmxDomain;
                var folder = tree.get(domainName);

                if (folder) {
                    children = folder.children;
                }
                angular.forEach(children,  function(child) {
                    Artemis.log.debug("child=" + child.text + " " + child.id);
                });
                var treeElement = $("#artemistree");
                this.removeTree();
                Jmx.enableTree(this.$scope, this.$location, this.workspace, $(treeElementId), children);
                this.updateSelectionFromURL();
            }
        };
        TreeController.prototype.removeTree = function () {
            var tree = $(treeElementId).treeview(true);
            // There is no exposed API to check whether the tree has already been initialized,
            // so let's just check if the methods are presents
            if (tree.clearSearch) {
                tree.clearSearch();
                // Bootstrap tree view leaks the node elements into the data structure
                // so let's clean this up when the user leaves the view
                var cleanTreeFolder_1 = function (node) {
                    delete node['$el'];
                    if (node.nodes)
                        node.nodes.forEach(cleanTreeFolder_1);
                };
                cleanTreeFolder_1(this.workspace.tree);
                // Then call the tree clean-up method
                tree.remove();
            }
        }
    }
    TreeController.$inject = ['$scope', '$location', 'workspace', '$element'];

})(Artemis || (Artemis = {}));
