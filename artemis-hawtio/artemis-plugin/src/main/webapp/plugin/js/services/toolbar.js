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
    Artemis._module.run(configureToolbar);

       function configureToolbar($templateCache) {
           $templateCache.put('plugin/artemistoolbar.html',
                `
                <div class="row toolbar-pf table-view-pf-toolbar" id="toolbar1">
                    <form class="toolbar-pf-actions">
                        <div class="form-group toolbar-pf-filter">
                            <div class="input-group">
                                <div class="input-group-btn">
                                    <button id="filter.values.field" type="button" class="btn btn-default dropdown-toggle" id="filter" data-toggle="dropdown" aria-haspopup="true" aria-expanded="false">{{$ctrl.filter.text.fieldText}} <span class="caret"></span></button>
                                    <ul class="dropdown-menu">
                                        <li ng-repeat="option in $ctrl.filter.fieldOptions"
                                            id="option.id" ng-click="$ctrl.filter.values.field = option.id;$ctrl.filter.text.fieldText = option.name">{{ option.name }}</ul>
                                    </ul>
                                </div>
                                <div class="input-group-btn">
                                      <button type="button" class="btn btn-default dropdown-toggle" id="filter" data-toggle="dropdown" aria-haspopup="true" aria-expanded="false">{{$ctrl.filter.text.operationText}}<span class="caret"></span></button>
                                      <ul class="dropdown-menu">
                                        <li ng-repeat="option in $ctrl.filter.operationOptions"
                                              id="option.id" ng-click="$ctrl.filter.values.operation = option.id;$ctrl.filter.text.operationText = option.name">{{ option.name }}</ul>
                                      </ul>
                                </div>
                                <input type="text" class="form-control" ng-model="$ctrl.filter.values.value" placeholder="Value" autocomplete="off" id="filterInput">
                                <div class="input-group-btn">
                                      <button type="button" class="btn btn-default dropdown-toggle" id="filter" data-toggle="dropdown" aria-haspopup="true" aria-expanded="false">{{$ctrl.filter.text.sortOrderText}}<span class="caret"></span></button>
                                      <ul class="dropdown-menu">
                                        <li ng-repeat="option in $ctrl.filter.sortOptions"
                                              id="option.id" ng-click="$ctrl.filter.values.sortOrder = option.id;$ctrl.filter.text.sortOrderText = option.name">{{ option.name }}</ul>
                                      </ul>
                                </div>
                                <div class="input-group-btn">
                                      <button type="button" class="btn btn-default dropdown-toggle" id="filter" data-toggle="dropdown" aria-haspopup="true" aria-expanded="false">{{$ctrl.filter.text.sortByText}}<span class="caret"></span></button>
                                      <ul class="dropdown-menu">
                                        <li ng-repeat="option in $ctrl.filter.fieldOptions"
                                              id="option.id" ng-click="$ctrl.filter.values.sortColumn = option.id;$ctrl.filter.text.sortByText = option.name">{{ option.name }}</ul>
                                      </ul>
                                </div>
                                <div class="input-group-btn">
                                    <button class="btn btn-link btn-find" ng-click="$ctrl.refresh()" type="button">
                                        &nbsp;&nbsp;<span class="fa fa-search"></span>&nbsp;&nbsp;
                                    </button>
                                </div>
                                <div class="input-group-btn">
                                    <button class="btn btn-default primary-action ng-binding ng-scope"
                                        type="button"
                                        title=""
                                        ng-click="$ctrl.reset()">Reset
                                    </button>
                                </div>
                            </div>
                    </form>
                 </div>
               `
           )
           $templateCache.put('plugin/artemismessagetoolbar.html',
                `
                <div class="row toolbar-pf table-view-pf-toolbar" id="toolbar1">
                    <div class="col-sm-20">
                        <form class="toolbar-pf-actions">
                            <div class="form-group toolbar-pf-filter">
                                <div class="input-group">
                                    <input type="text" class="form-control" ng-model="$ctrl.filter.values.value" placeholder="Filter..." autocomplete="off" id="filterInput">
                                    <div class="input-group-btn">
                                        <button class="btn btn-link btn-find" ng-click="$ctrl.refresh()" type="button">
                                            &nbsp;&nbsp;<span class="fa fa-search"></span>&nbsp;&nbsp;
                                        </button>
                                    </div>
                                </div>
                            </div>
                            <div class="form-group">
                                    <button class="btn btn-default primary-action ng-binding ng-scope"
                                        type="button"
                                        title=""
                                        ng-click="$ctrl.reset()">Reset
                                    </button>
                                    <button ng-show="$ctrl.dlq" class="btn btn-default primary-action ng-binding ng-scope"
                                        type="button"
                                        title=""
                                        ng-click="$ctrl.retry()">Retry Messages
                                    </button>
                                    <button ng-show="$ctrl.dlq" class="btn btn-default primary-action ng-binding ng-scope"
                                        type="button"
                                        title=""
                                        ng-click="$ctrl.retry()">Move Messages
                                    </button>
                            </div>
                        </form>
                    </div>
                </div>
                `
          )
       }
       configureToolbar.$inject = ['$templateCache'];



})(Artemis || (Artemis = {}));