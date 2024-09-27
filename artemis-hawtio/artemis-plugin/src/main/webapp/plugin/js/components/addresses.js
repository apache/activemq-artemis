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
    //Artemis.log.debug("loading addresses");
    Artemis._module.component('artemisAddresses', {
        template:
            `<h1>Browse Addresses
                <button type="button" class="btn btn-link jvm-title-popover"
                          uib-popover-template="'addresses-instructions.html'" popover-placement="bottom-left"
                          popover-title="Instructions" popover-trigger="'outsideClick'">
                    <span class="pficon pficon-help"></span>
                </button>
            </h1>
             <div ng-include="'plugin/artemistoolbar.html'"></div>
             <pf-table-view config="$ctrl.tableConfig"
                            dt-options="$ctrl.dtOptions"
                            columns="$ctrl.tableColumns"
                            action-buttons="$ctrl.tableActionButtons"
                            items="$ctrl.addresses">
             </pf-table-view>
             <div ng-include="'plugin/artemispagination.html'"></div>
             <script type="text/ng-template" id="addresses-anchor-column-template">
                <a href="#" ng-click="$ctrl.handleColAction(key, item)">{{value}}</a>
             </script>
             <script type="text/ng-template" id="addresses-instructions.html">
             <div>
                <p>
                    This page allows you to browse all address on the broker. These can be narrowed down
                    by specifying a filter and also sorted using the sort function in the toolbar. To execute a query
                    click on the <span class="fa fa-search"></span> button.
                </p>
                <p>
                    You can also navigate directly to the JMX attributes and operations tabs by using the  <code>attributes</code>
                    and <code>operations</code> button under the <code>Actions</code> column.You can navigate to the
                    addresses queues by clicking on the <code>Queue Count</code> field.
                  </p>
                  <p>
                    Note that each page is loaded in from the broker when navigating to a new page or when a query is executed.
                  </p>
                </div>
             </script>
             `,
              controller: AddressesController
    })
    .name;


    function AddressesController($scope, workspace, jolokia, localStorage, artemisMessage, $location, $timeout, $filter, pagination, artemisAddress) {
        var ctrl = this;
        ctrl.pagination = pagination;
        ctrl.pagination.reset();
        var mbean = Artemis.getBrokerMBean(workspace, jolokia);
        ctrl.allAddresses = [];
        ctrl.addresses = [];
        ctrl.workspace = workspace;
        ctrl.refreshed = false;
        ctrl.dtOptions = {
           // turn of ordering as we do it ourselves
           ordering: false,
           columns: [
                {name: "ID", visible: true},
                {name: "Name", visible: true},
                {name: "Internal", visible: false},
                {name: "Routing Types", visible: true},
                {name: "Queue Count", visible: true}
           ]
        };

        Artemis.log.debug('sessionStorage: addressColumnDefs =', localStorage.getItem('addressColumnDefs'));
        if (localStorage.getItem('addressColumnDefs')) {
            loadedDefs = JSON.parse(localStorage.getItem('addressColumnDefs'));
            //sanity check to make sure columns havent been added
            if(loadedDefs.length === ctrl.dtOptions.columns.length) {
                ctrl.dtOptions.columns = loadedDefs;
            }

        }

        ctrl.updateColumns = function () {
            var attributes = [];
            ctrl.dtOptions.columns.forEach(function (column) {
                attributes.push({name: column.name, visible: column.visible});
            });
            Artemis.log.debug("saving columns " + JSON.stringify(attributes));
            localStorage.setItem('addressColumnDefs', JSON.stringify(attributes));
        }

        ctrl.filter = {
            fieldOptions: [
                {id: 'id', name: 'ID'},
                {id: 'name', name: 'Name'},
                {id: 'internal', name: 'Internal'},
                {id: 'routingTypes', name: 'Routing Types'},
                {id: 'queueCount', name: 'Queue Count'}
            ],
            operationOptions: [
                {id: 'EQUALS', name: 'Equals'},
                {id: 'NOT_EQUALS', name: 'Not Equals'},
                {id: 'CONTAINS', name: 'Contains'},
                {id: 'NOT_CONTAINS', name: 'Does Not Contain'},
                {id: 'GREATER_THAN', name: 'Greater Than'},
                {id: 'LESS_THAN', name: 'Less Than'}
            ],
            sortOptions: [
                {id: 'asc', name: 'ascending'},
                {id: 'desc', name: 'descending'}
            ],
            values: {
                field: "",
                operation: "",
                value: "",
                sortOrder: "asc",
                sortField: "id"
            },
            text: {
                fieldText: "Filter Field..",
                operationText: "Operation..",
                sortOrderText: "ascending",
                sortByText: "ID"
            }
        };

        ctrl.tableActionButtons = [
            {
                name: 'attributes',
                title: 'Navigate to attributes',
                actionFn: navigateToAddressAtts
            },
            {
               name: 'operations',
               title: 'navigate to operations',
               actionFn: navigateToAddressOps
            }
        ];
        ctrl.tableConfig = {
            selectionMatchProp: 'id',
            showCheckboxes: false
        };
        ctrl.tableColumns = [
            { header: 'ID', itemField: 'id' },
            { header: 'Name', itemField: 'name' },
            { header: 'Internal', itemField: 'internal' },
            { header: 'Routing Types', itemField: 'routingTypes' },
            { header: 'Queue Count', itemField: 'queueCount' , htmlTemplate: 'addresses-anchor-column-template', colActionFn: (item) => selectQueues(item.idx) }
        ];

        ctrl.refresh = function () {
            ctrl.refreshed = true;
            ctrl.pagination.load();
        };
        ctrl.reset = function () {
            ctrl.filter.values.field = "";
            ctrl.filter.values.operation = "";
            ctrl.filter.values.value = "";
            ctrl.filter.sortOrder = "asc";
            ctrl.filter.sortField = "id";
            ctrl.refreshed = true;
            artemisAddress.address = null;
            ctrl.pagination.load();
        };

        if (artemisAddress.address) {
            Artemis.log.debug("navigating to address = " + artemisAddress.address.address);
            ctrl.filter.values.field = ctrl.filter.fieldOptions[1].id;
            ctrl.filter.values.operation = ctrl.filter.operationOptions[0].id;
            ctrl.filter.values.value = artemisAddress.address.address;
            artemisAddress.address = null;
        }

        selectQueues = function (idx) {
            var address = ctrl.addresses[idx].name;
            Artemis.log.debug("navigating to queues:" + address)
            artemisAddress.address = { address: address };
            $location.path("artemis/artemisQueues");
        };

        function navigateToAddressAtts(action, item) {
            $location.path("artemis/attributes").search({"tab": "artemis", "nid": getAddressNid(item.name, $location)});
        };
        function navigateToAddressOps(action, item) {
            $location.path("artemis/operations").search({"tab": "artemis", "nid": getAddressNid(item.name, $location)});
        };
        function getAddressNid(address, $location) {
            var rootNID = getRootNid($location);
            var targetNID = rootNID + "addresses-" + address;
            Artemis.log.debug("targetNID=" + targetNID);
            return targetNID;
        }
        function getRootNid($location) {
            var currentNid = $location.search()['nid'];
            Artemis.log.debug("current nid=" + currentNid);
            var firstDash = currentNid.indexOf('-');
            var secondDash = currentNid.indexOf('-', firstDash + 1);
            var thirdDash = currentNid.indexOf('-', secondDash + 1);
            if (thirdDash < 0) {
                return currentNid + "-";
            }
            var rootNID = currentNid.substring(0, thirdDash + 1);
            return rootNID;
        }
        ctrl.loadOperation = function () {
            if (mbean) {
                var method = 'listAddresses(java.lang.String, int, int)';
                var addressFilter = {
                    field: ctrl.filter.values.field,
                    operation: ctrl.filter.values.operation,
                    value: ctrl.filter.values.value,
                    sortOrder: ctrl.filter.values.sortOrder,
                    sortField: ctrl.filter.values.sortField
                };

                if (ctrl.refreshed == true) {
                    ctrl.pagination.reset();
                    ctrl.refreshed = false;
                }
                jolokia.request({ type: 'exec', mbean: mbean, operation: method, arguments: [JSON.stringify(addressFilter), ctrl.pagination.pageNumber, ctrl.pagination.pageSize] }, Core.onSuccess(populateTable, { error: onError }));
            }
        };

        ctrl.pagination.setOperation(ctrl.loadOperation);

        function onError(response) {
            Core.notification("error", "could not invoke list sessions" + response.error);
            $scope.workspace.selectParentNode();
        };

        function populateTable(response) {
            var data = JSON.parse(response.value);
            ctrl.addresses = [];
            angular.forEach(data["data"], function (value, idx) {
                value.idx = idx;
                ctrl.addresses.push(value);
            });
            ctrl.pagination.page(data["count"]);
            allAddresses = ctrl.addresses;
            ctrl.addresses = allAddresses;
            Core.$apply($scope);
        }

        ctrl.pagination.load();
    }
    AddressesController.$inject = ['$scope', 'workspace', 'jolokia', 'localStorage', 'artemisMessage', '$location', '$timeout', '$filter', 'pagination', 'artemisAddress'];


})(Artemis || (Artemis = {}));