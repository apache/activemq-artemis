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
    Artemis.log.debug("loading producers");
    Artemis._module.component('artemisProducers', {
        template:
            `<h1>Browse Producers
                <button type="button" class="btn btn-link jvm-title-popover"
                          uib-popover-template="'producers-instructions.html'" popover-placement="bottom-left"
                          popover-title="Instructions" popover-trigger="'outsideClick'">
                    <span class="pficon pficon-help"></span>
                </button>
            </h1>
             <div ng-include="'plugin/artemistoolbar.html'"></div>
             <pf-table-view config="$ctrl.tableConfig"
                            dt-options="$ctrl.dtOptions"
                            columns="$ctrl.tableColumns"
                            items="$ctrl.producers">
             </pf-table-view>
             <div ng-include="'plugin/artemispagination.html'"></div>
             <script type="text/ng-template" id="producers-anchor-column-template">
                <a href="#" ng-click="$ctrl.handleColAction(key, item)">{{value}}</a>
             </script>
             <script type="text/ng-template" id="producers-instructions.html">
             <div>
                <p>
                    This page allows you to browse all producers currently open on the broker. These can be narrowed down
                    by specifying a filter and also sorted using the sort function in the toolbar. To execute a query
                    click on the <span class="fa fa-search"></span> button.
                </p>
                <p>
                    You can navigate to the producers  session by clicking on the <code>Session</code> field.
                  </p>
                  <p>
                    Note that each page is loaded in from the broker when navigating to a new page or when a query is executed.
                  </p>
                </div>
             </script>
             `,
              controller: ProducersController
    })
    .name;


    function ProducersController($scope, workspace, jolokia, localStorage, artemisMessage, $location, $timeout, $filter, pagination, artemisProducer, artemisAddress, artemisSession) {
        var ctrl = this;
        ctrl.pagination = pagination;
        ctrl.pagination.reset();
        var mbean = Artemis.getBrokerMBean(workspace, jolokia);
        ctrl.allProducers = [];
        ctrl.producers = [];
        ctrl.pageNumber = 1;
        ctrl.workspace = workspace;
        ctrl.refreshed = false;
        ctrl.dtOptions = {
           // turn of ordering as we do it ourselves
           ordering: false,
           columns: [
                {name: "ID", visible: true},
                {name: "Name", visible: true},
                {name: "Session", visible: true},
                {name: "Client ID", visible: true},
                {name: "Protocol", visible: true},
                {name: "User", visible: true},
                {name: "Validated User", visible: false},
                {name: "Address", visible: true},
                {name: "Remote Address", visible: true},
                {name: "Local Address", visible: true},
                {name: "Messages Sent", visible: false},
                {name: "Messages Sent Size", visible: false},
                {name: "Last Produced Message ID", visible: false}
           ]
          };

        Artemis.log.debug('localStorage: producersColumnDefs =', localStorage.getItem('producersColumnDefs'));
        if (localStorage.getItem('producersColumnDefs')) {
            loadedDefs = JSON.parse(localStorage.getItem('producersColumnDefs'));
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
            localStorage.setItem('producersColumnDefs', JSON.stringify(attributes));
        }
        ctrl.filter = {
            fieldOptions: [
                {id: 'id', name: 'ID'},
                {id: 'name', name: 'Name'},
                {id: 'session', name: 'Session'},
                {id: 'clientID', name: 'Client ID'},
                {id: 'user', name: 'User'},
                {id: 'validatedUser', name: 'Validated User'},
                {id: 'address', name: 'Address'},
                {id: 'protocol', name: 'Protocol'},
                {id: 'localAddress', name: 'Local Address'},
                {id: 'remoteAddress', name: 'Remote Address'}
            ],
            operationOptions: [
                {id: 'EQUALS', name: 'Equals'},
                {id: 'NOT_EQUALS', name: 'Not Equals'},
                {id: 'CONTAINS', name: 'Contains'}
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
        ctrl.tableConfig = {
            selectionMatchProp: 'id',
            showCheckboxes: false
        };
        ctrl.tableColumns = [
            { header: 'ID', itemField: 'id' },
            { header: 'Name', itemField: 'name' },
            { header: 'Session', itemField: 'session' , htmlTemplate: 'producers-anchor-column-template', colActionFn: (item) => selectSession(item.idx) },
            { header: 'Client ID', itemField: 'clientID' },
            { header: 'Protocol', itemField: 'protocol' },
            { header: 'User', itemField: 'user' },
            { header: 'Validated User', name: 'validatedUser'},
            { header: 'Address', itemField: 'addressName' , htmlTemplate: 'producers-anchor-column-template', colActionFn: (item) => selectAddress(item.idx) },
            { header: 'Remote Address', itemField: 'remoteAddress' },
            { header: 'Local Address', itemField: 'localAddress' },
            { header: 'Messages Sent', itemField: 'msgSent'},
            { header: 'Messages Sent Size', itemField: 'msgSizeSent'},
            { header: 'Last Produced Message ID', itemField: 'lastProducedMessageID'}
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
            ctrl.filter.text.fieldText = "Filter Field..";
            ctrl.filter.text.operationText = "Operation..";
            ctrl.filter.text.sortOrderText = "ascending";
            ctrl.filter.text.sortByText = "ID";
            ctrl.refreshed = true;
            artemisProducer.producer = null;
            ctrl.pagination.load();
        };

        selectAddress = function (idx) {
            var address = ctrl.producers[idx].address;
            Artemis.log.debug("navigating to address:" + address)
            artemisAddress.address = { address: address };
            $location.path("artemis/artemisAddresses");
        };

        selectSession = function (idx) {
            var session = ctrl.producers[idx].session;
            Artemis.log.debug("navigating to session:" + session)
            artemisSession.session = { session: session };
            $location.path("artemis/artemisSessions");
        };

        if (artemisProducer.producer) {
            Artemis.log.debug("navigating to producer = " + artemisProducer.producer.sessionID);
            ctrl.filter.values.field = ctrl.filter.fieldOptions[1].id;
            ctrl.filter.values.operation = ctrl.filter.operationOptions[0].id;
            ctrl.filter.values.value = artemisProducer.producer.sessionID;
            artemisProducer.producer = null;
        }

        ctrl.loadOperation = function () {
            if (mbean) {
                var method = 'listProducers(java.lang.String, int, int)';
                var producerFilter = {
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
                jolokia.request({ type: 'exec', mbean: mbean, operation: method, arguments: [JSON.stringify(producerFilter), ctrl.pagination.pageNumber, ctrl.pagination.pageSize] }, Core.onSuccess(populateTable, { error: onError }));
            }
        };

        ctrl.pagination.setOperation(ctrl.loadOperation);

        function onError(response) {
            Core.notification("error", "could not invoke list sessions" + response.error);
            $scope.workspace.selectParentNode();
        };

        function populateTable(response) {
            var data = JSON.parse(response.value);
            ctrl.producers = [];
            angular.forEach(data["data"], function (value, idx) {
                value.idx = idx;
                value.addressName = value.address;
                ctrl.producers.push(value);
            });
            ctrl.pagination.page(data["count"]);
            allProducers = ctrl.producers;
            ctrl.producers = allProducers;
            Core.$apply($scope);
        }

        ctrl.pagination.load();
    }
    ProducersController.$inject = ['$scope', 'workspace', 'jolokia', 'localStorage', 'artemisMessage', '$location', '$timeout', '$filter', 'pagination', 'artemisProducer', 'artemisAddress', 'artemisSession'];


})(Artemis || (Artemis = {}));