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
    Artemis.log.debug("loading connections");
    Artemis._module.component('artemisConnections', {
        template:
            `
            <h1>Browse Connections
                <button type="button" class="btn btn-link jvm-title-popover"
                          uib-popover-template="'connections-instructions.html'" popover-placement="bottom-left"
                          popover-title="Instructions" popover-trigger="'outsideClick'">
                    <span class="pficon pficon-help"></span>
                </button>
            </h1>
            <div ng-include="'plugin/artemistoolbar.html'"></div>
            <pf-table-view config="$ctrl.tableConfig"
                            dt-options="$ctrl.dtOptions"
                            columns="$ctrl.tableColumns"
                            action-buttons="$ctrl.tableActionButtons"
                            items="$ctrl.connections">
            </pf-table-view>
            <div ng-include="'plugin/artemispagination.html'"></div>
            <div hawtio-confirm-dialog="$ctrl.closeDialog" title="Close Connection?"
                ok-button-text="Close"
                cancel-button-text="Cancel"
                on-ok="$ctrl.closeConnection()">
                 <div class="dialog-body">
                    <p class="alert alert-warning">
                        <span class="pficon pficon-warning-triangle-o"></span>
                        You are about to close the selected connection: {{$ctrl.connectionToDelete}}
                        <p>Are you sure you want to continue.</p>
                    </p>
                 </div>
            </div>
            <script type="text/ng-template" id="connections-anchor-column-template">
               <a href="#" ng-click="$ctrl.handleColAction(key, item)">{{value}}</a>
            </script>
            <script type="text/ng-template" id="connections-instructions.html">
            <div>
                <p>
                    This page allows you to browse all connections currently connected to the broker, including client, cluster
                    and bridge connections. These can be narrowed down by specifying a filter and also sorted using the sort
                    function in the toolbar. To execute a query click on the <span class="fa fa-search"></span> button.
                </p>
                <p>
                    Connections can be closed by using the <code>close</code> button under the <code>Actions</code> column and you can
                    navigate to the connections sessions open by clicking on the <code>Session Count</code> field.
                  </p>
                  <p>
                    Note that each page is loaded in from the broker when navigating to a new page or when a query is executed.
                  </p>
                </div>
            </script>
            `,
              controller: ConnectionsController
    })
    .name;


    function ConnectionsController($scope, workspace, jolokia, localStorage, artemisMessage, $location, $timeout, $filter, pagination, artemisConnection, artemisSession) {
        var ctrl = this;
        ctrl.pagination = pagination;
        ctrl.pagination.reset();
        var mbean = Artemis.getBrokerMBean(workspace, jolokia);
        ctrl.allConnections = [];
        ctrl.connections = [];
        ctrl.pageNumber = 1;
        ctrl.workspace = workspace;
        ctrl.refreshed = false;
        ctrl.connectionToDelete = '';
        ctrl.closeDialog = false;
        ctrl.dtOptions = {
           // turn of ordering as we do it ourselves
           ordering: false,
           columns: [
                {name: "ID", visible: true},
                {name: "Client ID", visible: true},
                {name: "Users", visible: true},
                {name: "Protocol", visible: true},
                {name: "Session Count", visible: true},
                {name: "Remote Address", visible: true},
                {name: "Local Address", visible: true},
                {name: "Session ID", visible: true},
                {name: "Creation Time", visible: true}
           ]
        };

        Artemis.log.debug('localStorage: connectionsColumnDefs =', localStorage.getItem('connectionsColumnDefs'));
        if (localStorage.getItem('connectionsColumnDefs')) {
            loadedDefs = JSON.parse(localStorage.getItem('connectionsColumnDefs'));
            //sanity check to make sure columns havent been added
            if(loadedDefs.length === ctrl.dtOptions.columns.length) {
                ctrl.dtOptions.columns = loadedDefs;
            }
            Artemis.log.debug('loaded' + ctrl.dtOptions.columns);
        }

        ctrl.updateColumns = function () {
            var attributes = [];
            ctrl.dtOptions.columns.forEach(function (column) {
                attributes.push({name: column.name, visible: column.visible});
            });
            Artemis.log.debug("saving columns " + JSON.stringify(attributes));
            localStorage.setItem('connectionsColumnDefs', JSON.stringify(attributes));
        }

        ctrl.filter = {
            fieldOptions: [
                {id: 'connectionID', name: 'ID'},
                {id: 'clientID', name: 'Client ID'},
                {id: 'users', name: 'Users'},
                {id: 'protocol', name: 'Protocol'},
                {id: 'sessionCount', name: 'Session Count'},
                {id: 'remoteAddress', name: 'Remote Address'},
                {id: 'localAddress', name: 'Local Address'},
                {id: 'sessionID', name: 'Session ID'}
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
                sortField: "connectionID"
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
            name: 'Close',
            title: 'Close the Connection',
            actionFn: openCloseDialog
          }
        ];

        ctrl.tableConfig = {
            selectionMatchProp: 'connectionID',
            showCheckboxes: false
        };
        ctrl.tableColumns = [
            { header: 'ID', itemField: 'connectionID' },
            { header: 'Client ID', itemField: 'clientID' },
            { header: 'Users', itemField: 'users' },
            { header: 'protocol', itemField: 'protocol' },
            { header: 'Session Count', itemField: 'sessionCount', htmlTemplate: 'connections-anchor-column-template', colActionFn: (item) => selectSessions(item.idx) },

            { header: 'Remote Address', itemField: 'remoteAddress' },
            { header: 'Local Address', itemField: 'localAddress' },
            { header: 'Creation Time', itemField: 'creationTime' }
        ];

        selectSessions = function (idx) {
            var connection = ctrl.connections[idx].connectionID;
            Artemis.log.debug("navigating to connection:" + connection)
            artemisConnection.connection = { connectionID: connection };
            $location.path("artemis/artemisSessions");
        };

        if (artemisSession.session) {
            Artemis.log.debug("navigating to session = " + artemisSession.session.connectionID);
            ctrl.filter.values.field = ctrl.filter.fieldOptions[0].id;
            ctrl.filter.values.operation = ctrl.filter.operationOptions[0].id;
            ctrl.filter.values.value = artemisSession.session.connectionID;
            artemisSession.session = null;
        }

        ctrl.refresh = function () {
            ctrl.refreshed = true;
            ctrl.pagination.load();
        };
        ctrl.reset = function () {
            ctrl.filter.values.field = "";
            ctrl.filter.values.operation = "";
            ctrl.filter.values.value = "";
            ctrl.filter.sortOrder = "asc";
            ctrl.filter.sortField = "connectionID";
            ctrl.filter.text.fieldText = "Filter Field..";
            ctrl.filter.text.operationText = "Operation..";
            ctrl.filter.text.sortOrderText = "ascending";
            ctrl.filter.text.sortByText = "ID";
            ctrl.refreshed = true;
            artemisSession.session = null;
            ctrl.pagination.load();
        };

        ctrl.loadOperation = function () {
            if (mbean) {
                var method = 'listConnections(java.lang.String, int, int)';
                var connectionsFilter = {
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
                Artemis.log.info(JSON.stringify(connectionsFilter));
                jolokia.request({ type: 'exec', mbean: mbean, operation: method, arguments: [JSON.stringify(connectionsFilter), ctrl.pagination.pageNumber, ctrl.pagination.pageSize] }, Core.onSuccess(populateTable, { error: onError }));
            }
        };

        function openCloseDialog(action, item) {
            ctrl.connectionToDelete = item.connectionID;
            ctrl.closeDialog = true;
        }

        ctrl.closeConnection = function () {
           Artemis.log.debug("closing connection: " + ctrl.connectionToDelete);
              if (mbean) {
                  jolokia.request({ type: 'exec',
                     mbean: mbean,
                     operation: 'closeConnectionWithID(java.lang.String)',
                     arguments: [ctrl.connectionToDelete] },
                     Core.onSuccess(ctrl.pagination.load(), { error: function (response) {
                        Core.defaultJolokiaErrorHandler("Could not close connection: " + response);
                 }}));
           }
        };

        pagination.setOperation(ctrl.loadOperation);

        function onError(response) {
            Core.notification("error", "could not invoke list connections" + response.error);
            $scope.workspace.selectParentNode();
        };

        function populateTable(response) {
            var data = JSON.parse(response.value);
            ctrl.connections = [];
            angular.forEach(data["data"], function (value, idx) {
                value.idx = idx;
                ctrl.connections.push(value);
            });
            ctrl.pagination.page(data["count"]);
            allConnections = ctrl.connections;
            ctrl.connections = allConnections;
            Core.$apply($scope);
        }

        ctrl.pagination.load();
    }
    ConnectionsController.$inject = ['$scope', 'workspace', 'jolokia', 'localStorage', 'artemisMessage', '$location', '$timeout', '$filter', 'pagination', 'artemisConnection', 'artemisSession'];


})(Artemis || (Artemis = {}));