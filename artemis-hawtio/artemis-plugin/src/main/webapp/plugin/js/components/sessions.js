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
    Artemis.log.debug("loading sessions");
    Artemis._module.component('artemisSessions', {
        template:
            `<h1>Browse Sessions
                <button type="button" class="btn btn-link jvm-title-popover"
                          uib-popover-template="'sessions-instructions.html'" popover-placement="bottom-left"
                          popover-title="Instructions" popover-trigger="'outsideClick'">
                    <span class="pficon pficon-help"></span>
                </button>
            </h1>
             <div ng-include="'plugin/artemistoolbar.html'"></div>
             <pf-table-view config="$ctrl.tableConfig"
                            dt-options="$ctrl.dtOptions"
                            columns="$ctrl.tableColumns"
                            action-buttons="$ctrl.tableActionButtons"
                            items="$ctrl.sessions">
             </pf-table-view>
             <div ng-include="'plugin/artemispagination.html'"></div>
             <div hawtio-confirm-dialog="$ctrl.closeDialog" title="Close Session?"
                 ok-button-text="Close"
                 cancel-button-text="Cancel"
                 on-ok="$ctrl.closeSession()">
                 <div class="dialog-body">
                     <p class="alert alert-warning">
                         <span class="pficon pficon-warning-triangle-o"></span>
                         You are about to close the selected session: {{$ctrl.sessionToDelete}}
                         <p>Are you sure you want to continue.</p>
                     </p>
                 </div>
             </div>
             <script type="text/ng-template" id="sessions-anchor-column-template">
                <a href="#" ng-click="$ctrl.handleColAction(key, item)">{{value}}</a>
             </script>
             <script type="text/ng-template" id="sessions-instructions.html">
             <div>
                <p>
                    This page allows you to browse all session currently open on the broker. These can be narrowed down
                    by specifying a filter and also sorted using the sort function in the toolbar. To execute a query
                    click on the <span class="fa fa-search"></span> button.
                </p>
                <p>
                    Sessions can be closed by using the <code>close</code> button under the <code>Actions</code> column and you can
                    navigate to the connection, consumers and producers by clicking on the appropriate field.
                  </p>
                  <p>
                    Note that each page is loaded in from the broker when navigating to a new page or when a query is executed.
                  </p>
                </div>
             </script>
             `,
              controller: SessionsController
    })
    .name;


    function SessionsController($scope, workspace, jolokia, localStorage, artemisMessage, $location, $timeout, $filter, pagination, artemisConnection, artemisSession, artemisConsumer, artemisProducer) {
        var ctrl = this;
        ctrl.pagination = pagination;
        ctrl.pagination.reset();
        var mbean = Artemis.getBrokerMBean(workspace, jolokia);
        ctrl.allSessions = [];
        ctrl.sessions = [];
        ctrl.pageNumber = 1;
        ctrl.workspace = workspace;
        ctrl.refreshed = false;
        ctrl.sessionToDeletesConnection = '';
        ctrl.sessionToDelete = '';
        ctrl.closeDialog = false;
        ctrl.dtOptions = {
           // turn of ordering as we do it ourselves
           ordering: false,
           columns: [
                {name: "ID", visible: true},
                {name: "Connection", visible: true},
                {name: "User", visible: true},
                {name: "Validated User", visible: false},
                {name: "Consumer Count", visible: true},
                {name: "Producer Count", visible: true},
                {name: "Creation Time", visible: true}
             ]
        };

        Artemis.log.debug('localStorage: sessionsColumnDefs =', localStorage.getItem('sessionsColumnDefs'));
        if (localStorage.getItem('sessionsColumnDefs')) {
            loadedDefs = JSON.parse(localStorage.getItem('sessionsColumnDefs'));
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
                localStorage.setItem('sessionsColumnDefs', JSON.stringify(attributes));
        }
        ctrl.filter = {
            fieldOptions: [
                {id: 'id', name: 'ID'},
                {id: 'connectionID', name: 'Connection ID'},
                {id: 'consumerCount', name: 'Consumer Count'},
                {id: 'user', name: 'User'},
                {id: 'validatedUser', name: 'Validated User'},
                {id: 'protocol', name: 'Protocol'},
                {id: 'clientID', name: 'Client ID'},
                {id: 'localAddress', name: 'Local Address'},
                {id: 'remoteAddress', name: 'Remote Address'}
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
            name: 'Close',
            title: 'Close the Session',
            actionFn: openCloseDialog
           }
        ];
        ctrl.tableConfig = {
            selectionMatchProp: 'id',
            showCheckboxes: false
        };
        ctrl.tableColumns = [
            { header: 'ID', itemField: 'id' },
            { header: 'Connection', itemField: 'connectionID', htmlTemplate: 'sessions-anchor-column-template', colActionFn: (item) => selectConnection(item.idx) },
            { header: 'User', itemField: 'user' },
            { header: 'Validated User', itemField: 'validatedUser' },
            { header: 'Consumer Count', itemField: 'consumerCount', htmlTemplate: 'sessions-anchor-column-template', colActionFn: (item) => selectConsumers(item.idx) },
            { header: 'Producer Count', itemField: 'producerCount', htmlTemplate: 'sessions-anchor-column-template', colActionFn: (item) => selectProducers(item.idx) },
            { header: 'Creation Time', itemField: 'creationTime' }
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
            artemisConnection.connection = null;
            artemisSession.session = null;
            artemisConsumer.consumer = null;
            ctrl.pagination.load();
        };

        selectConnection = function (idx) {
            var connection = ctrl.sessions[idx].connectionID;
            Artemis.log.debug("navigating to connection:" + connection)
            artemisSession.session = { connectionID: connection };
            $location.path("artemis/artemisConnections");
        };

        selectConsumers = function (idx) {
            var session = ctrl.sessions[idx].id;
            Artemis.log.debug("navigating to consumers:" + session)
            artemisConsumer.consumer = { sessionID: session };
            $location.path("artemis/artemisConsumers");
        };

        selectProducers = function (idx) {
            var session = ctrl.sessions[idx].id;
            Artemis.log.debug("navigating to producers:" + session)
            artemisProducer.producer = { sessionID: session };
            $location.path("artemis/artemisProducers");
        };

        if (artemisConnection.connection) {
            Artemis.log.debug("navigating to connection = " + artemisConnection.connection.connectionID);
            ctrl.filter.values.field = ctrl.filter.fieldOptions[1].id;
            ctrl.filter.values.operation = ctrl.filter.operationOptions[0].id;
            ctrl.filter.values.value = artemisConnection.connection.connectionID;
            artemisConnection.connection = null;
        }

        if (artemisSession.session) {
            Artemis.log.debug("navigating to session = " + artemisSession.session.session);
            ctrl.filter.values.field = ctrl.filter.fieldOptions[0].id;
            ctrl.filter.values.operation = ctrl.filter.operationOptions[0].id;
            ctrl.filter.values.value = artemisSession.session.session;
            artemisSession.session = null;
        }

        function openCloseDialog(action, item) {
            ctrl.sessionToDelete = item.id;
            ctrl.sessionToDeletesConnection = item.connectionID;
            ctrl.closeDialog = true;
        }

        ctrl.closeSession = function () {
           Artemis.log.debug("closing session: " + ctrl.sessionToDelete);
              if (mbean) {
                  jolokia.request({ type: 'exec',
                     mbean: mbean,
                     operation: 'closeSessionWithID(java.lang.String,java.lang.String)',
                     arguments: [ctrl.sessionToDeletesConnection, ctrl.sessionToDelete] },
                     Core.onSuccess(ctrl.pagination.load(), { error: function (response) {
                        Core.defaultJolokiaErrorHandler("Could not close session: " + response);
                 }}));
           }
        };
        ctrl.loadOperation = function () {
            if (mbean) {
                var method = 'listSessions(java.lang.String, int, int)';
                var sessionsFilter = {
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
                jolokia.request({ type: 'exec', mbean: mbean, operation: method, arguments: [JSON.stringify(sessionsFilter), ctrl.pagination.pageNumber, ctrl.pagination.pageSize] }, Core.onSuccess(populateTable, { error: onError }));
            }
        };

        ctrl.pagination.setOperation(ctrl.loadOperation);

        function onError(response) {
            Core.notification("error", "could not invoke list sessions" + response.error);
            $scope.workspace.selectParentNode();
        };

        function populateTable(response) {
            var data = JSON.parse(response.value);
            ctrl.sessions = [];
            angular.forEach(data["data"], function (value, idx) {
                value.idx = idx;
                ctrl.sessions.push(value);
            });
            ctrl.pagination.page(data["count"]);
            allSessions = ctrl.sessions;
            ctrl.sessions = allSessions;
            Core.$apply($scope);
        }

        ctrl.pagination.load();
    }
    SessionsController.$inject = ['$scope', 'workspace', 'jolokia', 'localStorage', 'artemisMessage', '$location', '$timeout', '$filter', 'pagination', 'artemisConnection', 'artemisSession', 'artemisConsumer', 'artemisProducer'];


})(Artemis || (Artemis = {}));