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
    Artemis._module.component('artemisConsumers', {
        template:
            `<h1>Browse Consumers
                <button type="button" class="btn btn-link jvm-title-popover"
                          uib-popover-template="'consumers-instructions.html'" popover-placement="bottom-left"
                          popover-title="Instructions" popover-trigger="'outsideClick'">
                    <span class="pficon pficon-help"></span>
                </button>
            </h1>
             <div ng-include="'plugin/artemistoolbar.html'"></div>
             <pf-table-view config="$ctrl.tableConfig"
                            dt-options="$ctrl.dtOptions"
                            columns="$ctrl.tableColumns"
                            action-buttons="$ctrl.tableActionButtons"
                            items="$ctrl.consumers">
             </pf-table-view>
             <div ng-include="'plugin/artemispagination.html'"></div>
             <div hawtio-confirm-dialog="$ctrl.closeDialog" title="Close Consumer?"
                  ok-button-text="Close"
                  cancel-button-text="Cancel"
                  on-ok="$ctrl.closeConsumer()">
                  <div class="dialog-body">
                      <p class="alert alert-warning">
                          <span class="pficon pficon-warning-triangle-o"></span>
                          You are about to close the selected consumer: {{$ctrl.consumerToDelete}}
                          <p>Are you sure you want to continue.</p>
                      </p>
                  </div>
             </div>
             <script type="text/ng-template" id="consumers-instructions.html">
             <div>
                <p>
                    This page allows you to browse all consumers currently open on the broker. These can be narrowed down
                    by specifying a filter and also sorted using the sort function in the toolbar. To execute a query
                    click on the <span class="fa fa-search"></span> button.
                </p>
                <p>
                    Consumers can be closed by using the <code>close</code> button under the <code>Actions</code> column and you can
                    navigate to the consumers  session, address and queue by clicking on the appropriate field.
                  </p>
                  <p>
                    Note that each page is loaded in from the broker when navigating to a new page or when a query is executed.
                  </p>
                </div>
             </script>
             `,
              controller: ConsumersController
    })
    .name;


    function ConsumersController($scope, workspace, jolokia, localStorage, artemisMessage, $location, $timeout, $filter, $sanitize, pagination, artemisConsumer, artemisQueue, artemisAddress, artemisSession) {
        var ctrl = this;
        ctrl.pagination = pagination;
        ctrl.pagination.reset();
        var mbean = Artemis.getBrokerMBean(workspace, jolokia);
        ctrl.allConsumers = [];
        ctrl.consumers = [];
        ctrl.pageNumber = 1;
        ctrl.workspace = workspace;
        ctrl.refreshed = false;
        ctrl.consumerToDeletesSession = '';
        ctrl.consumerToDelete = '';
        ctrl.closeDialog = false;
        ctrl.dtOptions = {
           // turn of ordering as we do it ourselves
           ordering: false,
           columns: [
              {name: "ID", visible: true},
              {name: "Session", visible: true},
              {name: "Client ID", visible: true},
              {name: "Protocol", visible: true},
              {name: "Queue", visible: true},
              {name: "Queue Type", visible: true},
              {name: "Filter", visible: true},
              {name: "Address", visible: true},
              {name: "Remote Address", visible: true},
              {name: "Local Address", visible: true},
              {name: "Creation Time", visible: true}
         ]
        };

        Artemis.log.debug('localStorage: consumersColumnDefs =', localStorage.getItem('consumersColumnDefs'));
        if (localStorage.getItem('consumersColumnDefs')) {
          loadedDefs = JSON.parse(localStorage.getItem('consumersColumnDefs'));
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
          localStorage.setItem('consumersColumnDefs', JSON.stringify(attributes));
        }
        ctrl.filter = {
            fieldOptions: [
                {id: 'ID', name: 'ID'},
                {id: 'SESSION_ID', name: 'Session ID'},
                {id: 'CLIENT_ID', name: 'Client ID'},
                {id: 'USER', name: 'User'},
                {id: 'ADDRESS', name: 'Address'},
                {id: 'QUEUE', name: 'Queue'},
                {id: 'PROTOCOL', name: 'Protocol'},
                {id: 'LOCAL_ADDRESS', name: 'Local Address'},
                {id: 'REMOTE_ADDRESS', name: 'Remote Address'}
            ],
            operationOptions: [
                {id: 'EQUALS', name: 'Equals'},
                {id: 'CONTAINS', name: 'Contains'},
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
                sortColumn: "id"
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
            title: 'Close the Consumer',
            actionFn: openCloseDialog
           }
        ];
        ctrl.tableConfig = {
            selectionMatchProp: 'id',
            showCheckboxes: false
        };
        ctrl.tableColumns = [
            { header: 'ID', itemField: 'id' },
            { header: 'Session', itemField: 'session' , templateFn: function(value, item) { return '<a href="#" onclick="selectSession(' + item.idx + ')">' + $sanitize(value) + '</a>' }},
            { header: 'Client ID', itemField: 'clientID' },
            { header: 'Protocol', itemField: 'protocol' },
            { header: 'Queue', itemField: 'queue', templateFn: function(value, item) { return '<a href="#" onclick="selectQueue(' + item.idx + ')">' + $sanitize(value) + '</a>' }},
            { header: 'Queue Type', itemField: 'queueType' },
            { header: 'Filter', itemField: 'filter' },
            { header: 'Address', itemField: 'address' , templateFn: function(value, item) { return '<a href="#" onclick="selectAddress(' + item.idx + ')">' + $sanitize(value) + '</a>' }},
            { header: 'Remote Address', itemField: 'remoteAddress' },
            { header: 'Local Address', itemField: 'localAddress' },
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
            ctrl.filter.sortColumn = "id";
            ctrl.filter.text.fieldText = "Filter Field..";
            ctrl.filter.text.operationText = "Operation..";
            ctrl.filter.text.sortOrderText = "ascending";
            ctrl.filter.text.sortByText = "ID";
            ctrl.refreshed = true;
            artemisConsumer.consumer = null;
            ctrl.pagination.load();
        };

        if (artemisConsumer.consumer) {
            Artemis.log.debug("navigating to consumer = " + artemisConsumer.consumer.sessionID);
            ctrl.filter.values.field = ctrl.filter.fieldOptions[1].id;
            ctrl.filter.values.operation = ctrl.filter.operationOptions[0].id;
            ctrl.filter.values.value = artemisConsumer.consumer.sessionID;
            artemisConsumer.consumer = null;
        }

        if (artemisQueue.queue) {
            Artemis.log.info("navigating to consumer = " + artemisQueue.queue.queue);
            ctrl.filter.values.field = ctrl.filter.fieldOptions[5].id;
            ctrl.filter.values.operation = ctrl.filter.operationOptions[0].id;
            ctrl.filter.values.value = artemisQueue.queue.queue;
            artemisQueue.queue = null;
        }

        selectQueue = function (idx) {
            var queue = ctrl.consumers[idx].queue;
            Artemis.log.debug("navigating to queue:" + queue)
            artemisQueue.queue = { queue: queue };
            $location.path("artemis/artemisQueues");
        };

        selectAddress = function (idx) {
            var address = ctrl.consumers[idx].address;
            Artemis.log.debug("navigating to address:" + address)
            artemisAddress.address = { address: address };
            $location.path("artemis/artemisAddresses");
        };

        selectSession = function (idx) {
            var session = ctrl.consumers[idx].session;
            Artemis.log.debug("navigating to session:" + session)
            artemisSession.session = { session: session };
            $location.path("artemis/artemisSessions");
        };

        function openCloseDialog(action, item) {
            ctrl.consumerToDelete = item.id;
            ctrl.consumerToDeletesSession = item.session;
            ctrl.closeDialog = true;
        }

        ctrl.closeConsumer = function () {
           Artemis.log.debug("closing session: " + ctrl.consumerToDelete);
              if (mbean) {
                  jolokia.request({ type: 'exec',
                     mbean: mbean,
                     operation: 'closeConsumerWithID(java.lang.String,java.lang.String)',
                     arguments: [ctrl.consumerToDeletesSession, ctrl.consumerToDelete] },
                     Core.onSuccess(ctrl.pagination.load(), { error: function (response) {
                        Core.defaultJolokiaErrorHandler("Could not close session: " + response);
                 }}));
           }
        };
        ctrl.loadOperation = function () {
            if (mbean) {
                var method = 'listConsumers(java.lang.String, int, int)';
                var sessionsFilter = {
                    field: ctrl.filter.values.field,
                    operation: ctrl.filter.values.operation,
                    value: ctrl.filter.values.value,
                    sortOrder: ctrl.filter.values.sortOrder,
                    sortColumn: ctrl.filter.values.sortColumn
                };

                if (ctrl.refreshed == true) {
                    ctrl.pagination.reset();
                    ctrl.refreshed = false;
                }

                Artemis.log.info(JSON.stringify(sessionsFilter));
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
            ctrl.consumers = [];
            angular.forEach(data["data"], function (value, idx) {
                value.idx = idx;
                ctrl.consumers.push(value);
            });
            ctrl.pagination.page(data["count"]);
            allConsumers = ctrl.consumers;
            ctrl.consumers = allConsumers;
            Core.$apply($scope);
        }

        ctrl.pagination.load();
    }
    ConsumersController.$inject = ['$scope', 'workspace', 'jolokia', 'localStorage', 'artemisMessage', '$location', '$timeout', '$filter', '$sanitize', 'pagination', 'artemisConsumer', 'artemisQueue', 'artemisAddress', 'artemisSession'];


})(Artemis || (Artemis = {}));