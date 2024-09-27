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
             <script type="text/ng-template" id="consumers-anchor-column-template">
                <a href="#" ng-click="$ctrl.handleColAction(key, item)">{{value}}</a>
             </script>
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


    function ConsumersController($scope, workspace, jolokia, localStorage, artemisMessage, $location, $timeout, $filter, pagination, artemisConsumer, artemisQueue, artemisAddress, artemisSession) {
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
              {name: "Validated User", visible: false},
              {name: "Protocol", visible: true},
              {name: "Queue", visible: true},
              {name: "Queue Type", visible: true},
              {name: "Filter", visible: true},
              {name: "Address", visible: true},
              {name: "Remote Address", visible: true},
              {name: "Local Address", visible: true},
              {name: "Creation Time", visible: true},
              {name: "Messages in Transit", visible: false},
              {name: "Messages in Transit Size", visible: false},
              {name: "Messages Delivered", visible: false},
              {name: "Messages Delivered Size", visible: false},
              {name: "Messages Acknowledged", visible: false},
              {name: "Messages Acknowledged awaiting Commit", visible: false},
              {name: "Last Delivered Time", visible: false},
              {name: "Last Acknowledged Time", visible: false},
              {name: "Status", visible: false}
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
                {id: 'id', name: 'ID'},
                {id: 'session', name: 'Session'},
                {id: 'clientID', name: 'Client ID'},
                {id: 'user', name: 'User'},
                {id: 'validatedUser', name: 'Validated User'},
                {id: 'address', name: 'Address'},
                {id: 'queue', name: 'Queue'},
                {id: 'protocol', name: 'Protocol'},
                {id: 'localAddress', name: 'Local Address'},
                {id: 'remoteAddress', name: 'Remote Address'},
                {id: 'messagesInTransit', name: 'Messages in Transit'},
                {id: 'messagesInTransitSize', name: 'Messages in Transit Size'},
                {id: 'messagesDelivered', name: 'Messages Delivered'},
                {id: 'messagesDeliveredSize', name: 'Messages Delivered Size'},
                {id: 'messagesAcknowledged', name: 'Messages Acknowledged'},
                {id: 'messagesAcknowledgedAwaitingCommit', name: 'Messages Acknowledged awaiting Commit'},
                {id: 'status', name: 'status'}
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
            { header: 'Session', itemField: 'session' , htmlTemplate: 'consumers-anchor-column-template', colActionFn: (item) => selectSession(item.idx) },
            { header: 'Client ID', itemField: 'clientID' },
            { header: 'Validated User', itemField: 'validatedUser' },
            { header: 'Protocol', itemField: 'protocol' },
            { header: 'Queue', itemField: 'queueName', htmlTemplate: 'consumers-anchor-column-template', colActionFn: (item) => selectQueue(item.idx) },
            { header: 'Queue Type', itemField: 'queueType' },
            { header: 'Filter', itemField: 'filter' },
            { header: 'Address', itemField: 'addressName' , htmlTemplate: 'consumers-anchor-column-template', colActionFn: (item) => selectAddress(item.idx) },
            { header: 'Remote Address', itemField: 'remoteAddress' },
            { header: 'Local Address', itemField: 'localAddress' },
            { header: 'Creation Time', itemField: 'creationTime' },
            { header: 'Messages in Transit', itemField: 'messagesInTransit' },
            { header: 'Messages in Transit Size', itemField: 'messagesInTransitSize' },
            { header: 'Messages Delivered', itemField: 'messagesDelivered' },
            { header: 'Messages Delivered Size', itemField: 'messagesDeliveredSize' },
            { header: 'Messages Acknowledged', itemField: 'messagesAcknowledged' },
            { header: 'Messages Acknowledged awaiting Commit', itemField: 'messagesAcknowledgedAwaitingCommit' },
            { header: 'Last Delivered Time', itemField: 'lastDeliveredTime', templateFn: function(value) { return formatTimestamp(value);} },
            { header: 'Last Acknowledged Time', itemField: 'lastAcknowledgedTime' ,  templateFn: function(value) { return formatTimestamp(value);} },
            { header: 'Status', itemField: 'status' }
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
                    sortField: ctrl.filter.values.sortField
                };

                if (ctrl.refreshed == true) {
                    ctrl.pagination.reset();
                    ctrl.refreshed = false;
                }

                Artemis.log.info(JSON.stringify(sessionsFilter));
                jolokia.request({ type: 'exec', mbean: mbean, operation: method, arguments: [JSON.stringify(sessionsFilter), ctrl.pagination.pageNumber, ctrl.pagination.pageSize] }, Core.onSuccess(populateTable, { error: onError }));
            }
        };

        function formatTimestamp(timestamp) {
             Artemis.log.info("in timestamp " + timestamp + " " + (typeof timestamp !== "number"));
             if (isNaN(timestamp) || typeof timestamp !== "number") {
                Artemis.log.info("returning timestamp " + timestamp + " " + (typeof timestamp !== "number"));
                return timestamp;
             }
             if (timestamp === 0) {
                return "N/A";
             }
             var d = new Date(timestamp);
             // "yyyy-MM-dd HH:mm:ss"
             //add 1 to month as getmonth returns the position not the actual month
             return d.getFullYear() + "-" + pad2(d.getMonth() + 1) + "-" + pad2(d.getDate()) + " " + pad2(d.getHours()) + ":" + pad2(d.getMinutes()) + ":" + pad2(d.getSeconds());
        }
        function pad2(value) {
            return (value < 10 ? '0' : '') + value;
        }


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
                value.addressName = value.address;
                value.queueName = value.queue;
                ctrl.consumers.push(value);
            });
            ctrl.pagination.page(data["count"]);
            allConsumers = ctrl.consumers;
            ctrl.consumers = allConsumers;
            Core.$apply($scope);
        }

        ctrl.pagination.load();
    }
    ConsumersController.$inject = ['$scope', 'workspace', 'jolokia', 'localStorage', 'artemisMessage', '$location', '$timeout', '$filter', 'pagination', 'artemisConsumer', 'artemisQueue', 'artemisAddress', 'artemisSession'];


})(Artemis || (Artemis = {}));