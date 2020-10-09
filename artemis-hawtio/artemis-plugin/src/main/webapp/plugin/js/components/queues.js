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
    //Artemis.log.info("loading addresses");
    Artemis._module.component('artemisQueues', {
        template:
            `<h1>Browse Queues
                <button type="button" class="btn btn-link jvm-title-popover"
                          uib-popover-template="'queues-instructions.html'" popover-placement="bottom-left"
                          popover-title="Instructions" popover-trigger="'outsideClick'">
                    <span class="pficon pficon-help"></span>
                </button>
            </h1>
             <div ng-include="'plugin/artemistoolbar.html'"></div>
             <pf-table-view config="$ctrl.tableConfig"
                            columns="$ctrl.tableColumns"
                            action-buttons="$ctrl.tableActionButtons"
                            items="$ctrl.queues">
             </pf-table-view>
             <div ng-include="'plugin/artemispagination.html'"></div>
             <script type="text/ng-template" id="queues-instructions.html">
             <div>
                <p>
                    This page allows you to browse all queues on the broker. These can be narrowed down
                    by specifying a filter and also sorted using the sort function in the toolbar. To execute a query
                    click on the <span class="fa fa-search"></span> button.
                </p>
                <p>
                    You can also navigate directly to the JMX attributes and operations tabs by using the  <code>attributes</code>
                    and <code>operations</code> button under the <code>Actions</code> column.You can navigate to the
                    queues address by clicking on the <code>Address</code> field.
                  </p>
                  <p>
                    Note that each page is loaded in from the broker when navigating to a new page or when a query is executed.
                  </p>
                </div>
             </script>
             `,
              controller: QueuesController
    })
    .name;


    function QueuesController($scope, workspace, jolokia, localStorage, artemisMessage, $location, $timeout, $filter, pagination, artemisQueue, artemisAddress) {
        var ctrl = this;
        ctrl.pagination = pagination;
        var mbean = Artemis.getBrokerMBean(workspace, jolokia);
        ctrl.allAddresses = [];
        ctrl.queues = [];
        ctrl.workspace = workspace;
        ctrl.refreshed = false;
        ctrl.filter = {
            fieldOptions: [
                {id: 'ID', name: 'ID'},
                {id: 'NAME', name: 'Name'},
                {id: 'CONSUMER_ID', name: 'Consumer ID'},
                {id: 'ADDRESS', name: 'Address'},
                {id: 'FILTER', name: 'Filter'},
                {id: 'MAX_CONSUMERS', name: 'maxConsumers'},
                {id: 'ROUTING_TYPE', name: 'Routing Type'},
                {id: 'PURGE_ON_NO_CONSUMERS', name: 'Purge On No Consumers'},
                {id: 'USER', name: 'User'},
                {id: 'MESSAGE_COUNT', name: 'Message Count'},
                {id: 'DELIVERING_COUNT', name: 'Delivering Count'},
                {id: 'PAUSED', name: 'Paused'},
                {id: 'TEMPORARY', name: 'Temporary'},
                {id: 'AUTO_CREATED', name: 'Auto Created'},
                {id: 'RATE', name: 'Rate'}
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
                name: 'attributes',
                title: 'Navigate to attributes',
                actionFn: navigateToQueuesAtts
            },
            {
               name: 'operations',
               title: 'navigate to operations',
               actionFn: navigateToQueuesOps
            }
        ];
        ctrl.tableConfig = {
            selectionMatchProp: 'id',
            showCheckboxes: false
        };
        ctrl.tableColumns = [
            { header: 'ID', itemField: 'id' },
            { header: 'Name', itemField: 'name' },
            { header: 'Routing Types', itemField: 'routingTypes' },
            { header: 'Queue Count', itemField: 'queueCount' },
            { header: 'Address', itemField: 'address' , templateFn: function(value, item) { return '<a href="#" onclick="selectAddress(\'' + item.address + '\')">' + value + '</a>' }},
            { header: 'Routing Type', itemField: 'routingType' },
            { header: 'Filter', itemField: 'filter' },
            { header: 'Durable', itemField: 'durable' },
            { header: 'Max Consumers', itemField: 'maxConsumers' },
            { header: 'Purge On No Consumers', itemField: 'purgeOnNoConsumers' },
            { header: 'Consumer Count', itemField: 'consumerCount' },
            { header: 'Rate', itemField: 'rate' },
            { header: 'Message Count', itemField: 'messageCount' },
            { header: 'Paused', itemField: 'paused' },
            { header: 'Temporary', itemField: 'temporary' },
            { header: 'Auto Created', itemField: 'autoCreated' },
            { header: 'User', itemField: 'user' },
            { header: 'Total Messages Added', itemField: 'messagesAdded' },
            { header: 'Total Messages Acked', itemField: 'messagesAcked' },
            { header: 'Delivering Count', itemField: 'deliveringCount' },
            { header: 'Messages Killed', itemField: 'messagesKilled' },
            { header: 'Direct Deliver', itemField: 'directDeliver' }
        ];

        ctrl.refresh = function () {
            ctrl.refreshed = true;
            loadTable();
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
            ctrl.pagination.load();
        };

        if (artemisQueue.queue) {
            Artemis.log.info("navigating to queue = " + artemisQueue.queue.queue);
            ctrl.filter.values.field = ctrl.filter.fieldOptions[1].id;
            ctrl.filter.values.operation = ctrl.filter.operationOptions[0].id;
            ctrl.filter.values.value = artemisQueue.queue.queue;
        }

        if (artemisAddress.address) {
            Artemis.log.info("navigating to queue = " + artemisAddress.address.address);
            ctrl.filter.values.field = ctrl.filter.fieldOptions[3].id;
            ctrl.filter.values.operation = ctrl.filter.operationOptions[0].id;
            ctrl.filter.values.value = artemisAddress.address.address;
        }

        function navigateToQueuesAtts(action, item) {
            $location.path("artemis/attributes").search({"tab": "artemis", "nid": getQueuesNid(item, $location)});
        };
        function navigateToQueuesOps(action, item) {
            $location.path("artemis/operations").search({"tab": "artemis", "nid": getQueuesNid(item, $location)});
        };
        selectAddress = function (address) {
            Artemis.log.info("navigating to address:" + address)
            artemisAddress.address = { address: address };
            $location.path("artemis/artemisAddresses");
        };
        function getQueuesNid(item, $location) {
            var rootNID = getRootNid($location);
            var targetNID = rootNID + "addresses-" + item.address + "-queues-" + item.routingType.toLowerCase() + "-" + item.name;
            Artemis.log.info("targetNID=" + targetNID);
            return targetNID;
        }
        function getRootNid($location) {
            var currentNid = $location.search()['nid'];
            Artemis.log.info("current nid=" + currentNid);
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
                var method = 'listQueues(java.lang.String, int, int)';
                var queuesFilter = {
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
                jolokia.request({ type: 'exec', mbean: mbean, operation: method, arguments: [JSON.stringify(queuesFilter), ctrl.pagination.pageNumber, ctrl.pagination.pageSize] }, Core.onSuccess(populateTable, { error: onError }));
            }
        };

        ctrl.pagination.setOperation(ctrl.loadOperation);

        function onError(response) {
            Core.notification("error", "could not invoke list sessions" + response.error);
            $scope.workspace.selectParentNode();
        };

        function populateTable(response) {
            var data = JSON.parse(response.value);
            ctrl.queues = [];
            angular.forEach(data["data"], function (value, idx) {
                ctrl.queues.push(value);
            });
            ctrl.pagination.page(data["count"]);
            allQueues = ctrl.queues;
            ctrl.queues = allQueues;
            Core.$apply($scope);
        }

        ctrl.pagination.load();
    }
    QueuesController.$inject = ['$scope', 'workspace', 'jolokia', 'localStorage', 'artemisMessage', '$location', '$timeout', '$filter', 'pagination', 'artemisQueue', 'artemisAddress'];


})(Artemis || (Artemis = {}));