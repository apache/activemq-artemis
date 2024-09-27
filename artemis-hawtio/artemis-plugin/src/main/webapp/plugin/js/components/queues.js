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
                            dt-options="$ctrl.dtOptions"
                            columns="$ctrl.tableColumns"
                            action-buttons="$ctrl.tableActionButtons"
                            items="$ctrl.queues">
             </pf-table-view>
             <div ng-include="'plugin/artemispagination.html'"></div>
             <script type="text/ng-template" id="queues-anchor-column-template">
                <a href="#" ng-click="$ctrl.handleColAction(key, item)">{{value}}</a>
             </script>
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
        ctrl.pagination.reset();
        var mbean = Artemis.getBrokerMBean(workspace, jolokia);
        ctrl.allAddresses = [];
        ctrl.queues = [];
        ctrl.workspace = workspace;
        ctrl.refreshed = false;
        ctrl.dtOptions = {
           // turn of ordering as we do it ourselves
           ordering: false,
           columns: [
                  {name: "ID", visible: true},
                  {name: "name", visible: true},
                  {name: "Address", visible: true},
                  {name: "Routing Type", visible: true},
                  {name: "Filter", visible: true},
                  {name: "Durable", visible: true},
                  {name: "Max Consumers", visible: true},
                  {name: "Purge On No Consumers", visible: true},
                  {name: "Consumer Count", visible: true},
                  {name: "Message Count", visible: true},
                  {name: "Paused", visible: false},
                  {name: "Temporary", visible: false},
                  {name: "Auto Created", visible: false},
                  {name: "User", visible: false},
                  {name: "Total Messages Added", visible: false},
                  {name: "Total Messages Acked", visible: false},
                  {name: "Delivering Count", visible: false},
                  {name: "Messages Killed", visible: false},
                  {name: "Direct Deliver", visible: false},
                  {name: "Exclusive", visible: false},
                  {name: "Last Value", visible: false},
                  {name: "Last Value Key", visible: false},
                  {name: "Scheduled Count", visible: false},
                  {name: "Group Rebalance", visible: false},
                  {name: "Group Rebalance Pause Dispatch", visible: false},
                  {name: "Group Buckets", visible: false},
                  {name: "Group First Key", visible: false},
                  {name: "Queue Enabled", visible: false},
                  {name: "Ring Size", visible: false},
                  {name: "Consumers Before Dispatch", visible: false},
                  {name: "Delay Before Dispatch", visible: false},
                  {name: "Auto Delete", visible: false},
                  {name: "Internal", visible: false}
             ]
        };

        Artemis.log.debug('localStorage: queuesColumnDefs =', localStorage.getItem('queuesColumnDefs'));
        if (localStorage.getItem('queuesColumnDefs')) {
        Artemis.log.info("loading columns " + localStorage.getItem('queuesColumnDefs'))
              loadedDefs = JSON.parse(localStorage.getItem('queuesColumnDefs'));
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
              localStorage.setItem('queuesColumnDefs', JSON.stringify(attributes));
        }
        ctrl.filter = {
            fieldOptions: [
                {id: 'id', name: 'ID'},
                {id: 'name', name: 'Name'},
                {id: 'consumerCount', name: 'Consumer Count'},
                {id: 'address', name: 'Address'},
                {id: 'filter', name: 'Filter'},
                {id: 'maxConsumers', name: 'Max Consumers'},
                {id: 'routingType', name: 'Routing Type'},
                {id: 'purgeOnNoConsumers', name: 'Purge On No Consumers'},
                {id: 'user', name: 'User'},
                {id: 'messageCount', name: 'Message Count'},
                {id: 'deliveringCount', name: 'Delivering Count'},
                {id: 'paused', name: 'Paused'},
                {id: 'temporary', name: 'Temporary'},
                {id: 'autoCreated', name: 'Auto Created'},
                {id: 'autoDelete', name: 'Auto Delete'},
                {id: 'internalQueue', name: 'Internal'}
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
            { header: 'Name', itemField: 'name', htmlTemplate: 'queues-anchor-column-template', colActionFn: (item) => selectQueue(item.idx) },
            { header: 'Address', itemField: 'address', htmlTemplate: 'queues-anchor-column-template', colActionFn: (item) => selectAddress(item.idx) },
            { header: 'Routing Type', itemField: 'routingType'},
            { header: 'Filter', itemField: 'filter' },
            { header: 'Durable', itemField: 'durable' },
            { header: 'Max Consumers', itemField: 'maxConsumers' },
            { header: 'Purge On No Consumers', itemField: 'purgeOnNoConsumers' },
            { header: 'Consumer Count', itemField: 'consumerCount', htmlTemplate: 'queues-anchor-column-template', colActionFn: (item) => selectConsumers(item.idx) },
            { header: 'Message Count', itemField: 'messageCount', htmlTemplate: 'queues-anchor-column-template', colActionFn: (item) => browseQueue(item.idx) },
            { header: 'Paused', itemField: 'paused' },
            { header: 'Temporary', itemField: 'temporary' },
            { header: 'Auto Created', itemField: 'autoCreated' },
            { header: 'User', itemField: 'user' },
            { header: 'Total Messages Added', itemField: 'messagesAdded' },
            { header: 'Total Messages Acked', itemField: 'messagesAcked' },
            { header: 'Delivering Count', itemField: 'deliveringCount' },
            { header: 'Messages Killed', itemField: 'messagesKilled' },
            { header: 'Direct Deliver', itemField: 'directDeliver' },
            { header: 'exclusive', itemField: 'exclusive' },
            { header: 'Last Value', itemField: 'lastValue' },
            { header: 'Last Value Key', itemField: 'lastValueKey' },
            { header: 'Scheduled Count', itemField: 'scheduledCount' },
            { header: 'Group Rebalance', itemField: 'groupRebalance' },
            { header: 'Group Rebalance Pause Dispatch', itemField: 'groupRebalancePauseDispatch' },
            { header: 'Group Buckets', itemField: 'groupBuckets' },
            { header: 'Group First Key', itemField: 'groupFirstKey' },
            { header: 'Enabled', itemField: 'enabled'},
            { header: 'Ring Size', itemField: 'ringSize'},
            { header: 'Consumers Before Dispatch', itemField: 'consumersBeforeDispatch'},
            { header: 'Delay Before Dispatch', itemField: 'delayBeforeDispatch'},
            { header: 'Auto Delete', itemField: 'autoDelete'},
            { header: 'Internal', itemField: 'internalQueue'}
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
            ctrl.pagination.load();
        };

        if (artemisQueue.queue) {
            Artemis.log.debug("navigating to queue = " + artemisQueue.queue.queue);
            ctrl.filter.values.field = ctrl.filter.fieldOptions[1].id;
            ctrl.filter.values.operation = ctrl.filter.operationOptions[0].id;
            ctrl.filter.values.value = artemisQueue.queue.queue;
            artemisQueue.queue = null;
        }

        if (artemisAddress.address) {
            Artemis.log.debug("navigating to address = " + artemisAddress.address.address);
            ctrl.filter.values.field = ctrl.filter.fieldOptions[3].id;
            ctrl.filter.values.operation = ctrl.filter.operationOptions[0].id;
            ctrl.filter.values.value = artemisAddress.address.address;
            artemisAddress.address = null;
        }

        function navigateToQueuesAtts(action, item) {
            qnid = getQueuesNid(item, $location);
            Artemis.log.info(qnid);
            $location.path("artemis/attributes").search({"tab": "artemis", "nid": qnid });
        };
        function navigateToQueuesOps(action, item) {
            $location.path("artemis/operations").search({"tab": "artemis", "nid": getQueuesNid(item, $location)});
        };
        selectAddress = function (idx) {
            var item = ctrl.queues[idx]
            Artemis.log.debug("navigating to address:" + item.address);
            artemisAddress.address = { address: item.address };
            $location.path("artemis/artemisAddresses").search({"tab": "artemis", "nid": getAddressesNid(item, $location)});
        };
        selectQueue = function (idx) {
            var item = ctrl.queues[idx];
            var nid = getQueuesNid(item, $location);
            Artemis.log.debug("navigating to queue:" + nid);
            artemisQueue.queue = { queue: item.name };
            $location.path("artemis/artemisQueues").search({"tab": "artemis", "nid": nid});
        };
        selectConsumers = function (idx) {
            var item = ctrl.queues[idx];
            var nid = getQueuesNid(item, $location);
            artemisQueue.queue = { queue: item.name };
            $location.path("artemis/artemisConsumers").search({"tab": "artemis", "nid": nid});
        };
        browseQueue = function (idx) {
            var item = ctrl.queues[idx];
            var nid = getQueuesNid(item, $location);
            Artemis.log.debug("navigating to queue browser:" + nid);
            $location.path("artemis/artemisBrowseQueue").search({"tab": "artemis", "nid": nid});
        };
        function getQueuesNid(item, $location) {
            var rootNID = getRootNid($location);
            var targetNID = rootNID + "addresses-" + item.address + "-queues-" + item.routingType.toLowerCase() + "-" + item.name;
            Artemis.log.debug("targetNID=" + targetNID);
            return targetNID;
        }
        function getAddressesNid(item, $location) {
            var rootNID = getRootNid($location);
            var targetNID = rootNID + "addresses-" + item.address;
            Artemis.log.debug("targetNID=" + targetNID);
            return targetNID;
        }
        function getRootNid($location) {
            var mBean = Artemis.getBrokerMBean(workspace, jolokia);
            var details = Core.parseMBean(mBean);
            var properties = details['attributes'];
            var brokerAddress = properties["broker"] || "unknown";
            var artemisJmxDomain = localStorage['artemisJmxDomain'] || "org.apache.activemq.artemis";
            //we have to remove the surrounding quotes
            return "root-" + artemisJmxDomain + "-" + brokerAddress.replace(/^"|"$/g, '') + "-";

        }
        ctrl.loadOperation = function () {
            if (mbean) {
                var method = 'listQueues(java.lang.String, int, int)';
                var queuesFilter = {
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
                jolokia.request({ type: 'exec', mbean: mbean, operation: method, arguments: [JSON.stringify(queuesFilter), ctrl.pagination.pageNumber, ctrl.pagination.pageSize] }, Core.onSuccess(populateTable, { error: onError }));
            }
        };

        ctrl.pagination.setOperation(ctrl.loadOperation);

        function onError(response) {
            Core.notification("error", "could not invoke list queues" + response.error);
            $scope.workspace.selectParentNode();
        };

        function populateTable(response) {
            var data = JSON.parse(response.value);
            ctrl.queues = [];
            angular.forEach(data["data"], function (value, idx) {
                value.idx = idx;
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