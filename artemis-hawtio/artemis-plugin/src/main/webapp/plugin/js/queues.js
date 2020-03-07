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
/**
 * @module ARTEMIS
 */
var ARTEMIS = (function(ARTEMIS) {

    ARTEMIS.QueuesController = function ($scope, $location, workspace, ARTEMISService, jolokia, localStorage, artemisConnection, artemisSession, artemisQueue, artemisAddress) {

        var artemisJmxDomain = localStorage['artemisJmxDomain'] || "org.apache.activemq.artemis";

        /**
         *  Required For Each Object Type
         */

        var objectType = "queue";
        var method = 'listQueues(java.lang.String, int, int)';
        var defaultAttributes = [
            {
                field: 'manage',
                displayName: 'manage',
                width: '*',
                cellTemplate: '<div class="ngCellText"><a ng-click="navigateToQueueAtts(row)">attributes</a>&nbsp;<a ng-click="navigateToQueueOps(row)">operations</a></div>'
            },
            {
                field: 'id',
                displayName: 'ID',
                width: '*'
            },
            {
                field: 'name',
                displayName: 'Name',
                width: '*',
                cellTemplate: '<div class="ngCellText" title="{{row.entity.name}}">{{row.entity.name}}</div>'
            },
            {
                field: 'address',
                displayName: 'Address',
                width: '*',
                cellTemplate: '<div class="ngCellText" title="{{row.entity.address}}"><a ng-click="selectAddress(row)">{{row.entity.address}}</a></div>'
            },
            {
                field: 'routingType',
                displayName: 'Routing Type',
                width: '*'
            },
            {
                field: 'filter',
                displayName: 'Filter',
                width: '*',
                cellTemplate: '<div class="ngCellText" title="{{row.entity.filter}}">{{row.entity.filter}}</div>'
            },
            {
                field: 'durable',
                displayName: 'Durable',
                width: '*'
            },
            {
                field: 'maxConsumers',
                displayName: 'Max Consumers',
                width: '*'
            },
            {
                field: 'purgeOnNoConsumers',
                displayName: 'Purge On No Consumers',
                width: '*'
            },
            {
                field: 'consumerCount',
                displayName: 'Consumer Count',
                width: '*'
            },
            {
                field: 'rate',
                displayName: 'Rate',
                width: '*'
            },
            {
                field: 'messageCount',
                displayName: 'Message Count',
                width: '*',
                cellTemplate: '<div class="ngCellText"><a ng-click="navigateToBrowseQueue(row)">{{row.entity.messageCount}}</a></div>'
            },

            // Hidden
            {
                field: 'paused',
                displayName: 'Paused',
                width: '*',
                visible: false
            },
            {
                field: 'temporary',
                displayName: 'Temporary',
                width: '*',
                visible: false
            },
            {
                field: 'autoCreated',
                displayName: 'Auto Created',
                width: '*',
                visible: false
            },
            {
                field: 'user',
                displayName: 'User',
                width: '*',
                visible: false
            },
            {
                field: 'messagesAdded',
                displayName: 'Total Messages Added',
                width: '*',
                visible: false
            },
            {
                field: 'messagesAcked',
                displayName: 'Total Messages Acked',
                width: '*',
                visible: false
            },
            {
                field: 'deliveringCount',
                displayName: 'Delivering Count',
                width: '*',
                visible: false
            },
            {
                field: 'messagesKilled',
                displayName: 'Messages Killed',
                width: '*',
                visible: false
            },
            {
                field: 'directDeliver',
                displayName: 'Direct Deliver',
                width: '*',
                visible: false
            }
        ];
        ARTEMIS.log.debug('sessionStorage: queuesColumnDefs =', sessionStorage.getItem('queuesColumnDefs'));
        var attributes = defaultAttributes;
        if (sessionStorage.getItem('queuesColumnDefs')) {
            attributes = JSON.parse(sessionStorage.getItem('queuesColumnDefs'));
        }
        $scope.$on('ngGridEventColumns', function (newColumns) {
            ARTEMIS.log.debug('ngGridEventColumns:', newColumns);
            var visibles = newColumns.targetScope.columns.reduce(function (visibles, column) {
                visibles[column.field] = column.visible;
                return visibles;
            }, {});
            ARTEMIS.log.debug('ngGridEventColumns: visibles =', visibles);
            attributes.forEach(function (attribute) {
                attribute.visible = visibles[attribute.field];
            });
            sessionStorage.setItem('queuesColumnDefs', JSON.stringify(attributes));
        });

        $scope.filter = {
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
                {id: 'RATE', name: 'Rate'},
            ],
            operationOptions: [
                {id: 'EQUALS', name: 'Equals'},
                {id: 'CONTAINS', name: 'Contains'},
                {id: 'GREATER_THAN', name: 'Greater Than'},
                {id: 'LESS_THAN', name: 'Less Than'}
            ],
            values: {
                field: "",
                operation: "",
                value: "",
                sortOrder: "asc",
                sortBy: "id"
            }
        };

        /**
         *  Below here is utility.
         *
         *  TODO Refactor into new separate files
         */
        if (artemisAddress.address) {
            $scope.filter.values.field = $scope.filter.fieldOptions[3].id;
            $scope.filter.values.operation = $scope.filter.operationOptions[0].id;
            $scope.filter.values.value = artemisAddress.address.name;
            artemisAddress.address = null;
        }
        if (artemisQueue.queue) {
            $scope.filter.values.field = $scope.filter.fieldOptions[1].id;
            $scope.filter.values.operation = $scope.filter.operationOptions[0].id;
            $scope.filter.values.value = artemisQueue.queue.queue;
            artemisQueue.queue = null;
        }

        artemisSession.session = null;
        $scope.navigateToQueueAtts = function (row) {
            $location.path("jmx/attributes").search({"tab": "artemis", "nid": ARTEMIS.getQueueNid(row.entity, $location)});
        };
        $scope.navigateToQueueOps = function (row) {
            $location.path("jmx/operations").search({"tab": "artemis", "nid": ARTEMIS.getQueueNid(row.entity, $location)});
        };
        $scope.navigateToBrowseQueue = function (row) {
            $location.path("artemis/browseQueue").search({"tab": "artemis", "nid": ARTEMIS.getQueueNid(row.entity, $location)});
        };
        $scope.selectAddress = function (row) {
            artemisAddress.address = row.entity;
            $location.path("artemis/addresses");
        };
        $scope.workspace = workspace;
        $scope.objects = [];
        $scope.totalServerItems = 0;
        $scope.pagingOptions = {
            pageSizes: [50, 100, 200],
            pageSize: 100,
            currentPage: 1
        };
        $scope.sortOptions = {
                fields: ["id"],
                columns: ["id"],
                directions: ["asc"]
            };
        var refreshed = false;

        $scope.gridOptions = {
            selectedItems: [],
            data: 'objects',
            showFooter: true,
            showFilter: true,
            showColumnMenu: true,
            enableCellSelection: false,
            enableHighlighting: true,
            enableColumnResize: true,
            enableColumnReordering: true,
            selectWithCheckboxOnly: false,
            showSelectionCheckbox: false,
            multiSelect: false,
            displaySelectionCheckbox: false,
            pagingOptions: $scope.pagingOptions,
            enablePaging: true,
            totalServerItems: 'totalServerItems',
            maintainColumnRatios: false,
            columnDefs: attributes,
            enableFiltering: true,
            useExternalFiltering: true,
            sortInfo: $scope.sortOptions,
            useExternalSorting: true,
        };
        $scope.refresh = function () {
            refreshed = true;
            $scope.loadTable();
        };
        $scope.reset = function () {
            $scope.filter.values.field = "";
            $scope.filter.values.operation = "";
            $scope.filter.values.value = "";
            $scope.loadTable();
        };
        $scope.loadTable = function () {
            $scope.filter.values.sortColumn = $scope.sortOptions.fields[0];
            $scope.filter.values.sortBy = $scope.sortOptions.directions[0];
            $scope.filter.values.sortOrder = $scope.sortOptions.directions[0];
            var mbean = getBrokerMBean(jolokia);
            if (mbean.includes("undefined")) {
                onBadMBean();
            } else if (mbean) {
                var filter = JSON.stringify($scope.filter.values);
                console.log("Filter string: " + filter);
                jolokia.request({ type: 'exec', mbean: mbean, operation: method, arguments: [filter, $scope.pagingOptions.currentPage, $scope.pagingOptions.pageSize] }, onSuccess(populateTable, { error: onError }));
            }
        };
        function onError() {
            Core.notification("error", "Could not retrieve " + objectType + " list from Artemis.");
        }
        function onBadMBean() {
            Core.notification("error", "Could not retrieve " + objectType + " list. Wrong MBean selected.");
        }
        function populateTable(response) {
            var data = JSON.parse(response.value);
            $scope.objects = [];
            angular.forEach(data["data"], function (value, idx) {
                $scope.objects.push(value);
            });
            $scope.totalServerItems = data["count"];
            if (refreshed == true) {
                $scope.gridOptions.pagingOptions.currentPage = 1;
                refreshed = false;
            }
            Core.$apply($scope);
        }
        $scope.$watch('sortOptions', function (newVal, oldVal) {
            if (newVal !== oldVal) {
                $scope.loadTable();
            }
        }, true);
        $scope.$watch('pagingOptions', function (newVal, oldVal) {
            if (parseInt(newVal.currentPage) && newVal !== oldVal && newVal.currentPage !== oldVal.currentPage) {
                $scope.loadTable();
            }
            if (parseInt(newVal.pageSize) && newVal !== oldVal && newVal.pageSize !== oldVal.pageSize) {
                $scope.pagingOptions.currentPage = 1;
                $scope.loadTable();
            }
        }, true);

        function getBrokerMBean(jolokia) {
            var mbean = null;
            var selection = workspace.selection;
            var folderNames = selection.folderNames;
            mbean = "" + folderNames[0] + ":broker=" + folderNames[1];
            ARTEMIS.log.info("broker=" + mbean);
            return mbean;
        };
        $scope.refresh();
    };
    return ARTEMIS;
} (ARTEMIS || {}));
