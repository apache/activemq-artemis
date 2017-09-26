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

    ARTEMIS.SessionsController = function ($scope, $location, workspace, ARTEMISService, jolokia, localStorage, artemisConnection, artemisSession, artemisConsumer, artemisProducer) {

        var artemisJmxDomain = localStorage['artemisJmxDomain'] || "org.apache.activemq.artemis";

        /**
         *  Required For Each Object Type
         */

        var objectType = "sessions"
        var method = 'listSessions(java.lang.String, int, int)';
        var attributes = [
            {
                field: 'id',
                displayName: 'ID',
                width: '*'
            },
            {
                field: 'connectionID',
                displayName: 'Connection',
                width: '*',
                cellTemplate: '<div class="ngCellText"><a ng-click="selectConnection(row)">{{row.entity.connectionID}}</a></div>'
            },
            {
                field: 'user',
                displayName: 'User',
                width: '*'
            },
            {
                field: 'consumerCount',
                displayName: 'Consumer Count',
                width: '*',
                cellTemplate: '<div class="ngCellText"><a ng-click="selectConsumers(row)">{{row.entity.consumerCount}}</a></div>'
            },
            {
                field: 'producerCount',
                displayName: 'Producer Count',
                width: '*',
                cellTemplate: '<div class="ngCellText"><a ng-click="selectProducers(row)">{{row.entity.producerCount}}</a></div>'
            },
            {
                field: 'creationTime',
                displayName: 'Creation Time',
                width: '*'
            },
        ];
        $scope.filter = {
            fieldOptions: [
                {id: 'ID', name: 'ID'},
                {id: 'CONNECTION_ID', name: 'Connection ID'},
                {id: 'CONSUMER_COUNT', name: 'Consumer Count'},
                {id: 'USER', name: 'User'},
                {id: 'PROTOCOL', name: 'Protocol'},
                {id: 'CLIENT_ID', name: 'Client ID'},
                {id: 'LOCAL_ADDRESS', name: 'Local Address'},
                {id: 'REMOTE_ADDRESS', name: 'Remote Address'},
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
                sortBy: "ID"
            }
        };
        // Configure Parent/Child click through relationships
        $scope.selectConnection = function (row) {
            artemisSession.session = row.entity;
            $location.path("artemis/connections");
        };

        $scope.selectProducers = function (row) {
           artemisSession.session = row.entity;
           $location.path("artemis/producers");
        };

        $scope.selectConsumers = function (row) {
           artemisSession.session = row.entity;
           $location.path("artemis/consumers");
        };

        if (artemisConnection.connection) {
            ARTEMIS.log.info("navigating to connection = " + artemisConnection.connection.connectionID);
            $scope.filter.values.field = $scope.filter.fieldOptions[1].id;
            $scope.filter.values.operation = $scope.filter.operationOptions[0].id;
            $scope.filter.values.value = artemisConnection.connection.connectionID;
        }

        if (artemisConsumer.consumer) {
            ARTEMIS.log.info("navigating to connection = " + artemisConsumer.consumer.ID);
            $scope.filter.values.field = $scope.filter.fieldOptions[0].id;
            $scope.filter.values.operation = $scope.filter.operationOptions[0].id;
            $scope.filter.values.value = artemisConsumer.consumer.session;
        }

        if (artemisProducer.producer) {
            ARTEMIS.log.info("navigating to producer = " + artemisProducer.producer.ID);
            $scope.filter.values.field = $scope.filter.fieldOptions[0].id;
            $scope.filter.values.operation = $scope.filter.operationOptions[0].id;
            $scope.filter.values.value = artemisProducer.producer.session;
        }
        //refresh after use
        artemisSession.connection = null;
        artemisConsumer.consumer = null;
        artemisProducer.producer = null;
        $scope.closeSession = function () {
           var sessionID = $scope.gridOptions.selectedItems[0].id;
           var connectionID = $scope.gridOptions.selectedItems[0].connectionID;
           ARTEMIS.log.info("closing session: " + sessionID);
           if (workspace.selection) {
              var mbean = getBrokerMBean(jolokia);
              if (mbean) {
                 jolokia.request({ type: 'exec',
                    mbean: mbean,
                    operation: 'closeSessionWithID(java.lang.String, java.lang.String)',
                    arguments: [connectionID, sessionID] },
                    onSuccess($scope.loadTable(), { error: function (response) {
                       Core.defaultJolokiaErrorHandler("Could not close session: " + response);
                    }}));
             }
          }
        };
        /**
         *  Below here is utility.
         *
         *  TODO Refactor into new separate files
         */

        $scope.workspace = workspace;
        $scope.objects = [];
        $scope.totalServerItems = 0;
        $scope.pagingOptions = {
            pageSizes: [50, 100, 200],
            pageSize: 100,
            currentPage: 1
        };
        $scope.sort = {
            fields: ["ID"],
            directions: ["asc"]
        };
        var refreshed = false;
        $scope.showClose = false;
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
            $scope.filter.values.sortColumn = $scope.sort.fields[0];
            $scope.filter.values.sortBy = $scope.sort.directions[0];
            var mbean = getBrokerMBean(jolokia);
            if (mbean) {
                var filter = JSON.stringify($scope.filter.values);
                console.log("Filter string: " + filter);
                jolokia.request({ type: 'exec', mbean: mbean, operation: method, arguments: [filter, $scope.pagingOptions.currentPage, $scope.pagingOptions.pageSize] }, onSuccess(populateTable, { error: onError }));
            }
        };
        $scope.selectGridRow = function () { 
            $scope.showClose =  $scope.gridOptions.selectedItems.length > 0;
        };
        function onError() {
            Core.notification("error", "Could not retrieve " + objectType + " list from Artemis.");
        }
        function populateTable(response) {
            $scope.gridOptions.selectedItems.length = 0;
            $scope.showClose =  false;
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
