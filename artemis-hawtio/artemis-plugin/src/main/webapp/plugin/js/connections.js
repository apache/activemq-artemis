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

    ARTEMIS.ConnectionsController = function ($scope, $location, workspace, ARTEMISService, jolokia, localStorage, artemisConnection, artemisSession) {

        var artemisJmxDomain = localStorage['artemisJmxDomain'] || "org.apache.activemq.artemis";

        /**
         *  Required For Each Object Type
         */

        var objectType = "connection"
        var method = 'listConnections(java.lang.String, int, int)';
        var attributes = [
            {
                field: 'connectionID',
                displayName: 'ID',
                width: '*'
            },
            {
                field: 'clientID',
                displayName: 'Client ID',
                width: '*'
            },
            {
                field: 'users',
                displayName: 'Users',
                width: '*'
            },
            {
                field: 'protocol',
                displayName: 'Protocol',
                width: '*'
            },
            {
                field: 'sessionCount',
                displayName: 'Session Count',
                width: '*',
                cellTemplate: '<div class="ngCellText"><a ng-click="selectSessions(row)">{{row.entity.sessionCount}}</a></div>',
                sortable: false
            },
            {
                field: 'remoteAddress',
                displayName: 'Remote Address',
                width: '*'
            },
            {
                field: 'localAddress',
                displayName: 'Local Address',
                width: '*'
            },
            {
                field: 'creationTime',
                displayName: 'Creation Time',
                width: '*'
            }
        ];
        $scope.filter = {
            fieldOptions: [
                {id: 'CONNECTION_ID', name: 'Connection ID'},
                {id: 'CLIENT_ID', name: 'Client ID'},
                {id: 'USERS', name: 'Users'},
                {id: 'PROTOCOL', name: 'Protocol'},
                {id: 'SESSION_COUNT', name: 'Session Count'},
                {id: 'REMOTE_ADDRESS', name: 'Remote Address'},
                {id: 'LOCAL_ADDRESS', name: 'Local Address'},
                {id: 'SESSION_ID', name: 'Session ID'}
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
                sortBy: "connectionID"
            }
        };

        // Configure Parent/Child click through relationships
        if (artemisSession.session) {
            $scope.filter.values.field = $scope.filter.fieldOptions[0].id;
            $scope.filter.values.operation = $scope.filter.operationOptions[0].id;
            $scope.filter.values.value = artemisSession.session.connectionID;
            artemisSession.session = null;
        }

        $scope.selectSessions = function (connection) {
            artemisConnection.connection = connection.entity;
            $location.path("artemis/sessions");
        };
        $scope.closeConnection = function () {
            var connectionID = $scope.gridOptions.selectedItems[0].connectionID
           ARTEMIS.log.info("closing connection: " + connectionID);
           if (workspace.selection) {
              var mbean = getBrokerMBean(jolokia);
              if (mbean) {
                  jolokia.request({ type: 'exec',
                     mbean: mbean,
                     operation: 'closeConnectionWithID(java.lang.String)',
                     arguments: [connectionID] },
                     onSuccess($scope.loadTable(), { error: function (response) {
                        Core.defaultJolokiaErrorHandler("Could not close connection: " + response);
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
        $scope.sortOptions = {
            fields: ["connectionID"],
            columns: ["connectionID"],
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
        $scope.selectGridRow = function () {
            $scope.showClose =  $scope.gridOptions.selectedItems.length > 0;
        };
        function onError() {
            Core.notification("error", "Could not retrieve " + objectType + " list from Artemis.");
        }
        function onBadMBean() {
            Core.notification("error", "Could not retrieve " + objectType + " list. Wrong MBean selected.");
        }
        function populateTable(response) {
            $scope.gridOptions.selectedItems.length = 0;
            $scope.showClose = false;
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
