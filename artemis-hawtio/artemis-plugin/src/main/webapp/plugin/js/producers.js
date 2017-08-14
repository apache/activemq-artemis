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

    ARTEMIS.ProducersController = function ($scope, $location, workspace, ARTEMISService, jolokia, localStorage, artemisAddress, artemisSession, artemisProducer) {

        var artemisJmxDomain = localStorage['artemisJmxDomain'] || "org.apache.activemq.artemis";

        /**
         *  Required For Each Object Type
         */

        var objectType = "producer";
        var method = 'listProducers(java.lang.String, int, int)';
        var attributes = [
            {
                field: 'id',
                displayName: 'ID',
                width: '*'
            },
            {
                field: 'session',
                displayName: 'Session',
                width: '*',
                cellTemplate: '<div class="ngCellText"><a ng-click="selectSession(row)">{{row.entity.session}}</a></div>'
            },
            {
                field: 'clientID',
                displayName: 'Client ID',
                width: '*'
            },
            {
                field: 'protocol',
                displayName: 'Protocol',
                width: '*'
            },
            {
                field: 'user',
                displayName: 'User',
                width: '*'
            },
            {
                field: 'address',
                displayName: 'Address',
                width: '*',
                cellTemplate: '<div class="ngCellText"><a ng-click="selectAddress(row)">{{row.entity.address}}</a></div>'
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
            }
        ];
        $scope.filter = {
            fieldOptions: [
                {id: 'ID', name: 'ID'},
                {id: 'SESSION_ID', name: 'Session ID'},
                {id: 'CLIENT_ID', name: 'Client ID'},
                {id: 'USER', name: 'User'},
                {id: 'ADDRESS', name: 'Address'},
                {id: 'PROTOCOL', name: 'Protocol'},
                {id: 'REMOTE_ADDRESS', name: 'Remote Address'},
                {id: 'LOCAL_ADDRESS', name: 'Local Address'}
            ],
            operationOptions: [
                {id: 'EQUALS', name: 'Equals'},
                {id: 'CONTAINS', name: 'Contains'}
            ],
            values: {
                field: "",
                operation: "",
                value: "",
                sortOrder: "asc",
                sortBy: "ID"
            }
        };

        $scope.selectAddress = function (row) {
           artemisAddress.address = row.entity;
           $location.path("artemis/addresses");
        };

        $scope.selectSession = function (row) {
            artemisProducer.producer = row.entity;
            $location.path("artemis/sessions");
        };

        // Configure Parent/Child click through relationships
        if (artemisSession.session) {
            $scope.filter.values.field = $scope.filter.fieldOptions[1].id;
            $scope.filter.values.operation = $scope.filter.operationOptions[0].id;
            $scope.filter.values.value = artemisSession.session.id;
            artemisSession.session = null;
        }

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
        function onError() {
            Core.notification("error", "Could not retrieve " + objectType + " list from Artemis.");
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
