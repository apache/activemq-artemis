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
 /// <reference path="tree.component.ts"/>
var Artemis;
(function (Artemis) {
    Artemis.log.debug("loading status");
    Artemis._module.component('artemisStatus', {
        template:
               `<h1>Current Status
                <button type="button" class="btn btn-link jvm-title-popover"
                          uib-popover-template="'status-instructions.html'" popover-placement="bottom-left"
                          popover-title="Instructions" popover-trigger="'outsideClick'">
                    <span class="pficon pficon-help"></span>
                </button>
                </h1>
                <div class="container-fluid">
                     <div class="row">
                        <div class="col-md-3 text-center">
                            <label>Address Memory Used</label>
                            <p class="text-left">
                            <pf-donut-pct-chart config="$ctrl.addressMemoryConfig" data="$ctrl.addressMemoryData" center-label="$ctrl.addressMemoryLabel"></pf-donut-pct-chart>
                            </p>
                        </div>
                     </div>
                     <div class="row">
                         <div class="col-xs-12 col-sm-6 col-md-6 col-lg-4">
                               <pf-info-status-card status="$ctrl.infoStatus" show-top-border="true"></pf-info-status-card>
                         </div>
                     </div>
                     <div class="row">
                         <div class="col-xs-12 col-sm-6 col-md-6 col-lg-4">
                               <pf-info-status-card status="$ctrl.clusterInfoStatus" show-top-border="true"></pf-info-status-card>
                         </div>
                     </div>
                </div>
                <script type="text/ng-template" id="status-instructions.html">
                  <div>
                    <p>
                      This page allows you to view the current status of the broker as well as the status of the cluster it belongs to.
                    </p>
                    <p>
                    It also shows the current address memory usage in relationship to the <code>global-max-size</code>
                    </p>
                    <p>Note these metrics update every 5 seconds</p>
                  </div>
                </script>
        `,
        controller: StatusController
    })
    .name;
    Artemis.log.debug("loaded address " + Artemis.addressModule);

    function StatusController($scope, workspace, jolokia, localStorage, $interval) {
        var ctrl = this;
        var mbean = Artemis.getBrokerMBean(workspace, jolokia);

        StatusController.prototype.$onInit = function () {
            jolokia.request({ type: 'read', mbean: mbean, attribute: 'Version'}, Core.onSuccess(function(response) {
                ctrl.infoStatus.info[0] = "version: " + response.value;
            }));
            jolokia.request({ type: 'read', mbean: mbean, attribute: 'HAPolicy'}, Core.onSuccess(function(response) {
                ctrl.clusterInfoStatus.info[2] = "HA Policy: " + response.value;
            }));
            loadStatus();
            ctrl.promise = $interval(function () { return loadStatus(); }, 5000);
        };
        StatusController.prototype.$onDestroy = function () {
            $interval.cancel(this.promise);
        };

        ctrl.infoStatus = {
          "title":"Broker Info",
          "href":"#",
          "iconClass": "pficon pficon-ok",
          "info":[
            "version",
            "uptime:",
            "started:"
          ]
        };
        ctrl.clusterInfoStatus = {
          "title":"Cluster Info",
          "href":"#",
          "iconClass": "pficon pficon-ok",
          "info":[
            "Lives:",
            "Backups:",
            "HA Policy: :"
          ]
        };
        ctrl.addressMemoryConfig = {
            'chartId': 'addressMemoryChart',
            'units': 'MiB',
            'thresholds':{'warning':'75','error':'90'}
        };

        ctrl.addressMemoryData = {
            'used': '0',
            'total': '1000'
        };
        ctrl.addressMemoryLabel = "used";

        function loadStatus() {
            jolokia.request({ type: 'read', mbean: mbean, attribute: 'GlobalMaxSize'}, Core.onSuccess(function(response) { ctrl.addressMemoryData.total = (response.value / 1048576).toFixed(2); }));
            jolokia.request({ type: 'read', mbean: mbean, attribute: 'AddressMemoryUsage'}, Core.onSuccess(function(response) { ctrl.addressMemoryData.used = (response.value / 1048576).toFixed(2); }));
            jolokia.request({ type: 'read', mbean: mbean, attribute: 'Uptime'}, Core.onSuccess(function(response) {
                ctrl.infoStatus.info[1] = "uptime: " + response.value;
            }));
            jolokia.request({ type: 'read', mbean: mbean, attribute: 'Started'}, Core.onSuccess(function(response) {
                ctrl.infoStatus.info[2] = "started: " + response.value;
                if(response.value == false) {
                    ctrl.infoStatus.iconClass = "pficon pficon-error-circle-o";
                } else {
                    ctrl.infoStatus.iconClass = "pficon pficon-ok";
                }
            }));
            var lives = 0;
            var backups = 0;
            var response = jolokia.request({ type: 'exec', mbean: mbean, operation: 'listNetworkTopology()' }, Core.onSuccess(null));
            var responseValue = response.value;
            var brokers = angular.fromJson(responseValue);
            angular.forEach(brokers, function (broker) {
                if (broker.live) {
                    lives++;
                }
                if (broker.backup) {
                    backups++;
                }
            })
            ctrl.clusterInfoStatus.info[0] = "Lives: " + lives;
            ctrl.clusterInfoStatus.info[1] = "Backups: " + backups;
            if (ctrl.clusterInfoStatus.info[2] == "HA Policy: Replicated") {
                jolokia.request({ type: 'read', mbean: mbean, attribute: 'ReplicaSync'}, Core.onSuccess(function(response) {
                    ctrl.clusterInfoStatus.info[3] = "replicating: " + response.value;
                    if (response.value == false) {
                        ctrl.clusterInfoStatus.iconClass = "pficon pficon-error-circle-o";
                    } else {
                        ctrl.clusterInfoStatus.iconClass = "pficon pficon-ok";
                    }
                }));
            }
        }
    }
    StatusController.$inject = ['$scope', 'workspace', 'jolokia', 'localStorage', '$interval'];

})(Artemis || (Artemis = {}));