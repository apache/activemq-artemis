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
    Artemis._module.component('artemisCreateQueue', {
        template:
            `
             <h1>Create Queue
                 <button type="button" class="btn btn-link jvm-title-popover"
                           uib-popover-template="'create-queue-instructions.html'" popover-placement="bottom-left"
                           popover-title="Instructions" popover-trigger="'outsideClick'">
                     <span class="pficon pficon-help"></span>
                 </button>
             </h1>
             <form class="form-horizontal">

                 <div class="form-group">
                     <label class="col-sm-2 control-label" for="name-markup">Queue name</label>

                     <div class="col-sm-10">
                         <input id="name-markup" class="form-control" type="text" maxlength="300"
                                name="queueName" ng-model="$ctrl.queueName" placeholder="Queue name"/>
                     </div>
                 </div>
                 <div class="form-group">
                     <label class="col-sm-2 control-label">Routing type</label>

                     <div class="col-sm-10">
                         <label class="checkbox">
                             <input type="radio" ng-model="$ctrl.routingType" value="Anycast"> Anycast
                         </label>
                         <label class="checkbox">
                             <input type="radio" ng-model="$ctrl.routingType" value="Multicast"> Multicast
                         </label>
                     </div>
                 </div>

                 <div class="form-group">
                     <label class="col-sm-2 control-label">Durable</label>

                     <div class="col-sm-10">
                         <label class="checkbox">
                             <input type="checkbox" ng-model="$ctrl.durable">
                         </label>
                     </div>
                 </div>

                 <div class="form-group">
                    <label class="col-sm-2 control-label" for="filter-markup">Filter</label>

                    <div class="col-sm-10">
                        <input id="filter-markup" class="form-control" type="text" maxlength="300"
                         name="filter" ng-model="$ctrl.filter" placeholder="Filter"/>
                    </div>
                 </div>

                 <div class="form-group">
                    <label class="col-sm-2 control-label" for="maxConsumers-markup">Max consumers</label>

                    <div class="col-sm-10">
                        <input id="maxConsumers-markup" class="form-control" type="integer" maxlength="300"
                         name="filter" ng-model="$ctrl.maxConsumers" placeholder="Max consumers"/>
                    </div>
                 </div>

                 <div class="form-group">
                    <label class="col-sm-2 control-label">Purge when no consumers</label>

                    <div class="col-sm-10">
                         <label class="checkbox">
                         <input type="checkbox" ng-model="$ctrl.purgeWhenNoConsumers">
                    </label>
                 </div>

                 <div class="form-group">
                     <div class="col-sm-offset-2 col-sm-10">
                         <button type="submit" class="btn btn-primary"
                                 ng-click="$ctrl.createQueue($ctrl.queueName, $ctrl.routingType, $ctrl.durable, $ctrl.filter, $ctrl.maxConsumers, $ctrl.purgeWhenNoConsumers)"
                                 ng-disabled="!$ctrl.queueName">Create Queue
                         </button>
                     </div>
                 </div>

                 <div hawtio-confirm-dialog="$ctrl.createDialog"
                      ok-button-text="Create"
                      cancel-button-text="Cancel"
                      on-ok="$ctrl.createQueue($ctrl.queueName, $ctrl.routingType, $ctrl.durable, $ctrl.filter, $ctrl.maxConsumers, $ctrl.purgeWhenNoConsumers)">
                     <div class="dialog-body">
                         <p>Queue name <b>{{$ctrl.queueName}}</b> contains unrecommended characters: <code>:</code></p>
                         <p>This may cause unexpected problems. Are you really sure to create this {{$ctrl.uncapitalisedDestinationType()}}?</p>
                     </div>
                 </div>

             </form>
             <script type="text/ng-template" id="create-queue-instructions.html">
             <div>
                <p>
                    This page allows you to create a queue bound to the chosen address.
                </p>
                <p>
                    if you want the queue to support JMS like queues, i.e. point to point, then choose anycast. If you
                    want your address to support JMS like topic subscriptions, publish/subscribe, then choose multicast.
                  </p>
                  <p>
                    Selecting durable means that the queue will survive a restart of the broker.
                  </p>
                  <p>
                    Adding a filter expression will mean that only messages that match that filter will be routed to this queue:
                    see <a href="https://activemq.apache.org/components/artemis/documentation/latest/filter-expressions.html" target="_blank">Filter Expressions</a>
                  </p>
                  <p>
                    Max consumers will limit how many consumers can consume from a queue at any one time, -1 means no limit.
                  </p>
                  <p>
                    Purge on no consumers means the queue will not start receiving messages until a consumer is attached.
                    When the last consumer is detached from the queue.  The queue is purged (its messages are removed)
                    and will not receive any more messages until a new consumer is attached.
                  </p>
                </div>
             </script>
        `,
        controller: CreateQueueController
    })
    .name;
    Artemis.log.debug("loaded queue " + Artemis.createQueueModule);

    function CreateQueueController($scope, workspace, jolokia, localStorage) {
        Artemis.log.debug("loaded queue controller");
        var ctrl = this;
        Core.initPreferenceScope($scope, localStorage, {
            'durable': {
                'value': true,
                'converter': Core.parseBooleanValue
            },
            'maxConsumers': {
                'value': -1,
                'converter': parseInt,
                'formatter': parseInt
            },
            'purgeWhenNoConsumers': {
                'value': false,
                'converter': Core.parseBooleanValue
            }
        });
        var artemisJmxDomain = localStorage['artemisJmxDomain'] || "org.apache.activemq.artemis";
        ctrl.workspace = workspace;
        ctrl.maxConsumers = -1;
        ctrl.routingType = "Anycast";
        ctrl.filter = "";
        ctrl.purgeWhenNoConsumers = false;
        ctrl.durable = true;
        ctrl.queueName = '';

        $scope.$watch('workspace.selection', function () {
            workspace.moveIfViewInvalid();
        });

        function operationSuccess() {
            ctrl.queueName = "";
            ctrl.workspace.operationCounter += 1;
            Core.$apply($scope);
            Core.notification("success", $scope.message);
            ctrl.workspace.loadTree();
        }
        function onError(response) {
            Core.notification("error", "Could not create queue: " + response.error);
        }
        this.createQueue = function (queueName, routingType, durable, filter, maxConsumers, purgeWhenNoConsumers) {
            var mbean = Artemis.getBrokerMBean(workspace, jolokia);
            if (mbean) {
                var selection = workspace.selection;
                var entries = selection.entries;
                var address = entries["address"];
                if (address.charAt(0) === '"' && address.charAt(address.length -1) === '"')
                {
                    address = address.substr(1,address.length -2);
                }
                address = unescape(address)
                $scope.message = "Created queue " + queueName + " durable=" + durable + " filter=" + filter + " routing type=" + routingType + " max consumers=" + maxConsumers + " purge..=" + purgeWhenNoConsumers + " on address " + address;
                if (routingType == "Multicast") {
                    Artemis.log.debug($scope.message);
                    jolokia.execute(mbean, "createQueue(java.lang.String,java.lang.String,java.lang.String,java.lang.String,boolean,int,boolean,boolean)", address, "MULTICAST", queueName, filter, durable, maxConsumers, purgeWhenNoConsumers, true, Core.onSuccess(operationSuccess, { error: onError }));
                } else {
                   Artemis.log.debug($scope.message);
                   jolokia.execute(mbean, "createQueue(java.lang.String,java.lang.String,java.lang.String,java.lang.String,boolean,int,boolean,boolean)", address, "ANYCAST", queueName, filter, durable, maxConsumers, purgeWhenNoConsumers, true, Core.onSuccess(operationSuccess, { error: onError }));
                }
            }
        };

        // unescape name from JMX https://docs.oracle.com/en/java/javase/11/docs/api/java.management/javax/management/ObjectName.html#quote(java.lang.String)
        function unescape(input) {
            var result = input;

            result = result.replace('\\"', '"');
            result = result.replace("\\*", "*");
            result = result.replace("\\?", "?");
            result = result.replace("\\\\", "\\");

            return result;
        }
    }
    CreateQueueController.$inject = ['$scope', 'workspace', 'jolokia', 'localStorage'];

})(Artemis || (Artemis = {}));
