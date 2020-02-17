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
    Artemis._module.component('artemisDeleteAddress', {
        template:
            `
             <h1>Delete Address
                 <button type="button" class="btn btn-link jvm-title-popover"
                           uib-popover-template="'delete-address-instructions.html'" popover-placement="bottom-left"
                           popover-title="Instructions" popover-trigger="'outsideClick'">
                     <span class="pficon pficon-help"></span>
                 </button>
             </h1>
             <p>
               <div class="alert alert-warning">
                 <span class="pficon pficon-warning-triangle-o"></span>
                 This operation cannot be undone. Please be careful!
               </div>
             </p>

             <h2>Delete address</h2>
             <p>Remove the address completely.</p>
             <button type="submit" class="btn btn-danger" ng-click="$ctrl.deleteDialog = true">
               Delete address
             </button>

             <div hawtio-confirm-dialog="$ctrl.deleteDialog"
                  title="Confirm delete address"
                  ok-button-text="Delete"
                  cancel-button-text="Cancel"
                  on-ok="$ctrl.deleteAddress()">
               <div class="dialog-body">
                 <p>You are about to delete address <b>{{ $ctrl.selectedName() }}</b>.</p>
                 <p>This operation cannot be undone so please be careful.</p>
               </div>
             </div>
             <script type="text/ng-template" id="delete-address-instructions.html">
             <div>
                <p>
                    This page allows you to delete the chosen address on the broker.
                </p>
                <p>
                    Note that this will only succeed if the address has no queues bound to it.
                </p>
                </div>
             </script>
        `,
        controller: DeleteAddressController
    })
    .name;
    Artemis.log.debug("loaded address " + Artemis.addressModule);

    function DeleteAddressController($scope, workspace, jolokia, localStorage) {
        var ctrl = this;
        ctrl.workspace = workspace;
        ctrl.deleteDialog = false;

        $onInit = function () {
            Artemis.log.debug("loaded address controller");
        }

        $scope.$watch('workspace.selection', function () {
            workspace.moveIfViewInvalid();
        });

        function operationSuccess() {
            // lets set the selection to the parent
            workspace.removeAndSelectParentNode();
            ctrl.workspace.operationCounter += 1;
            Core.$apply($scope);
            Core.notification("success", $scope.message);
            ctrl.workspace.loadTree();
        }

        function onError(response) {
            Core.notification("error", "Could not delete address: " + response.error);
        }

        ctrl.deleteAddress = function () {
            var selection = workspace.selection;
            var entries = selection.entries;
            var mbean = Artemis.getBrokerMBean(workspace, jolokia);
            Artemis.log.debug(mbean);
            if (mbean) {
                if (selection && jolokia && entries) {
                    var domain = selection.domain;
                    var name = entries["address"];
                    Artemis.log.debug("name = " + name)
                    name = Core.unescapeHTML(name);
                    if (name.charAt(0) === '"' && name.charAt(name.length -1) === '"')
                    {
                        name = name.substr(1,name.length -2);
                    }
                    name = Artemis.ownUnescape(name);
                    Artemis.log.debug(name);
                    var operation;
                    $scope.message = "Deleted address " + name;
                    jolokia.execute(mbean, "deleteAddress(java.lang.String)", name,  Core.onSuccess(operationSuccess, { error: onError }));
                }
            }
        };

        ctrl.selectedName = function () {
            var selection = ctrl.workspace.selection;
            return selection ? _.unescape(selection.text) : null;
        };
    }
    DeleteAddressController.$inject = ['$scope', 'workspace', 'jolokia', 'localStorage'];

})(Artemis || (Artemis = {}));
