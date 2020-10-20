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
    Artemis._module.component('artemisDeleteQueue', {
        template:
            `<p>
               <div class="alert alert-warning">
                 <span class="pficon pficon-warning-triangle-o"></span>
                 These operations cannot be undone. Please be careful!
               </div>
             </p>

             <h2>Delete queue
                 <button type="button" class="btn btn-link jvm-title-popover"
                           uib-popover-template="'delete-queue-instructions.html'" popover-placement="bottom-left"
                           popover-title="Instructions" popover-trigger="'outsideClick'">
                     <span class="pficon pficon-help"></span>
                 </button>
             </h2>
             <p>Remove the queue completely.</p>
             <button type="submit" class="btn btn-danger" ng-click="$ctrl.deleteDialog = true">
               Delete Queue
             </button>

             <div hawtio-confirm-dialog="$ctrl.deleteDialog"
                  title="Confirm delete address"
                  ok-button-text="Delete"
                  cancel-button-text="Cancel"
                  on-ok="$ctrl.deleteQueue()">
               <div class="dialog-body">
                 <p>You are about to delete queue <b>{{$ctrl.selectedName()}}</b>.</p>
                 <p>This operation cannot be undone so please be careful.</p>
               </div>
             </div>

             <h2>Purge queue
                 <button type="button" class="btn btn-link jvm-title-popover"
                           uib-popover-template="'purge-queue-instructions.html'" popover-placement="bottom-left"
                           popover-title="Instructions" popover-trigger="'outsideClick'">
                     <span class="pficon pficon-help"></span>
                 </button>
             </h2>
            <p>Remove all messages from queue.</p>
            <button type="submit" class="btn btn-danger" ng-click="$ctrl.purgeDialog = true">
            Purge Queue
            </button>

            <div hawtio-confirm-dialog="$ctrl.purgeDialog"
               title="Confirm purge address"
               ok-button-text="Purge"
               cancel-button-text="Cancel"
               on-ok="$ctrl.purgeQueue()">
            <div class="dialog-body">
              <p>You are about to purge queue <b>{{$ctrl.selectedName()}}</b>.</p>
              <p>This operation cannot be undone so please be careful.</p>
            </div>
            </div>
             <script type="text/ng-template" id="delete-queue-instructions.html">
             <div>
                <p>
                    This will delete the queue and all of the messages it holds
                </p>
                </div>
             </script>
             <script type="text/ng-template" id="purge-queue-instructions.html">
             <div>
                <p>
                    This will delete all of the messages held within the queue but will not delete the queue
                </p>
                </div>
             </script>
        `,
        controller: DeleteQueueController
    })
    .name;
    Artemis.log.debug("loaded delete queue " + Artemis.createQueueModule);

    function DeleteQueueController($scope, workspace, jolokia, localStorage) {
        Artemis.log.debug("loaded queue controller");
        var ctrl = this;
        ctrl.workspace = workspace;
        ctrl.deleteDialog = false;
        ctrl.purgeDialog = false;

        $scope.$watch('workspace.selection', function () {
            ctrl.workspace.moveIfViewInvalid();
        });

        function operationSuccess() {
           // lets set the selection to the parent
           ctrl.workspace.removeAndSelectParentNode();
           ctrl.workspace.operationCounter += 1;
           Core.$apply($scope);
           Core.notification("success", $scope.message);
           ctrl.workspace.loadTree();
        }

        function onError(response) {
           Core.notification("error", "Could not delete address: " + response.error);
        }

       function operationPurgeSuccess() {
           // lets set the selection to the parent
           /*$scope.workspace.operationCounter += 1;
           Core.$apply($scope);*/
           Core.notification("success", $scope.message);
       }

      function onPurgeError(response) {
          Core.notification("error", "Could not purge address: " + response.error);
      }

       this.deleteQueue = function () {
           var selection = ctrl.workspace.selection;
           var entries = selection.entries;
           var mbean = Artemis.getBrokerMBean(ctrl.workspace, jolokia);
           Artemis.log.debug(mbean);
           if (mbean) {
               if (selection && jolokia && entries) {
                   var domain = selection.domain;
                   var name = entries["queue"];
                   Artemis.log.debug("name = " + name)
                   name = Core.unescapeHTML(name);
                   if (name.charAt(0) === '"' && name.charAt(name.length -1) === '"')
                   {
                       name = name.substr(1,name.length -2);
                   }
                   name = Artemis.ownUnescape(name);
                   Artemis.log.debug(name);
                   var operation;
                   $scope.message = "Deleted queue " + name;
                   jolokia.execute(mbean, "destroyQueue(java.lang.String)", name,  Core.onSuccess(operationPurgeSuccess, { error: onError }));
               }
           }
       };

        this.purgeQueue = function () {
            var selection = ctrl.workspace.selection;
            var entries = selection.entries;
            var mbean = selection.objectName;
            if (selection && jolokia && entries) {
                var name = entries["Destination"] || entries["destinationName"] || selection.title;
                name = Core.unescapeHTML(name);
                $scope.message = "Purged queue " + name;
                jolokia.execute(mbean, "removeAllMessages()", Core.onSuccess(operationSuccess, { error: onPurgeError }));
            }
        };

        ctrl.selectedName = function () {
            var selection = ctrl.workspace.selection;
            return selection ? _.unescape(selection.text) : null;
        };
    }
    DeleteQueueController.$inject = ['$scope', 'workspace', 'jolokia', 'localStorage'];

})(Artemis || (Artemis = {}));
