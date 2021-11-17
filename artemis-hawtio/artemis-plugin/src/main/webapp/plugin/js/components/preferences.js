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
    Artemis._module
    .controller("Artemis.PreferencesController", ["$scope", "localStorage", "userDetails", "$rootScope", function ($scope, localStorage, userDetails, $rootScope) {
          Core.initPreferenceScope($scope, localStorage, {
             'artemisDLQ': {
                'value': "^DLQ$"
             },
             'artemisExpiryQueue': {
                'value': "^ExpiryQueue$"
             }
         })}])
   .run(configurePreferences)
   .name;

   function configurePreferences(preferencesRegistry, $templateCache, workspace) {

        var path = 'plugin/preferences.html';
        preferencesRegistry.addTab("Artemis", path, function () {
            return workspace.treeContainsDomainAndProperties("org.apache.activemq.artemis");
        });
        $templateCache.put(path,
            `<form class="form-horizontal artemis-preferences-form" ng-controller="Artemis.PreferencesController">
                  <div class="form-group">
                    <label class="col-md-2 control-label" for="artemisDLQ">
                      Dead-letter address regex
                      <span class="pficon pficon-info" data-toggle="tooltip" data-placement="top" title="A regular expression to match one or more dead-letter addresses"></span>
                    </label>
                    <div class="col-md-6">
                      <input type="text" id="artemisDLQ" ng-model="artemisDLQ">
                    </div>
                  </div>

                  <div class="form-group">
                    <label class="col-md-2 control-label" for="artemisExpiryQueue">
                      Expiry address regex
                      <span class="pficon pficon-info" data-toggle="tooltip" data-placement="top" title="A regular expression to match one or more expiry addresses"></span>
                    </label>
                    <div class="col-md-6">
                      <input type="text" id="artemisExpiryQueue" ng-model="artemisExpiryQueue">
                    </div>
                  </div>
            </form>`
        );
   }
   configurePreferences.$inject = ['preferencesRegistry', '$templateCache', 'workspace'];

})(Artemis || (Artemis = {}));
