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
 * The main entry point for the Simple module
 */
var Artemis = (function (Artemis) {

    /**
    * The name of this plugin
    */
    Artemis.pluginName = 'artemis-plugin';

    /**
    * This plugin's logger instance
    */
    Artemis.log = Logger.get('artemis-plugin');

    /**
    * The top level path of this plugin on the server
    */
    Artemis.contextPath = "/artemis-plugin/";

    Artemis.log.info("loading artemis plugin")
    Artemis._module = angular.module(Artemis.pluginName, [
        'angularResizable'
    ])
    .component('artemis', {
        template:
           `<div class="tree-nav-layout">
                <div class="sidebar-pf sidebar-pf-left" resizable r-directions="['right']">
                  <artemis-tree-header></artemis-tree-header>
                  <artemis-tree></artemis-tree>
                </div>
                <div class="tree-nav-main">
                  <jmx-header></jmx-header>
                  <artemis-navigation></artemis-navigation>
                  <div class="contents" ng-view></div>
                </div>
           </div>
          `
          })
    .run(configurePlugin);

  function configurePlugin(mainNavService, workspace, helpRegistry, preferencesRegistry, localStorage, preLogoutTasks, documentBase, $templateCache) {
        var artemisJmxDomain = localStorage['artemisJmxDomain'] || "org.apache.activemq.artemis";
        mainNavService.addItem({
            title: 'Artemis',
            basePath: '/artemis',
            template: '<artemis></artemis>',
            isValid: function () { return workspace.treeContainsDomainAndProperties(artemisJmxDomain); }
        });

        // clean up local storage upon logout
        preLogoutTasks.addTask('CleanupArtemisCredentials', function () {
            Artemis.log.debug("Clean up Artemis credentials in local storage");
            localStorage.removeItem('artemisUserName');
            localStorage.removeItem('artemisPassword');
        });
    }
    configurePlugin.$inject = ['mainNavService', 'workspace', 'helpRegistry', 'preferencesRegistry', 'localStorage', 'preLogoutTasks', 'documentBase', '$templateCache'];

  return Artemis;

})(Artemis || {});

// tell the Hawtio plugin loader about our plugin so it can be
// bootstrapped with the rest of AngularJS
hawtioPluginLoader.addModule(Artemis.pluginName);
