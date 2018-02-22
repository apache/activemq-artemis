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
 * @main ARTEMIS
 *
 * The main entrypoint for the ARTEMIS module
 *
 */
var ARTEMIS = (function(ARTEMIS) {

   /**
    * @property pluginName
    * @type {string}
    *
    * The name of this plugin
    */
   ARTEMIS.pluginName = "ARTEMIS";

   /**
    * @property log
    * @type {Logging.Logger}
    *
    * This plugin's logger instance
    */
   ARTEMIS.log = Logger.get(ARTEMIS.pluginName);

   /**
    * @property templatePath
    * @type {string}
    *
    * The top level path to this plugin's partials
    */
   ARTEMIS.templatePath = "../artemis-plugin/plugin/html/";

   /**
    * @property jmxDomain
    * @type {string}
    *
    * The JMX domain this plugin mostly works with
    */
   ARTEMIS.jmxDomain = "hawtio"

   /**
    * @property mbeanType
    * @type {string}
    *
    * The mbean type this plugin will work with
    */
   ARTEMIS.mbeanType = "ARTEMISHandler";

   /**
    * @property mbean
    * @type {string}
    *
    * The mbean's full object name
    */
   ARTEMIS.mbean = ARTEMIS.jmxDomain + ":type=" + ARTEMIS.mbeanType;

   /**
    * @property SETTINGS_KEY
    * @type {string}
    *
    * The key used to fetch our settings from local storage
    */
   ARTEMIS.SETTINGS_KEY = 'ARTEMISSettings';

   /**
    * @property module
    * @type {object}
    *
    * This plugin's angularjs module instance
    */
   ARTEMIS.module = angular.module(ARTEMIS.pluginName, ['bootstrap', 'ngResource', 'ui.bootstrap.dialog', 'hawtioCore', 'camel', 'hawtio-ui']);

   // set up the routing for this plugin, these are referenced by the subleveltabs added below
   ARTEMIS.module.config(function($routeProvider) {
      $routeProvider
         .when('/artemis/createAddress', {
            templateUrl: ARTEMIS.templatePath + 'createAddress.html'
         })
         .when('/artemis/deleteAddress', {
           templateUrl: ARTEMIS.templatePath + 'deleteAddress.html'
         })
         .when('/artemis/deleteQueue', {
            templateUrl: ARTEMIS.templatePath + 'deleteQueue.html'
         })
         .when('/artemis/createQueue', {
            templateUrl: ARTEMIS.templatePath + 'createQueue.html'
         })
         .when('/artemis/browseQueue', {
            templateUrl: ARTEMIS.templatePath + 'browseQueue.html'
         })
         .when('/artemis/diagram', {
            templateUrl: ARTEMIS.templatePath + 'brokerDiagram.html'
         })
         .when('/artemis/sendMessage', {
            templateUrl: ARTEMIS.templatePath + 'sendMessage.html'
         })
         .when('/artemis/connections', {
            templateUrl: ARTEMIS.templatePath + 'connections.html'
         })
         .when('/jmx/connections', {
            templateUrl: ARTEMIS.templatePath + 'connections.html'
         })
         .when('/artemis/sessions', {
            templateUrl: ARTEMIS.templatePath + 'sessions.html'
         })
         .when('/jmx/sessions', {
            templateUrl: ARTEMIS.templatePath + 'sessions.html'
         })
         .when('/artemis/consumers', {
            templateUrl: ARTEMIS.templatePath + 'consumers.html'
         })
         .when('/jmx/consumers', {
            templateUrl: ARTEMIS.templatePath + 'consumers.html'
         })
         .when('/artemis/producers', {
            templateUrl: ARTEMIS.templatePath + 'producers.html'
         })
         .when('/jmx/producers', {
            templateUrl: ARTEMIS.templatePath + 'producers.html'
         })
         .when('/artemis/addresses', {
            templateUrl: ARTEMIS.templatePath + 'addresses.html'
         })
         .when('/jmx/addresses', {
            templateUrl: ARTEMIS.templatePath + 'addresses.html'
         })
         .when('/artemis/queues', {
            templateUrl: ARTEMIS.templatePath + 'queues.html'
         })
         .when('/jmx/queues', {
            templateUrl: ARTEMIS.templatePath + 'queues.html'
      });
   });

   ARTEMIS.module.factory('artemisMessage', function () {
        return { 'message': null };
   });
   ARTEMIS.module.factory('artemisConnection', function () {
        return { 'connection': null };
   });
   ARTEMIS.module.factory('artemisSession', function () {
        return { 'session': null };
   });
   ARTEMIS.module.factory('artemisConsumer', function () {
        return { 'consumer': null };
   });
   ARTEMIS.module.factory('artemisProducer', function () {
        return { 'producer': null };
   });
   ARTEMIS.module.factory('artemisQueue', function () {
        return { 'queue': null };
   });
   ARTEMIS.module.factory('artemisAddress', function () {
        return { 'address': null };
   });

   // one-time initialization happens in the run function
   // of our module
   ARTEMIS.module.run(function(workspace, viewRegistry, helpRegistry, preferencesRegistry, localStorage, jolokia, ARTEMISService, $rootScope, preLogoutTasks) {
      // let folks know we're actually running
      ARTEMIS.log.info("plugin running " + jolokia);

      var artemisJmxDomain = localStorage['artemisJmxDomain'] || "org.apache.activemq.artemis";

      ARTEMISService.initArtemis();

      // tell hawtio that we have our own custom layout for
      // our view
      viewRegistry["artemis"] = ARTEMIS.templatePath + "artemisLayout.html";

      helpRegistry.addUserDoc("artemis", "../artemis-plugin/plugin/doc/help.md", function () {
         return workspace.treeContainsDomainAndProperties(artemisJmxDomain);
     });

      preferencesRegistry.addTab("Artemis", ARTEMIS.templatePath + "preferences.html", function () {
         return workspace.treeContainsDomainAndProperties(artemisJmxDomain);
      });

      // Add a top level tab to hawtio's navigation bar
      workspace.topLevelTabs.push({
         id: "artemis",
         content: "Artemis",
         title: "Artemis Broker",
         isValid: function (workspace) {
            return workspace.treeContainsDomainAndProperties(artemisJmxDomain);
         },
         href: function () {
            return "#/jmx/attributes?tab=artemis";
         },
         isActive: function () {
            return workspace.isLinkActive("artemis");
         }
      });

      workspace.subLevelTabs.push({
         content: '<i class="icon-plus"></i> Create',
         title: "Create a new address",
         isValid: function (workspace) {
            return isBroker(workspace, artemisJmxDomain) || isAddressFolder(workspace, artemisJmxDomain);
         },
         href: function () {
            return "#/artemis/createAddress";
         }
      });

      workspace.subLevelTabs.push({
         content: '<i class="icon-plus"></i> Delete',
         title: "Delete an address",
         isValid: function (workspace) {
            return isAddress(workspace, artemisJmxDomain);
         },
         href: function () {
            return "#/artemis/deleteAddress";
         }
      });

      workspace.subLevelTabs.push({
         content: '<i class="icon-plus"></i> Create',
         title: "Create a new queue",
         isValid: function (workspace) {
            return isAddress(workspace, artemisJmxDomain)
         },
         href: function () {
            return "#/artemis/createQueue"
         }
      });

      workspace.subLevelTabs.push({
         content: '<i class="icon-remove"></i> Delete',
         title: "Delete or purge this queue",
         isValid: function (workspace) {
            return isQueue(workspace, artemisJmxDomain)
         },
         href: function () {
            return "#/artemis/deleteQueue"
         }
      });

      workspace.subLevelTabs.push({
          content: '<i class="icon-envelope"></i> Browse',
          title: "Browse the messages on the queue",
          isValid: function (workspace) { return isQueue(workspace, artemisJmxDomain); },
          href: function () { return "#/artemis/browseQueue"; }
      });

      workspace.subLevelTabs.push({
          content: '<i class="icon-pencil"></i> Send',
          title: "Send a message to this address",
          isValid: function (workspace) { return isAddress(workspace, artemisJmxDomain) || isQueue(workspace, artemisJmxDomain); },
          href: function () { return "#/artemis/sendMessage"; }
      });

      workspace.subLevelTabs.unshift({
          content: '<i class="icon-picture"></i> Diagram&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;|',
          title: "View a diagram of the producers, destinations and consumers",
          isValid: function (workspace) { return workspace.isTopTabActive("artemis") || workspace.selectionHasDomain(artemisJmxDomain); },
          href: function () { return "#/artemis/diagram"; }
      });

      workspace.subLevelTabs.unshift({
          content: '<i class="icon-th-list"></i> Queues',
          title: "Manage Queues",
          isValid: function (workspace) { return workspace.isTopTabActive("artemis") || workspace.selectionHasDomain(artemisJmxDomain); },
          href: function () { if (workspace.isTopTabActive("artemis")) return "#/artemis/queues"; else return  "#/jmx/queues"; }
      });

      workspace.subLevelTabs.unshift({
          content: '<i class="icon-book"></i> Addresses',
          title: "Manage Addresses",
          isValid: function (workspace) { return workspace.isTopTabActive("artemis") || workspace.selectionHasDomain(artemisJmxDomain); },
          href: function () { if (workspace.isTopTabActive("artemis")) return "#/artemis/addresses"; else return  "#/jmx/addresses"; }
      });

      workspace.subLevelTabs.unshift({
          content: '<i class="icon-upload-alt"></i> Producers',
          title: "Manage Producers",
          isValid: function (workspace) { return workspace.isTopTabActive("artemis") || workspace.selectionHasDomain(artemisJmxDomain); },
          href: function () { if (workspace.isTopTabActive("artemis")) return "#/artemis/producers"; else return  "#/jmx/producers"; }
      });

      workspace.subLevelTabs.unshift({
          content: '<i class="icon-download-alt"></i> Consumers',
          title: "Manage Consumers",
          isValid: function (workspace) { return workspace.isTopTabActive("artemis") || workspace.selectionHasDomain(artemisJmxDomain); },
          href: function () { if (workspace.isTopTabActive("artemis")) return "#/artemis/consumers"; else return  "#/jmx/consumers"; }
      });

      workspace.subLevelTabs.unshift({
          content: '<i class="icon-tasks"></i> Sessions',
          title: "Manage Sessions",
          isValid: function (workspace) { return workspace.isTopTabActive("artemis") || workspace.selectionHasDomain(artemisJmxDomain); },
          href: function () { if (workspace.isTopTabActive("artemis")) return "#/artemis/sessions"; else return  "#/jmx/sessions"; }
      });

      workspace.subLevelTabs.unshift({
          content: '<i class="icon-signal"></i> Connections',
          title: "Manage Connections",
          isValid: function (workspace) { return workspace.isTopTabActive("artemis") || workspace.selectionHasDomain(artemisJmxDomain); },
          href: function () { if (workspace.isTopTabActive("artemis")) return "#/artemis/connections"; else return  "#/jmx/connections"; }
      });

      preLogoutTasks.addTask("clearArtemisCredentials", () => {
          localStorage.removeItem('artemisUserName');
          localStorage.removeItem('artemisPassword');
      });
});


   function isBroker(workspace, domain) {
      return workspace.hasDomainAndProperties(domain, {'broker': 'Broker'}, 3);
   }

   function isAddressFolder(workspace, domain) {
      return workspace.selectionHasDomainAndLastFolderName(domain, 'addresses');
   }

   function isAddress(workspace, domain) {
      return workspace.hasDomainAndProperties(domain, {'component': 'addresses'}) && !workspace.hasDomainAndProperties(domain, {'subcomponent': 'queues'});
   }

   function isQueue(workspace, domain) {
      return workspace.hasDomainAndProperties(domain, {'subcomponent': 'queues'});
   }

   return ARTEMIS;
}(ARTEMIS || {}));

// Very important!  Add our module to hawtioPluginLoader so it
// bootstraps our module
hawtioPluginLoader.addModule(ARTEMIS.pluginName);