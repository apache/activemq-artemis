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
 * The main entry point for the Branding module
 */
var Branding = (function (Branding) {

  /**
   * The name of this plugin
   */
  Branding.pluginName = 'activemq-branding';

  /**
   * This plugin's logger instance
   */
  Branding.log = Logger.get('activemq-branding-plugin');

  /**
   * The top level path of this plugin on the server
   */
  Branding.contextPath = '/activemq-branding';

  /**
   * This plugin's AngularJS module instance.
   */
  Branding.module = angular.module(Branding.pluginName, [])
    .run(initPlugin);

  /**
   * Here you can overwrite hawtconfig.json by putting the JSON
   * data directly to configManager.config property.
   */
  function initPlugin(configManager, aboutService) {
    configManager.config = {
      "branding": {
        "appName": "Artemis Console",
        "appLogoUrl": `${Branding.contextPath}/plugin/img/activemq.png`,
        "companyLogoUrl": `${Branding.contextPath}/plugin/img/activemq.png`,
        "css": `${Branding.contextPath}/plugin/css/activemq.css`,
        "favicon": `${Branding.contextPath}/plugin/img/favicon.png`
      },
      "login": {
        "description": "ActiveMQ Artemis Management Console",
        "links": [
          {
            "url": "/user-manual/index.html",
            "text": "User Manual"
          },
          {
            "url": "https://activemq.apache.org/",
            "text": "Website"
          }
        ]
      },
      "about": {
        "title": "ActiveMQ Artemis Management Console",
        "productInfo": [],
        "additionalInfo": "",
        "imgSrc": `${Branding.contextPath}/plugin/img/activemq.png`
      },
      "disabledRoutes": []
    };

    aboutService.addProductInfo('Artemis', '${project.version}');
    // Calling this function is required to apply the custom css and
    // favicon settings
    Core.applyBranding(configManager);

    Branding.log.info(Branding.pluginName, "loaded");
  }
  initPlugin.$inject = ['configManager', 'aboutService'];

  return Branding;

})(Branding || {});

// tell the Hawtio plugin loader about our plugin so it can be
// bootstrapped with the rest of AngularJS
hawtioPluginLoader.addModule(Branding.pluginName);
