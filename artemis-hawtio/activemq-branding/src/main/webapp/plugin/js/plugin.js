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
 * The activemq hawtio theme
 *
 * @module activemqBranding
 * @main activemq
 */
var localStorage = Core.getLocalStorage();
localStorage['showWelcomePage'] = false;
var activemqBranding = (function (self) {

    self.log = Logger.get("activemq");
    self.context = '../activemq-branding/';
    self.pluginName = 'hawtio-activemq-branding';

    hawtioPluginLoader.registerPreBootstrapTask(function (task) {
        Themes.definitions['activemq'] = {
            label: 'activemq',
            file: self.context + 'plugin/css/activemq.css',
            loginBg: self.context + 'plugin/img/login-screen-background.png'
        };
        if (!('theme' in localStorage)) {
            localStorage['theme'] = 'activemq';
        }
        localStorage['showWelcomePage'] = false;
        Themes.brandings['activemq'] = {
            label: 'activemq',
            setFunc: function(branding) {
                branding.appName = 'ActiveMQ Management Console';
                branding.appLogo = self.context + 'plugin/img/activemq.png';
                branding.logoOnly = true;
                branding.fullscreenLogin = true;
                branding.css = self.context + 'plugin/css/branding.css';
                branding.favicon = self.context + 'plugin/img/favicon.png';
                branding.welcomePageUrl = self.context + 'plugin/doc/welcome.md';
                return branding;
            }
        }
        if (!('branding' in localStorage)) {
            localStorage['branding'] = 'activemq';
        }
        task();
    });

    self.module = angular.module(self.pluginName, ['hawtioCore']);
    self.module.run(function (branding) {
        self.log.info("ActiveMQ theme loaded");
        if (localStorage['theme'] != 'activemq' || localStorage['branding'] != 'activemq') {
            localStorage['theme'] = 'activemq';
            localStorage['branding'] = 'activemq';
            location.reload();
        }

        /**
         * By default tabs are pulled from the "container" perspective, here
         * we can define includes or excludes to customize the available tabs
         * in hawtio.  Use "href" to match from the start of a URL and "rhref"
         * to match a URL via regex string.
         * 
         * Currently we need to exclude provided diagnostics,
         * as it exposes proprietary Oracle JVM feature, flight recorder.
         *
         */
        window['Perspective']['metadata'] = {
            container: {
                label: "Container",
                lastPage: "#/help",
                topLevelTabs: {
                    excludes: [
                        {
                            href: "#/diagnostics"
                        }
                    ]
                }
            }
        };

    });
    return self;
})(activemqBranding || {});

hawtioPluginLoader.addModule(activemqBranding.pluginName);
