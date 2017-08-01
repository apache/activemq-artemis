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
var activemqBranding = (function (self) {

    self.log = Logger.get("activemq");
    self.context = '../activemq-branding/';
    self.pluginName = 'hawtio-activemq-branding';

    hawtioPluginLoader.registerPreBootstrapTask(function (task) {
        Themes.definitions['activemq'] = {
            label: 'activemq',
            file: self.context + 'plugin/css/activemq.css',
            loginBg: self.context + 'plugin/img/login-screen-background.jpg'
        };
        var localStorage = Core.getLocalStorage();
        if (!('theme' in localStorage)) {
            localStorage['theme'] = 'activemq';
        }
        Themes.brandings['activemq'] = {
            label: 'activemq',
            setFunc: function(branding) {
                branding.appName = 'MANAGEMENT CONSOLE';
                branding.appLogo = self.context + 'plugin/img/activemq.png';
                branding.logoOnly = false;
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
        self.log.debug("ActivMQ theme loaded");
    });

    hawtioPluginLoader.addModule(self.pluginName);
    return self;
})(activemqBranding || {});

