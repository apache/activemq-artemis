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

    Artemis._module.factory('messageCreator',
        function () {
            return {
                createNewMessage: function (scope, location, route, localStorage, artemisMessage, workspace, element, timeout, jolokia) {
                    return new message(scope, location, route, localStorage, artemisMessage, workspace, element, timeout, jolokia);
                }
            }
        })


        function message(scope, location, route, localStorage, artemisMessage, workspace, element, timeout, jolokia) {
            this.noCredentials = false,
            this.durable = true,
            this.message = "",
            this.headers = [],
            this.scope = scope;
            this.element = element;
            this.timeout = timeout;
            this.workspace = workspace;
            this.jolokia = jolokia;
            this.artemisMessage = artemisMessage;
            // bind model values to search params...
            Core.bindModelToSearchParam(scope, location, "tab", "subtab", "compose");
            Core.bindModelToSearchParam(scope, location, "searchText", "q", "");
            // only reload the page if certain search parameters change
            Core.reloadWhenParametersChange(route, scope, location, localStorage);
            if (location.path().indexOf('artemis') > -1) {
                this.localStorage = localStorage;
                scope.$watch('localStorage.artemisUserName', this.checkCredentials);
                scope.$watch('localStorage.artemisPassword', this.checkCredentials);
                //prefill if it's a resend
                if (artemisMessage.message !== null) {
                    this.message = artemisMessage.message.bodyText;
                    if (artemisMessage.message.PropertiesText !== null) {
                        for (var p in artemisMessage.message.StringProperties) {
                            this.headers.push({name: p, value: artemisMessage.message.StringProperties[p]});
                        }
                    }
                }
                // always reset at the end

                artemisMessage.message = null;
            }
            var LANGUAGE_FORMAT_PREFERENCE = "defaultLanguageFormat";
            var sourceFormat = workspace.getLocalStorage(LANGUAGE_FORMAT_PREFERENCE) || "javascript";

            scope.codeMirrorOptions = CodeEditor.createEditorSettings({
                mode: {
                    name: sourceFormat
                }
            });

            scope.$on('hawtioEditor_default_instance', function (event, codeMirror) {
                scope.codeMirror = codeMirror;
            });

            checkCredentials = function () {
                this.noCredentials = (Core.isBlank(localStorage['artemisUserName']) || Core.isBlank(localStorage['artemisPassword']));
            };
            this.openPrefs = function (location) {
                Artemis.log.debug("opening prefs");
                location.path('/preferences').search({'pref': 'Artemis'});
            };
            this.addHeader = function  () {
                this.headers.push({name: "", value: ""});
                // lets set the focus to the last header
                var element = this.element;
                if (element) {
                    this.timeout(function () {
                        var lastHeader = element.find("input.headerName").last();
                        lastHeader.focus();
                    }, 100);
                }
            };
            this.removeHeader = function (header) {
                var index = this.headers.indexOf(header);
                this.headers.splice(index, 1);
            };
            this.defaultHeaderNames = function () {
                var answer = [];

                function addHeaderSchema(schema) {
                    angular.forEach(schema.definitions.headers.properties, function (value, name) {
                        answer.push(name);
                    });
                }

                addHeaderSchema(Artemis.jmsHeaderSchema);
                return answer;
            };
            this.operationSuccess = function () {
                Core.notification("success", "Message sent!");
                this.headers = [];
                this.message = "";
            };
            this.onError = function (response) {
               Core.notification("error", "Could not send message: " + response.error);
            };
            this.formatMessage = function () {
                CodeEditor.autoFormatEditor(this.scope.codeMirror);
            };
            this.sendMessage = function (durable) {
                var body = this.message;
                Artemis.log.debug(body);
                this.doSendMessage(this.durable, body);
            };
            this.doSendMessage = function(durable, body) {
                var selection = this.workspace.selection;
                if (selection) {
                    var mbean = selection.objectName;
                    if (mbean) {
                        var headers = null;
                        if (this.headers.length) {
                            headers = {};
                            angular.forEach(this.headers, function (object) {
                                var key = object.name;
                                if (key) {
                                    headers[key] = object.value;
                                }
                            });
                            Artemis.log.debug("About to send headers: " + JSON.stringify(headers));
                        }

                        var user = this.localStorage["artemisUserName"];
                        var pwd = this.localStorage["artemisPassword"];

                        if (!headers) {
                            headers = {};
                        }
                        var type = 3;
                        Artemis.log.debug(headers);
                        Artemis.log.debug(type);
                        Artemis.log.debug(body);
                        Artemis.log.debug(durable);
                        this.jolokia.execute(mbean, "sendMessage(java.util.Map, int, java.lang.String, boolean, java.lang.String, java.lang.String)", headers, type, body, durable, user, pwd,  Core.onSuccess(this.operationSuccess(), { error: this.onError }));
                        Core.$apply(this.scope);
                    }
                }
            };
        }
})(Artemis || (Artemis = {}));






















