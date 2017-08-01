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
 */
var ARTEMIS;
(function (ARTEMIS) {
    var DELIVERY_PERSISTENT = "2";
    ARTEMIS.SendMessageController = function($route, $scope, $element, $timeout, workspace, ARTEMISService,  jolokia, localStorage, $location, artemisMessage) {
        Core.initPreferenceScope($scope, localStorage, {
            'durable': {
                'value': true,
                'converter': Core.parseBooleanValue
            }
        });
        var log = Logger.get("ARTEMIS");
        $scope.noCredentials = false;
        $scope.showChoose = false;
        $scope.profileFileNames = [];
        $scope.profileFileNameToProfileId = {};
        $scope.selectedFiles = {};
        $scope.container = {};
        $scope.message = "\n\n\n\n";
        $scope.headers = [];
        // bind model values to search params...
        Core.bindModelToSearchParam($scope, $location, "tab", "subtab", "compose");
        Core.bindModelToSearchParam($scope, $location, "searchText", "q", "");
        // only reload the page if certain search parameters change
        Core.reloadWhenParametersChange($route, $scope, $location);
        $scope.checkCredentials = function () {
           ARTEMIS.log.info(localStorage['artemisUserName'] + " " + localStorage['artemisPassword']);
            $scope.noCredentials = (Core.isBlank(localStorage['artemisUserName']) || Core.isBlank(localStorage['artemisPassword']));
        };
        if ($location.path().has('artemis')) {
            $scope.localStorage = localStorage;
            $scope.$watch('localStorage.artemisUserName', $scope.checkCredentials);
            $scope.$watch('localStorage.artemisPassword', $scope.checkCredentials);
            //prefill if it's a resent
            if (artemisMessage.message !== null) {
                $scope.message = artemisMessage.message.bodyText;
                if (artemisMessage.message.PropertiesText !== null) {
                    for (var p in artemisMessage.message.StringProperties) {
                        $scope.headers.push({name: p, value: artemisMessage.message.StringProperties[p]});
                    }
                }
            }
            // always reset at the end
            artemisMessage.message = null;
        }
        $scope.openPrefs = function () {
            $location.search('pref', 'Artemis');
            $scope.$emit("hawtioOpenPrefs");
        };
        var LANGUAGE_FORMAT_PREFERENCE = "defaultLanguageFormat";
        var sourceFormat = workspace.getLocalStorage(LANGUAGE_FORMAT_PREFERENCE) || "javascript";
        // TODO Remove this if possible
        $scope.codeMirror = undefined;
        var options = {
            mode: {
                name: sourceFormat
            },
            // Quick hack to get the codeMirror instance.
            onChange: function (codeMirror) {
                if (!$scope.codeMirror) {
                    $scope.codeMirror = codeMirror;
                }
            }
        };
        $scope.codeMirrorOptions = CodeEditor.createEditorSettings(options);
        $scope.addHeader = function () {
            $scope.headers.push({name: "", value: ""});
            // lets set the focus to the last header
            if ($element) {
                $timeout(function () {
                    var lastHeader = $element.find("input.headerName").last();
                    lastHeader.focus();
                }, 100);
            }
        };
        $scope.removeHeader = function (header) {
            $scope.headers = $scope.headers.remove(header);
        };
        $scope.defaultHeaderNames = function () {
            var answer = [];

            function addHeaderSchema(schema) {
                angular.forEach(schema.definitions.headers.properties, function (value, name) {
                    answer.push(name);
                });
            }

            if (isJmsEndpoint()) {
                addHeaderSchema(ARTEMIS.jmsHeaderSchema);
            }
            /*if (isARTEMISEndpoint()) {
                addHeaderSchema(ARTEMIS.ARTEMISHeaderSchema);
            }*/
            return answer;
        };
        /* save the sourceFormat in preferences for later
         * Note, this would be controller specific preferences and not the global, overriding, preferences */
        // TODO Use ng-selected="changeSourceFormat()" - Although it seemed to fire multiple times..
        $scope.$watch('codeMirrorOptions.mode.name', function (newValue, oldValue) {
            workspace.setLocalStorage(LANGUAGE_FORMAT_PREFERENCE, newValue);
        });
        var sendWorked = function () {
            Core.notification("success", "Message sent!");
        };
        $scope.autoFormat = function () {
            setTimeout(function () {
                CodeEditor.autoFormatEditor($scope.codeMirror);
            }, 50);
        };
        $scope.sendMessage = function (durable) {
            var body = $scope.message;
           ARTEMIS.log.info(body);
            doSendMessage(durable, body, sendWorked);
        };
        function doSendMessage(durable, body, onSendCompleteFn) {
            var selection = workspace.selection;
            if (selection) {
                var mbean = selection.objectName;
                if (mbean) {
                    var headers = null;
                    if ($scope.headers.length) {
                        headers = {};
                        angular.forEach($scope.headers, function (object) {
                            var key = object.name;
                            if (key) {
                                headers[key] = object.value;
                            }
                        });
                        log.info("About to send headers: " + JSON.stringify(headers));
                    }
                    var callback = onSuccess(onSendCompleteFn);

                    ARTEMIS.log.info("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
                    var user = localStorage["artemisUserName"];
                    var pwd = localStorage["artemisPassword"];
                    // AMQ is sending non persistent by default, so make sure we tell to sent persistent by default
                    if (!headers) {
                        headers = {};
                    }
                    var type = 3;
                    ARTEMISService.artemisConsole.sendMessage(mbean, jolokia, headers, type, body, durable, user, pwd, callback, onSuccess(callback));

                }
            }
        }

        $scope.fileSelection = function () {
            var answer = [];
            angular.forEach($scope.selectedFiles, function (value, key) {
                if (value) {
                    answer.push(key);
                }
            });
            return answer;
        };
        $scope.sendSelectedFiles = function () {
            var filesToSend = $scope.fileSelection();
            var fileCount = filesToSend.length;
            var version = $scope.container.versionId || "1.0";

            function onSendFileCompleted(response) {
                if (filesToSend.length) {
                    var fileName = filesToSend.pop();
                    if (fileName) {
                        // lets load the file data...
                        var profile = $scope.profileFileNameToProfileId[fileName];
                        if (profile) {
                            var body = Fabric.getConfigFile(jolokia, version, profile, fileName);
                            if (!body) {
                                log.warn("No body for message " + fileName);
                                body = "";
                            }
                            doSendMessage(body, onSendFileCompleted);
                        }
                    }
                }
                else {
                    var text = Core.maybePlural(fileCount, "Message") + " sent!";
                    Core.notification("success", text);
                }
            }

            // now lets start sending
            onSendFileCompleted(null);
        };

        function isJmsEndpoint() {
            // TODO check for the jms/activemq endpoint in ARTEMIS or if its an activemq endpoint
            return true;
        }
    };
    return ARTEMIS;
} (ARTEMIS || {}));