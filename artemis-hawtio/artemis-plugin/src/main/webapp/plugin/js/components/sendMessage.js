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
    Artemis.log.info("loading send message");
    Artemis._module.component('artemisSendMessage', {
        template:
            `<h1>Send Message
                <button type="button" class="btn btn-link jvm-title-popover"
                          uib-popover-template="'send-message-instructions.html'" popover-placement="bottom-left"
                          popover-title="Instructions" popover-trigger="'outsideClick'">
                    <span class="pficon pficon-help"></span>
                </button>
            </h1>

            <div class="alert alert-warning" ng-show="$ctrl.noCredentials">
                <span class="pficon pficon-warning-triangle-o"></span>
                <strong>No credentials set for endpoint!</strong>
                Please set your username and password in the
                <a href="#" class="alert-link" ng-click="$ctrl.openPrefs()">Preferences</a> page.
            </div>

            <div class="row artemis-message-configuration">

                <div class="col-sm-12">
                    <form>
                        <div class="form-group">
                            <label>Durable </label>
                            <input id="durable" type="checkbox" ng-model="$ctrl.durable" value="true">
                        </div>
                    </form>
                </div>
            </div>

            <h3>Headers</h3>

            <div class="form-group"  ng-if="$ctrl.headers.length > 0">
                <table class="scr-component-references-table table">
                    <tbody>
                        <tr class="input-group"  ng-repeat="header in $ctrl.headers">
                            <td><input type="text" class="form-control" ng-model="header.name" placeholder="Name" autocomplete="off" id="name"></td>
                            <td><input type="text" class="form-control" ng-model="header.value" placeholder="Value" autocomplete="off" id="value"></td>
                            <td><div class="input-group-prepend">
                                <button type="button" class="btn btn-default" title="Delete" ng-click="$ctrl.removeHeader(header)">
                                    <span class="pficon pficon-delete"></span>
                                </button>
                            </div></td>
                        </tr>
                    </tbody>
                </table>
            </div>

            <p>
                <button type="button" class="btn btn-primary artemis-add-message-button" ng-click="$ctrl.addHeader()">Add Header</button>
            </p>

            <h3>Body</h3>

            <form>
                <div class="form-group">
                    <div hawtio-editor="$ctrl.message" mode="codeMirrorOptions.mode.name"></div>
                </div>
                <div class="form-group">
                    <select class="form-control artemis-send-message-format" ng-model="codeMirrorOptions.mode.name">
                        <option value="javascript">JSON</option>
                        <option value="xml">XML</option>
                    </select>
                    <button class="btn btn-default" ng-click="$ctrl.formatMessage()"
                       title="Automatically pretty prints the message so its easier to read">Format
                    </button>
                </div>
            </form>

            <p>
                <button type="button" class="btn btn-primary artemis-send-message-button" ng-click="$ctrl.sendMessage($ctrl.durable)">Send message</button>
            </p>
            <script type="text/ng-template" id="send-message-instructions.html">
            <div>
                <p>
                    This page allows you to send a message to the chosen queue. The message will be of type <code>text</code>
                    message and it will be possible to add headers to the message. The sending of the message will be authenticated
                    using the username and password set ion <code>preferences</code>, if this is not set then these will
                    be null.
                </p>
            </div>
        </script>
        `,
        controller: SendMessageController
    })
    .name;
    Artemis.log.info("loaded queue " + Artemis.createQueueModule);

    function SendMessageController($route, $scope, $element, $timeout, workspace,  jolokia, localStorage, $location, artemisMessage) {
        Core.initPreferenceScope($scope, localStorage, {
            'durable': {
                'value': true,
                'converter': Core.parseBooleanValue
            }
        });
        var ctrl = this;
        ctrl.noCredentials = false;
        ctrl.durable = true;
        ctrl.message = "";
        ctrl.headers = [];
        // bind model values to search params...
        Core.bindModelToSearchParam($scope, $location, "tab", "subtab", "compose");
        Core.bindModelToSearchParam($scope, $location, "searchText", "q", "");
        // only reload the page if certain search parameters change
        Core.reloadWhenParametersChange($route, $scope, $location);
        ctrl.checkCredentials = function () {
            ctrl.noCredentials = (Core.isBlank(localStorage['artemisUserName']) || Core.isBlank(localStorage['artemisPassword']));
        };
        if ($location.path().indexOf('artemis') > -1) {
            ctrl.localStorage = localStorage;
            $scope.$watch('localStorage.artemisUserName', ctrl.checkCredentials);
            $scope.$watch('localStorage.artemisPassword', ctrl.checkCredentials);
            //prefill if it's a resent
            if (artemisMessage.message !== null) {
                ctrl.message = artemisMessage.message.bodyText;
                if (artemisMessage.message.PropertiesText !== null) {
                    for (var p in artemisMessage.message.StringProperties) {
                        ctrl.headers.push({name: p, value: artemisMessage.message.StringProperties[p]});
                    }
                }
            }
            // always reset at the end
            artemisMessage.message = null;
        }

        this.openPrefs = function () {
            Artemis.log.info("opening prefs");
            $location.path('/preferences').search({'pref': 'Artemis'});
        }

        var LANGUAGE_FORMAT_PREFERENCE = "defaultLanguageFormat";
        var sourceFormat = workspace.getLocalStorage(LANGUAGE_FORMAT_PREFERENCE) || "javascript";

        $scope.codeMirrorOptions = CodeEditor.createEditorSettings({
            mode: {
                name: sourceFormat
            }
        });

        $scope.$on('hawtioEditor_default_instance', function (event, codeMirror) {
            $scope.codeMirror = codeMirror;
        });

        ctrl.addHeader = function  () {
            ctrl.headers.push({name: "", value: ""});
            // lets set the focus to the last header
            if ($element) {
                $timeout(function () {
                    var lastHeader = $element.find("input.headerName").last();
                    lastHeader.focus();
                }, 100);
            }
        }

        this.removeHeader = function (header) {
            var index = ctrl.headers.indexOf(header);
            ctrl.headers.splice(index, 1);
        };

        ctrl.defaultHeaderNames = function () {
            var answer = [];

            function addHeaderSchema(schema) {
                angular.forEach(schema.definitions.headers.properties, function (value, name) {
                    answer.push(name);
                });
            }

            addHeaderSchema(Artemis.jmsHeaderSchema);
            return answer;
        };

        function operationSuccess() {
            Core.notification("success", "Message sent!");
            ctrl.headers = [];
            ctrl.message = "";
        };

        function onError(response) {
           Core.notification("error", "Could not send message: " + response.error);
        }

        ctrl.formatMessage = function () {
            CodeEditor.autoFormatEditor($scope.codeMirror);
        };
        ctrl.sendMessage = function (durable) {
            var body = ctrl.message;
            Artemis.log.info(body);
            doSendMessage(ctrl.durable, body);
        };

        function doSendMessage(durable, body) {
            var selection = workspace.selection;
            if (selection) {
                var mbean = selection.objectName;
                if (mbean) {
                    var headers = null;
                    if (ctrl.headers.length) {
                        headers = {};
                        angular.forEach(ctrl.headers, function (object) {
                            var key = object.name;
                            if (key) {
                                headers[key] = object.value;
                            }
                        });
                        Artemis.log.debug("About to send headers: " + JSON.stringify(headers));
                    }

                    var user = ctrl.localStorage["artemisUserName"];
                    var pwd = ctrl.localStorage["artemisPassword"];

                    if (!headers) {
                        headers = {};
                    }
                    var type = 3;
                    Artemis.log.debug(headers);
                    Artemis.log.debug(type);
                    Artemis.log.debug(body);
                    Artemis.log.debug(durable);
                    jolokia.execute(mbean, "sendMessage(java.util.Map, int, java.lang.String, boolean, java.lang.String, java.lang.String)", headers, type, body, durable, user, pwd,  Core.onSuccess(operationSuccess, { error: onError }));
                }
            }
        }
    }
    SendMessageController.$inject = ['$route', '$scope', '$element', '$timeout', 'workspace', 'jolokia', 'localStorage', '$location', 'artemisMessage'];

})(Artemis || (Artemis = {}));
