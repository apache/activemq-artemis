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
    Artemis.log.debug("loading address send message");
    Artemis._module.component('artemisAddressSendMessage', {
        template:
            `<h1>Send Message
                <button type="button" class="btn btn-link jvm-title-popover"
                          uib-popover-template="'send-message-instructions.html'" popover-placement="bottom-left"
                          popover-title="Instructions" popover-trigger="'outsideClick'">
                    <span class="pficon pficon-help"></span>
                </button>
            </h1>

            <div class="row artemis-message-configuration">

                <div class="col-sm-12">
                    <form>
                        <div class="form-group">
                            <label>Durable </label>
                            <input id="durable" type="checkbox" ng-model="$ctrl.message.durable" value="true">
                            <button type="button" class="btn btn-link jvm-title-popover"
                                      uib-popover-template="'durable-info.html'" popover-placement="bottom-left"
                                      popover-title="Durable" popover-trigger="'mouseenter'">
                                <span class="pficon pficon-info"></span>
                            </button>
                        </div>
                        <div class="form-group">
                            <label>Create Message ID </label>
                            <input id="messageID" type="checkbox" ng-model="$ctrl.message.messageID" value="true">
                            <button type="button" class="btn btn-link jvm-title-popover"
                                      uib-popover-template="'message-id-info.html'" popover-placement="bottom-left"
                                      popover-title="Message ID" popover-trigger="'mouseenter'">
                                <span class="pficon pficon-info"></span>
                            </button>
                        </div>
                        <div class="form-group">
                            <label>Use current logon user </label>
                            <input id="useCurrentLogonUser" type="checkbox" ng-model="$ctrl.message.noCredentials" value="true">
                            <button type="button" class="btn btn-link jvm-title-popover"
                                      uib-popover-template="'use-current-logon-user-credentials-id-info.html'" popover-placement="bottom-left"
                                      popover-title="Use current logon user" popover-trigger="'mouseenter'">
                                <span class="pficon pficon-info"></span>
                            </button>
                        </div>
                        <div class="form-group" ng-hide="$ctrl.message.noCredentials">
                            <label class="col-sm-2 control-label" for="name-markup">Username</label>

                            <div class="col-sm-10">
                                <input id="name-markup" class="form-control" type="text" maxlength="300"
                                       name="username" ng-model="$ctrl.message.username" placeholder="username"/>
                            </div>
                        </div>
                        <div class="form-group" ng-hide="$ctrl.message.noCredentials">
                            <label class="col-sm-2 control-label" for="name-markup">Password</label>

                            <div class="col-sm-10">
                                <input id="name-markup" class="form-control" type="password" maxlength="300"
                                       name="password" ng-model="$ctrl.message.password" placeholder="password"/>
                            </div>
                        </div>
                    </form>
                </div>
            </div>

            <h3>Headers</h3>

            <div class="form-group"  ng-if="$ctrl.message.headers.length > 0">
                <table class="scr-component-references-table table">
                    <tbody>
                        <tr class="input-group"  ng-repeat="header in $ctrl.message.headers">
                            <td><input type="text" class="form-control" ng-model="header.name" placeholder="Name" autocomplete="off" id="name"></td>
                            <td><input type="text" class="form-control" ng-model="header.value" placeholder="Value" autocomplete="off" id="value"></td>
                            <td><div class="input-group-prepend">
                                <button type="button" class="btn btn-default" title="Delete" ng-click="$ctrl.message.removeHeader(header)">
                                    <span class="pficon pficon-delete"></span>
                                </button>
                            </div></td>
                        </tr>
                    </tbody>
                </table>
            </div>

            <p>
                <button type="button" class="btn btn-primary artemis-add-message-button" ng-click="$ctrl.message.addHeader()">Add Header</button>
            </p>

            <h3>Body</h3>

            <form>
                <div class="form-group">
                    <div hawtio-editor="$ctrl.message.message" mode="codeMirrorOptions.mode.name"></div>
                </div>
                <div class="form-group">
                    <select class="form-control artemis-send-message-format" ng-model="codeMirrorOptions.mode.name"
                        style="display:inline; width:auto">
                        <option value="javascript">JSON</option>
                        <option value="xml">XML</option>
                    </select>
                    <button class="btn btn-default" ng-click="$ctrl.message.formatMessage()"
                       title="Automatically pretty prints the message so it's easier to read"
                       style="vertical-align: initial">Format
                    </button>
                </div>
            </form>

            <p>
                <button type="button" class="btn btn-primary artemis-send-message-button" ng-click="$ctrl.message.sendMessage($ctrl.message.durable, $ctrl.message.messageID)">Send Message</button>
            </p>
            <script type="text/ng-template" id="send-message-instructions.html">
            <div>
                <p>
                    This page allows you to send a message to the chosen address. The message will be of type <code>text</code>
                    message and it will be possible to add headers to the message. The sending of the message will be authenticated
                    using the current logon user, unselect <code>use current logon user</code> to use a different user.
                </p>
            </div>
            </script>
            <script type="text/ng-template" id="message-id-info.html">
            <div>
                <p>
                    The Message ID is an automatically generated UUID that is set on the Message by the broker before it is routed.
                    If using a JMS client this would be the JMS Message ID on the JMS Message, this typically would not get
                    set for non JMS clients. Historically and on some other tabs this is also referred to as the User ID.
                </p>
            </div>
            </script>
            <script type="text/ng-template" id="durable-info.html">
            <div>
                <p>
                    If durable the message will be marked persistent and written to the brokers journal if the destination queue is durable.
                </p>
            </div>
            </script>
            <script type="text/ng-template" id="use-current-logon-user-credentials-id-info.html">
            <div>
                <p>
                    This option allows a user to send messages with the permissions of the user's current logon, disable it to send messages with different permissions than the user's current logon provides.
                </p>
            </div>
            </script>
        `,
        controller: AddressSendMessageController
    })
    .name;
    Artemis.log.debug("loaded queue " + Artemis.createQueueModule);

    function AddressSendMessageController($route, $scope, $element, $timeout, workspace,  jolokia, localStorage, $location, artemisMessage, messageCreator) {
        Core.initPreferenceScope($scope, localStorage, {
            'durable': {
                'value': true,
                'converter': Core.parseBooleanValue
            },
            'messageID': {
                'value': true,
                'converter': Core.parseBooleanValue
           }
        });
        var ctrl = this;
        ctrl.messageCreator = messageCreator;
        ctrl.message = ctrl.messageCreator.createNewMessage($scope, $location, $route, localStorage, artemisMessage, workspace, $element, $timeout, jolokia);

    }
    AddressSendMessageController.$inject = ['$route', '$scope', '$element', '$timeout', 'workspace', 'jolokia', 'localStorage', '$location', 'artemisMessage', 'messageCreator'];

})(Artemis || (Artemis = {}));
