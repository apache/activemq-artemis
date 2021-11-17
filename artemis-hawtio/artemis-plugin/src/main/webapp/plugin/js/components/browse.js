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
    Artemis._module.component('artemisBrowseQueue', {
        template:
            `<h1>Browse Queue
            <button type="button" class="btn btn-link jvm-title-popover"
                      uib-popover-template="'browse-instructions.html'" popover-placement="bottom-left"
                      popover-title="Instructions" popover-trigger="'outsideClick'">
                <span class="pficon pficon-help"></span>
            </button>
            </h1>


            <div class="table-view artemis-browse-main" ng-show="!$ctrl.showMessageDetails">
                <div class="row toolbar-pf table-view-pf-toolbar" id="toolbar1">
                    <div class="col-sm-20">
                        <form class="toolbar-pf-actions">
                            <div class="form-group toolbar-pf-filter">
                                <div class="input-group">
                                    <input type="text" class="form-control" ng-model="$ctrl.filter" placeholder="Filter..." autocomplete="off" id="filterInput">
                                    <div class="input-group-btn">
                                        <button class="btn btn-link btn-find" ng-click="$ctrl.refresh()" type="button">
                                            &nbsp;&nbsp;<span class="fa fa-search"></span>&nbsp;&nbsp;
                                        </button>
                                    </div>
                                </div>
                            </div>
                            <div class="form-group">
                                    <button class="btn btn-default primary-action ng-binding ng-scope"
                                        type="button"
                                        title=""
                                        ng-click="$ctrl.reset()">Reset
                                    </button>
                                    <button class="btn btn-default primary-action ng-binding ng-scope"
                                        type="button"
                                        title=""
                                        ng-disabled="$ctrl.deleteDisabled"
                                        ng-click="$ctrl.openDeleteDialog()">Delete Messages
                                    </button>
                                    <button class="btn btn-default primary-action ng-binding ng-scope"
                                        type="button"
                                        title=""
                                        ng-disabled="$ctrl.moveDisabled"
                                        ng-click="$ctrl.openMoveDialog()">Move Messages
                                    </button>
                                    <button ng-show="$ctrl.dlq" class="btn btn-default primary-action ng-binding ng-scope"
                                        type="button"
                                        title=""
                                        ng-disabled="$ctrl.retryDisabled"
                                        ng-click="$ctrl.openRetryDialog()">Retry Messages
                                    </button>
                                    <button class="btn btn-default primary-action ng-binding ng-scope"
                                        type="button"
                                        title=""
                                        ng-click="$ctrl.showColumns = true">Columns
                                    </button>
                            </div>
                        </form>
                    </div>
                </div>
                <pf-table-view config="$ctrl.tableConfig"
                    columns="$ctrl.tableColumns"
                    items="$ctrl.messages"
                    dt-options="$ctrl.dtOptions"
                    action-buttons="$ctrl.tableMenuActions">
                </pf-table-view>
                <div ng-include="'plugin/artemispagination.html'"></div>

            </div>
            <div class="form-group" ng-show="$ctrl.showMessageDetails">
                <button class="btn btn-primary" ng-click="$ctrl.currentMessage.selected = false;$ctrl.showMessageDetails = false">Back</button>
                <button class="btn btn-primary" ng-click="$ctrl.currentMessage.selected = true;$ctrl.actionText = 'You are about to move message ID=' + $ctrl.currentMessage.messageID;$ctrl.moveDialog = true">Move</button>
                <button class="btn btn-primary" ng-click="$ctrl.currentMessage.selected = true;$ctrl.actionText = 'You are about to delete this message ID=' + $ctrl.currentMessage.messageID;$ctrl.deleteDialog = true">Delete</button>
                <button class="btn btn-primary" title="First Page"  ng-disabled="$ctrl.pagination.pageNumber == 1" ng-click="$ctrl.firstPage()"><i class="fa fa-fast-backward" aria-hidden="true"/></button>
                <button class="btn btn-primary" title="Previous Page" ng-disabled="$ctrl.pagination.pageNumber == 1" ng-click="$ctrl.previousPage()"><i class="fa fa-step-backward" aria-hidden="true"/></button>
                <button class="btn btn-primary" title="Previous Message" ng-disabled="$ctrl.pagination.pageNumber == 1 && $ctrl.currentMessage.idx == 0" ng-click="$ctrl.previousMessage()"><i class="fa fa-backward" aria-hidden="true"/></button>
                <button class="btn btn-primary" title="Next Message" ng-disabled="$ctrl.pagination.pageNumber == $ctrl.pagination.pages && $ctrl.currentMessage.idx >= ($ctrl.messages.length - 1)" ng-click="$ctrl.nextMessage()"><i class="fa fa-forward" aria-hidden="true"/></button>
                <button class="btn btn-primary" title="Next Page" ng-disabled="$ctrl.pagination.pageNumber == $ctrl.pagination.pages" ng-click="$ctrl.nextPage()"><i class="fa fa-step-forward" aria-hidden="true"/></button>
                <button class="btn btn-primary" title="Last Page" ng-disabled="$ctrl.pagination.pageNumber == $ctrl.pagination.pages" ng-click="$ctrl.lastPage()"><i class="fa fa-fast-forward" aria-hidden="true"/></button>
                <h4>Message ID: {{$ctrl.currentMessage.messageID}}</h4>

                <div>
                <span>Display as:</span>
                <!-- first 3 controls (text/hex/decimal) are always visible and are enabled/disabled -->
                <label id="textDisplayAsLabel" style="margin-left:8px"><input style="vertical-align: middle; margin-top: 0; margin-right:2px" type="radio" name="displayAs" value="text" id="textDisplayAsRadio" ng-disabled="!$ctrl.currentMessage.textDescr" ng-model="$ctrl.displayAs" ng-change="$ctrl.updatedDisplayAs();">Text</label>
                <label id="hexDisplayAsLabel" style="margin-left:8px"><input style="vertical-align: middle; margin-top: 0; margin-right:2px" type="radio" name="displayAs" value="hex" id="hexDisplayAsRadio" ng-disabled="!$ctrl.currentMessage.hexDescr" ng-model="$ctrl.displayAs" ng-change="$ctrl.updatedDisplayAs();">Hex</label>
                <label id="decimalDisplayAsLabel" style="margin-left:8px"><input style="vertical-align: middle; margin-top: 0; margin-right:2px" type="radio" name="displayAs" value="decimal" id="decimalDisplayAsRadio" ng-disabled="!$ctrl.currentMessage.decimalDescr" ng-model="$ctrl.displayAs" ng-change="$ctrl.updatedDisplayAs();">Decimal</label>
                <!-- other controls (amqp/large/compressed) are only visible when applicable and hidden otherwise -->
                <label id="amqpDisplayAsLabel" style="margin-left:8px" ng-show="!!$ctrl.currentMessage.amqpDescr"><input style="vertical-align: middle; margin-top: 0; margin-right:2px" type="radio" name="displayAs" value="amqp" id="amqpDisplayAsRadio" ng-model="$ctrl.displayAs" ng-change="$ctrl.updatedDisplayAs();">AMQP</label>
                <label id="largeDisplayAsLabel" style="margin-left:8px" ng-show="!!$ctrl.currentMessage.largeDescr"><input style="vertical-align: middle; margin-top: 0; margin-right:2px" type="radio" name="displayAs" value="large" id="largeDisplayAsRadio" ng-model="$ctrl.displayAs" ng-change="$ctrl.updatedDisplayAs();">Large</label>
                <label id="compressedDisplayAsLabel" style="margin-left:8px" ng-show="!!$ctrl.currentMessage.compressedDescr"><input style="vertical-align: middle; margin-top: 0; margin-right:2px" type="radio" name="displayAs" value="compressed" id="compressedDisplayAsRadio" ng-model="$ctrl.displayAs" ng-change="$ctrl.updatedDisplayAs();">Compressed</label>
                </div>

                <h4>Displaying body as <span ng-bind="$ctrl.currentMessage.theDescr"></span></h4>
                <div hawtio-editor="$ctrl.currentMessage.theBody" read-only="true" mode='mode'></div>

                <h4>Headers</h4>
                <pf-toolbar config="$ctrl.messageToolbarConfig"></pf-toolbar>
                <pf-table-view config="$ctrl.messageTableConfig"
                    columns="$ctrl.messageTableColumns"
                    items="$ctrl.currentMessage.headers">
                </pf-table-view>

                <h4>Properties</h4>
                <div ng-show="$ctrl.showMessageDetails">
                    <pf-toolbar config="$ctrl.messagePToolbarConfig"></pf-toolbar>
                    <pf-table-view config="$ctrl.messagePTableConfig"
                        columns="$ctrl.messageTableColumns"
                        items="$ctrl.currentMessage.properties">
                    </pf-table-view>
                </div>
            </div>

            <div hawtio-confirm-dialog="$ctrl.deleteDialog" title="Delete messages?"
               ok-button-text="Delete"
               cancel-button-text="Cancel"
               on-ok="$ctrl.deleteMessages()">
                <div class="dialog-body">
                    <p class="alert alert-warning">
                    <span class="pficon pficon-warning-triangle-o"></span>
                    This operation cannot be undone so please be careful.
                    </p>
                    <p>{{$ctrl.actionText}}</p>
                </div>
            </div>

            <div hawtio-confirm-dialog="$ctrl.moveDialog" title="Move messages?"
               ok-button-text="Move"
               cancel-button-text="Cancel"
               on-ok="$ctrl.moveMessages()">
                <div class="dialog-body">
                    <p class="alert alert-warning">
                    <span class="pficon pficon-warning-triangle-o"></span>
                    You cannot undo this operation.<br/>
                    Though after the move you can always move them back again.
                    </p>
                    <p>{{$ctrl.actionText}}</p>
                    <p>Move
                    <ng-pluralize count="$filter('filter')(ctrl.messages, {selected: true}).length"
                                  when="{'1': 'message', 'other': '{} messages'}"></ng-pluralize>
                    to: <select ng-model="$ctrl.queueName" ng-options="qn for qn in $ctrl.queueNames" ng-init="queueName=$ctrl.queueNames[0]"></select>
                   </p>
                </div>
            </div>

            <div hawtio-confirm-dialog="$ctrl.retryDialog" title="Retry messages?"
               ok-button-text="Retry"
               cancel-button-text="Cancel"
               on-ok="$ctrl.retryMessages()">
                <div class="dialog-body">
                    <p class="alert alert-warning">
                    <span class="pficon pficon-warning-triangle-o"></span>
                    You cannot undo this operation.<br/>
                    Though after the move you can always move them back again.
                    </p>
                    <p>{{$ctrl.actionText}}</p>
                </div>
            </div>
            <div hawtio-confirm-dialog="$ctrl.showColumns"
              title="Column Selector"
              cancel-button-text="Close"
              on-cancel="$ctrl.updateColumns()"
              show-ok-button="false">
                <div class="dialog-body ng-non-bindable" >
                    <table class="table-view-container table table-striped table-bordered table-hover dataTable ng-scope ng-isolate-scope no-footer">
                        <tr ng-repeat="col in $ctrl.dtOptions.columns">
                            <td>{{ col.name }}</td>
                            <td><input type="checkbox" ng-model="col.visible" placeholder="Name" autocomplete="off" id="name"></td>
                        </tr>
                    </table>
                </div>
            </div>
            <script type="text/ng-template" id="browse-instructions.html">
              <div>
                <p>
                  This page allows you to browse messages on a queue in Artemis. Messages are loaded in from the broker
                  a page at a time and can be filtered at the broker using the <code>filter</code>: see <a href="https://activemq.apache.org/components/artemis/documentation/latest/filter-expressions.html" target="_blank">Filter Expressions</a>
                  . To execute a query click on the <span class="fa fa-search"></span> button.
                </p>
                <p>
                    Clicking on the <code>show</code> buton will show the messages details in more detail including, headers, properties
                    and the body if viewable. Clicking on the <code>resend</code> button will navigate to the <code>Send Message</code>
                    tab and copy the message details so a copy of the message can be resent. You can also use the cassette
                    buttons to move to the next/previous message, next/previous page or first/last page.
                </p>
              </div>
            </script>
        `,
        controller: BrowseQueueController
    })
    .name;


    function BrowseQueueController($scope, workspace, jolokia, localStorage, artemisMessage, $location, $timeout, $filter, pagination) {
        var ctrl = this;
        ctrl.dlq = false;
        ctrl.deleteDisabled = true;
        ctrl.moveDisabled = true;
        ctrl.retryDisabled = true;
        ctrl.pagination = pagination;
        ctrl.pagination.reset();
        ctrl.filter = '';
        ctrl.actionText = '';

        ctrl.allMessages = [];
        ctrl.messages = [];

        var objName;
        if (workspace.selection) {
            objName = workspace.selection.objectName;
        } else {
        // in case of refresh
            var key = location.search()['nid'];
            var node = workspace.keyToNodeMap[key];
            objName = node.objectName;
        }
        var artemisDLQ = localStorage['artemisDLQ'] || "^DLQ$";
        var artemisExpiryQueue = localStorage['artemisExpiryQueue'] || "^ExpiryQueue$";
        Artemis.log.debug("loading table" + artemisExpiryQueue);
        if (objName) {
            ctrl.dlq = false;
            var addressName = jolokia.getAttribute(objName, "Address");
            if (addressName.match(artemisDLQ) != null || addressName.match(artemisExpiryQueue) != null) {
                ctrl.dlq = true;
            }
        }

        ctrl.dtOptions = {
           // turn of ordering as we do it ourselves
           ordering: false,
           columns: [
                {name: "Select", visible: true},
                {name: "Message ID", visible: true},
                {name: "Type", visible: true},
                {name: "Durable", visible: true},
                {name: "Priority", visible: true},
                {name: "Timestamp", visible: true},
                {name: "Expires", visible: true},
                {name: "Redelivered", visible: true},
                {name: "Large", visible: true},
                {name: "Persistent Size", visible: true},
                {name: "User ID", visible: true},
                {name: "Validated User", visible: false}
           ]
        };

        Artemis.log.debug('sessionStorage: browseColumnDefs =', localStorage.getItem('browseColumnDefs'));
        if (localStorage.getItem('browseColumnDefs')) {
            loadedDefs = JSON.parse(localStorage.getItem('browseColumnDefs'));
            //sanity check to make sure columns havent been added
            if(loadedDefs.length === ctrl.dtOptions.columns.length) {
                ctrl.dtOptions.columns = loadedDefs;
            }
        }

        ctrl.updateColumns = function () {
            var attributes = [];
            ctrl.dtOptions.columns.forEach(function (column) {
                attributes.push({name: column.name, visible: column.visible});
            });
            Artemis.log.debug("saving columns " + JSON.stringify(attributes));
            localStorage.setItem('browseColumnDefs', JSON.stringify(attributes));
        }

        ctrl.tableConfig = {
            onCheckBoxChange: handleCheckBoxChange,
            selectionMatchProp: 'messageID',
            showCheckboxes: true
        };
        ctrl.tableColumns = [
            {
                itemField: 'messageID',
                header: 'Message ID'
            },
            {
                itemField: 'type',
                header: 'Type',
                templateFn: function(value) {
                    return formatType(value);
                }
            },
            {
                itemField: 'durable',
                header: 'Durable'
            },
            {
                itemField: 'priority',
                header: 'Priority'
            },
            {
                itemField: 'timestamp',
                header: 'Timestamp',
                templateFn: function(value) {
                   return formatTimestamp(value);
                }
            },
            {
                itemField: 'expiration',
                header: 'Expires',
                templateFn: function(value) {
                    return formatExpires(value, false);
                }
            },
            {
                header: 'Redelivered',
                itemField: 'redelivered'
            },
            {
                itemField: 'largeMessage',
                header: 'Large'
            },
            {
                itemField: 'persistentSize',
                header: 'Persistent Size',
                templateFn: function(value) {
                    return formatPersistentSize(value);
                }
            },
            {
                itemField: 'userID',
                header: 'User ID'
            },
            {
                itemField: 'validatedUser',
                header: 'Validated User',
                templateFn: function(value) {
                    return value._AMQ_VALIDATED_USER;
                }
            }

        ];

        if (ctrl.dlq) {
            origQueue = {
                itemField: 'StringProperties',
                header: 'Original Queue',
                templateFn: function(value) {
                    return (value['_AMQ_ORIG_QUEUE'] ? value['_AMQ_ORIG_QUEUE'] : value['extraProperties._AMQ_ORIG_QUEUE']);
                }
            };
            ctrl.tableColumns.push(origQueue);

            ctrl.dtOptions.columns.push({name: "Original Queue", visible: true});
        }

        var resendConfig = {
            name: 'Resend',
            title: 'Resend message',
            actionFn: resendMessage
        };

        var showConfig = {
            name: 'Show',
            title: 'Show message',
            actionFn: openMessageDialog
        };

        ctrl.messageTableConfig = { selectionMatchProp: 'key', itemsAvailable: true, showCheckboxes: false };
        ctrl.messagePTableConfig = { selectionMatchProp: 'key', itemsAvailable: true, showCheckboxes: false };
        ctrl.messageToolbarConfig = {
            isTableView: true
        };
        ctrl.messagePToolbarConfig = {
            isTableView: true
        };

        ctrl.messageTableColumns = [
        {
            itemField: 'key',
            header: 'key'
        },
        {
            itemField: 'value',
            header: 'value'
        }];

        ctrl.tableMenuActions = [ showConfig, resendConfig ];

        ctrl.sysprops = [];

        Artemis.log.debug("loaded browse 5" + Artemis.browseQueueModule);
        ctrl.currentMessage;

        ctrl.queueNames = [];
        ctrl.queueName = '';
        ctrl.resultSizeDialog = false;
        //success message
        ctrl.message = '';
        //error message
        ctrl.errorMessage = '';
        $scope.mode = 'text';
        ctrl.deleteDialog = false;
        ctrl.moveDialog = false;
        ctrl.retryDialog = false;
        ctrl.showMessageDetails = false;
        ctrl.displayAs = null;

        var ignoreColumns = ["PropertiesText", "theBody", "theDescr", "textBody", "textDescr", "bytesBody", "hexBody", "hexDescr", "decimalBody", "decimalDescr", "amqpBody", "amqpDescr", "largeBody", "largeDescr", "compressedBody", "compressedDescr", "headers", "properties", "idx", "selected"];
        var flattenColumns = ["BooleanProperties", "ByteProperties", "ShortProperties", "IntProperties", "LongProperties", "FloatProperties", "DoubleProperties", "StringProperties"];

        function openMessageDialog(action, item) {
            ctrl.currentMessage = item;
            ctrl.currentMessage.headers = createHeaders(ctrl.currentMessage)
            ctrl.currentMessage.properties = createProperties(ctrl.currentMessage);
            createBodyText(ctrl, ctrl.currentMessage);
            ctrl.showMessageDetails = true;
        };

        ctrl.previousMessage = function() {
            ctrl.currentMessage.selected = false;
            nextIdx = ctrl.currentMessage.idx - 1;
            if (nextIdx < 0) {
                ctrl.pagination.previousPage();
                ctrl.loadPrevousPage = true;
                //we return here and let the next table load in and move to message idx 0
                return;
            }
            nextMessage =  ctrl.messages.find(tree => tree.idx == nextIdx);
            ctrl.currentMessage = nextMessage;
            ctrl.currentMessage.headers = createHeaders(ctrl.currentMessage)
            ctrl.currentMessage.properties = createProperties(ctrl.currentMessage);
            createBodyText(ctrl, ctrl.currentMessage);
        };

        ctrl.nextMessage = function() {
            ctrl.currentMessage.selected = false;
            nextIdx = ctrl.currentMessage.idx + 1;
            if (nextIdx == ctrl.pagination.pageSize) {
                ctrl.pagination.nextPage();
                //we return here and let the next table load in and move to messae idx 0
                return;
            }
            nextMessage =  ctrl.messages.find(tree => tree.idx == nextIdx);
            ctrl.currentMessage = nextMessage;
            ctrl.currentMessage.headers = createHeaders(ctrl.currentMessage)
            ctrl.currentMessage.properties = createProperties(ctrl.currentMessage);
            createBodyText(ctrl, ctrl.currentMessage);
        };

        ctrl.previousPage = function() {
            ctrl.pagination.previousPage();
        };

        ctrl.nextPage = function() {
            ctrl.pagination.nextPage();
        };

        ctrl.firstPage = function() {
            ctrl.pagination.firstPage();
        };

        ctrl.lastPage = function() {
            ctrl.pagination.lastPage();
        };

        var MS_PER_SEC  = 1000;
        var MS_PER_MIN  = 60 * MS_PER_SEC;
        var MS_PER_HOUR = 60 * MS_PER_MIN;
        var MS_PER_DAY  = 24 * MS_PER_HOUR;

        function pad2(value) {
            return (value < 10 ? '0' : '') + value;
        }

        function formatExpires(timestamp, addTimestamp) {
             if (isNaN(timestamp) || typeof timestamp !== "number") {
                return timestamp;
             }
             if (timestamp == 0) {
                return "never";
             }
             var expiresIn = timestamp - Date.now();
             if (Math.abs(expiresIn) < MS_PER_DAY) {
                var duration = expiresIn < 0 ? -expiresIn : expiresIn;
                var hours = pad2(Math.floor((duration / MS_PER_HOUR) % 24));
                var mins  = pad2(Math.floor((duration / MS_PER_MIN) % 60));
                var secs  = pad2(Math.floor((duration / MS_PER_SEC) % 60));
                var ret;
                if (expiresIn < 0) {
                   // "HH:mm:ss ago"
                   ret = hours + ":" + mins + ":" + secs + " ago";
                } else {
                   // "in HH:mm:ss"
                   ret = "in " + hours + ":" + mins + ":" + secs;
                }
                if (addTimestamp) {
                   ret += ", at " + formatTimestamp(timestamp);
                }
                return ret;
             }
             return formatTimestamp(timestamp);
          }

          function formatTimestamp(timestamp) {
             if (isNaN(timestamp) || typeof timestamp !== "number") {
                return timestamp;
             }
             if (timestamp === 0) {
                return "N/A";
             }
             var d = new Date(timestamp);
             // "yyyy-MM-dd HH:mm:ss"
             //add 1 to month as getmonth returns the position not the actual month
             return d.getFullYear() + "-" + pad2(d.getMonth() + 1) + "-" + pad2(d.getDate()) + " " + pad2(d.getHours()) + ":" + pad2(d.getMinutes()) + ":" + pad2(d.getSeconds());
          }

        var typeLabels = ["default", "1", "object", "text", "bytes", "map", "stream", "embedded"];
        function formatType(type) {
            if (isNaN(type) || typeof type !== "number") {
                return type;
            }
            return type > -1 && type < typeLabels.length ? typeLabels[type] : type
        }

        var jmsTypeLabels = ["message", "object", "map", "bytes", "stream", "text"];
        function formatJmsType(type) {
            if (isNaN(type) || typeof type !== "number") {
                return type;
            }
            return type > -1 && type < jmsTypeLabels.length ? jmsTypeLabels[type] : type
        }

        var bindingTypeLabels = ["local-queue", "remote-queue", "divert"];
        function formatBindingType(type) {
            if (isNaN(type) || typeof type !== "number") {
                return type;
            }
            return type > -1 && type < bindingTypeLabels.length ? bindingTypeLabels[type] : type
        }

        var destTypeLabels = ["queue", "topic", "temp-queue", "temp-topic"];
        function formatDestType(type) {
            if (isNaN(type) || typeof type !== "number") {
                return type;
            }
            return type > -1 && type < destTypeLabels.length ? destTypeLabels[type] : type
        }

        ctrl.refresh = function() {
            Artemis.log.debug(ctrl.filter)
            //if refreshing always return to the first page
            ctrl.pagination.firstPage();
        }

        ctrl.reset = function() {
            ctrl.filter = '';
            //if resetting always return to the first page
            ctrl.pagination.firstPage();
        }

        function formatPersistentSize(bytes) {
            if(isNaN(bytes) || typeof bytes !== "number" || bytes < 0) return "N/A";
            if(bytes < 10240) return bytes.toLocaleString() + " Bytes";
            if(bytes < 1048576) return (bytes / 1024).toFixed(2) + " KiB";
            if(bytes < 1073741824) return (bytes / 1048576).toFixed(2) + " MiB";
            return (bytes / 1073741824).toFixed(2) + " GiB";
        }

        ctrl.openMoveDialog = function () {
            var selectedItems = $filter('filter')(ctrl.messages, {selected: true});
            if(!selectedItems) {
                return;
            }
            ctrl.actionText = "You are about to move " + Core.maybePlural(selectedItems.length, "message");
            Artemis.log.debug(ctrl.actionText);
            ctrl.moveDialog = true;
        };

        ctrl.moveMessages = function (action, item) {
            var selection = workspace.selection;
            var mbean = selection.objectName;
            if (mbean && selection) {
                var selectedItems = $filter('filter')(ctrl.messages, {selected: true});
                if(!selectedItems) {
                    selectedItems = [];
                    return;
                }
                ctrl.message = "Moved " + Core.maybePlural(selectedItems.length, "message") + " to " + ctrl.queueName;
                ctrl.errorMessage = "failed to move message";
                angular.forEach(selectedItems, function(item, idx) {
                    var id = item.messageID;
                    if (id) {
                        var callback = (idx + 1 < selectedItems.length) ? intermediateResult : moveSuccess;
                        jolokia.execute(mbean, "moveMessage(long,java.lang.String)", id,  ctrl.queueName, Core.onSuccess(callback, { error: onError }));
                    }
                });
            }
        };

        function resendMessage(action, item) {
            // always assume a single message
            artemisMessage.message = item;
            $location.path('artemis/artemisSendMessage');
        };

        function onError(response) {
            Core.notification("error", ctrl.errorMessage + response.error);
        }

        function handleCheckBoxChange (item) {
            var selectedItems = $filter('filter')(ctrl.messages, {selected: true});
            Artemis.log.debug("sel " + selectedItems.length);
            if (selectedItems.length == 0) {
                ctrl.deleteDisabled = true;
                ctrl.moveDisabled = true;
                ctrl.retryDisabled = true;
                return;
            }
            ctrl.deleteDisabled = false;
            ctrl.moveDisabled = false;
            ctrl.retryDisabled = false;
        }

        ctrl.openDeleteDialog = function () {
            var selectedItems = $filter('filter')(ctrl.messages, {selected: true});
            if(!selectedItems) {
                selectedItems = [];
                return;
            }
            ctrl.actionText = "You are about to delete " + Core.maybePlural(selectedItems.length, "message");
            Artemis.log.debug(ctrl.actionText);
            ctrl.deleteDialog = true;
        }

        ctrl.deleteMessages = function () {
            var selection = workspace.selection;
            var mbean = selection.objectName;
            if (mbean && selection) {
                var selectedItems = $filter('filter')(ctrl.allMessages, {selected: true});
                if(!selectedItems) {
                    selectedItems = [];
                    return;
                }
                ctrl.message = "Deleted " + Core.maybePlural(selectedItems.length, "message");
                ctrl.errorMessage = "failed to delete message";
                angular.forEach(selectedItems, function(item, idx) {
                    var id = item.messageID;
                    if (id) {
                    var callback = (idx + 1 < selectedItems.length) ? intermediateResult : operationSuccess;
                        jolokia.execute(mbean, "removeMessage(long)", id, Core.onSuccess(callback, { error: onError }));
                    }
                });
            }
        };

        ctrl.openRetryDialog = function () {
            var selectedItems = $filter('filter')(ctrl.messages, {selected: true});
            if(!selectedItems) {
                return;
            }
            ctrl.actionText = "You are about to retry " + Core.maybePlural(selectedItems.length, "message");
            Artemis.log.debug(ctrl.actionText);
            ctrl.retryDialog = true;
        };

        ctrl.retryMessages = function() {
            var selection = workspace.selection;
            var mbean = selection.objectName;
            if (mbean && selection) {
                var selectedItems = $filter('filter')(ctrl.messages, {selected: true});
                ctrl.message = "Retry " + Core.maybePlural(selectedItems.length, "message");
                ctrl.errorMessage = "failed to retry message";
                angular.forEach(selectedItems, function(item, idx) {
                    var id = item.messageID;
                    if (id) {
                        var callback = (idx + 1 < selectedItems.length) ? intermediateResult : operationSuccess;
                        jolokia.execute(mbean, "retryMessage(long)", id,  Core.onSuccess(callback, { error: onError }));
                    }
                });
            }
        };

        function populateTable(response) {
            Artemis.log.debug("loading data:" + data);
            if (ctrl.queueNames.length === 0) {
                var queueNames = getSelectionQueuesFolder(workspace);
                var selectedQueue = workspace.selection.text;
                ctrl.queueNames = queueNames.filter(function (name) { return name !== selectedQueue; });
            }
            var data = response.value;

            if (!angular.isArray(data)) {
                ctrl.allMessages = [];
                angular.forEach(data, function(value, idx) {
                    ctrl.allMessages.push(value);
                })
            } else {
                ctrl.allMessages = data;
            }
            idx = 0;
            angular.forEach(ctrl.allMessages, function(message) {
                createBodyText(ctrl, message);
                if (idx == 0 && !ctrl.loadPrevousPage) {
                //always load n the first message for pagination when viewing message details
                    ctrl.currentMessage = message;
                    ctrl.currentMessage.headers = createHeaders(ctrl.currentMessage)
                    ctrl.currentMessage.properties = createProperties(ctrl.currentMessage);
                }
                else if (idx == (pagination.pageSize - 1) && ctrl.loadPrevousPage) {
                    delete ctrl.loadPrevousPage;
                    ctrl.currentMessage = message;
                    ctrl.currentMessage.headers = createHeaders(ctrl.currentMessage)
                    ctrl.currentMessage.properties = createProperties(ctrl.currentMessage);
                }
                message.idx = idx;
                idx++;
            });
            ctrl.messages = ctrl.allMessages;
            ctrl.isLoading = false;
            Core.$apply($scope);
            createBodyText(ctrl, ctrl.currentMessage);
        }

        function findFolder(node) {
            if (!node) {
                return null;
            }
            var answer = [];

            var addresses = node.children;

            angular.forEach(addresses, function (address) {
                var subQueues = address.children;
                angular.forEach(subQueues, function (subQueue) {
                    var routingTypes = subQueue.children;
                    angular.forEach(routingTypes, function (routingType) {
                        var queues = routingType.children;
                        angular.forEach(queues, function (queue) {
                            answer.push(queue.title);
                        });
                    });
                });
            });
            return answer;
        }

        function findAddressesNode(node) {
            if (!node) {
                return null;
            }
            if (node.title === "addresses") {
                return node;
            }
            if (node.title == Artemis.jmxDomain) {
                return null;
            }
            return findAddressesNode(node.parent);
        }

        function getSelectionQueuesFolder(workspace) {
            var selection = workspace.selection;
            var addressesNode = findAddressesNode(selection);
            var queueFolder = selection ? findFolder(addressesNode) : null;
            return queueFolder;
        }
        
        // bytes messages are passed as a bytes array while
        // other message types get a ready-to-use representation
        // thia is more compact and avoids duplication (e.g. hex+decimal).
        ctrl.updateBytesBodies = function(message) {
           if (!message.bytesBody) {
              // no bytes-body is even present
           }
           if (message.hexBody || message.decimalBody) {
              // the work has already been done
           }

           var hexBytesArr = [];
           for (var b of message.bytesBody) {
              var unsignedByte = b & 0xff;
              if (unsignedByte < 16) {
                 // hex and must be 2 digit so they space out evenly
                 hexBytesArr.push('0' + unsignedByte.toString(16));
              } else {
                 hexBytesArr.push(unsignedByte.toString(16));
              }
           }
           message.hexBody = hexBytesArr.join(" ");

           var decimalBytesArr = [];
           for (var b of message.bytesBody) {
              var unsignedByte = b & 0xff;
              // just show as is without spacing out, as that is usually more used for hex than decimal
              decimalBytesArr.push(unsignedByte.toString(10));
           }
           message.decimalBody = decimalBytesArr.join(" ");
        }

        // apply the chosen representation
        ctrl.updatedDisplayAs = function () {
            var message = ctrl.currentMessage;

            ctrl.updateBytesBodies(message);

            if(ctrl.displayAs === "text") {
                message.theDescr = message.textDescr;
                message.theBody = message.textBody;
            } else if(ctrl.displayAs === "hex") {
                message.theDescr = message.hexDescr;
                message.theBody = message.hexBody;
            } else if(ctrl.displayAs === "decimal") {
                message.theDescr = message.decimalDescr;
                message.theBody = message.decimalBody;
            } else if(ctrl.displayAs === "amqp") {
                message.theDescr = message.amqpDescr;
                message.theBody = message.amqpBody;
            } else if(ctrl.displayAs === "large") {
                message.theDescr = message.largeDescr;
                message.theBody = message.largeBody;
            } else if(ctrl.displayAs === "compressed") {
                message.theDescr = message.compressedDescr;
                message.theBody = message.compressedBody;
            } else {
                message.theDescr = "(unknown representation: " + ctrl.displayAs + ")";
                message.theBody = "(unknown)";
            }

            // retrieve the setting for the presentation
            var representations = sessionStorage.getItem('SelectedRepresentations');
            if (!representations) representations = "";

            // remove the current choice (if any) and insert it again at the front
            // so that it becomes the highest priority
            representations = representations.split(",");
            var p = representations.indexOf(ctrl.displayAs);
            if(p >= 0) representations.splice(p, 1);
            representations.unshift(ctrl.displayAs);

            // save the setting for the presentation
            // prevent various incomplete forms of it before doing that
            // TODO: remove invalid words
            // TODO: remove duplicate words
            representations = representations.join(",");
            representations = representations.replace(/,,,*/, ","); // no empty strings
            representations = representations.replace(/^,/, ""); // no empty strings at start
            representations = representations.replace(/,$/, ""); // no empty strings at end
            sessionStorage.setItem('SelectedRepresentations', representations);
        }

        /*
        * Set up the initial message view:
        * - button enable/disable and/or hide/show is done by the controller
        * - extra: make label for disabled radio-button the disabled-look (gray)
        * - extra: set the pointer to also indicate whether radio button is active
        */
        function createBodyText(ctrl, message) {
           document.getElementById("textDisplayAsLabel").style.color = message.textDescr ? "inherit" : "#9c9c9c";
           document.getElementById("textDisplayAsLabel").style.cursor = message.textDescr ? "pointer" : "not-allowed";
           document.getElementById("textDisplayAsRadio").style.cursor = message.textDescr ? "pointer" : "not-allowed";

           document.getElementById("hexDisplayAsLabel").style.color = message.hexDescr ? "inherit" : "#9c9c9c";
           document.getElementById("hexDisplayAsLabel").style.cursor = message.hexDescr ? "pointer" : "not-allowed";
           document.getElementById("hexDisplayAsRadio").style.cursor = message.hexDescr ? "pointer" : "not-allowed";

           document.getElementById("decimalDisplayAsLabel").style.color = message.decimalDescr ? "inherit" : "#9c9c9c";
           document.getElementById("decimalDisplayAsLabel").style.cursor = message.decimalDescr ? "pointer" : "not-allowed";
           document.getElementById("decimalDisplayAsRadio").style.cursor = message.decimalDescr ? "pointer" : "not-allowed";

           document.getElementById("amqpDisplayAsLabel").style.cursor = message.amqpDescr ? "pointer" : "not-allowed";
           document.getElementById("amqpDisplayAsRadio").style.cursor = message.amqpDescr ? "pointer" : "not-allowed";

           document.getElementById("largeDisplayAsLabel").style.cursor = message.largeDescr ? "pointer" : "not-allowed";
           document.getElementById("largeDisplayAsRadio").style.cursor = message.largeDescr ? "pointer" : "not-allowed";

           document.getElementById("compressedDisplayAsLabel").style.cursor = message.compressedDescr ? "pointer" : "not-allowed";
           document.getElementById("compressedDisplayAsRadio").style.cursor = message.compressedDescr ? "pointer" : "not-allowed";

           var representations = sessionStorage.getItem('SelectedRepresentations');
           if (!representations) representations = "";

           representations = representations.split(",");

           ctrl.updateBytesBodies(message);

           // try the representations in the preference order
           for (var i = 0; i < representations.length; i++) {
              if(representations[i] === "text" && message.textDescr) {
                 ctrl.displayAs = "text";
                 message.theDescr = message.textDescr;
                 message.theBody = message.textBody;
                 return;
              }
              if(representations[i] === "hex" && message.hexDescr) {
                 ctrl.displayAs = "hex";
                 message.theDescr = message.hexDescr;
                 message.theBody = message.hexBody;
                 return;
              }
              if(representations[i] === "decimal" && message.decimalDescr) {
                 ctrl.displayAs = "decimal";
                 message.theDescr = message.decimalDescr;
                 message.theBody = message.decimalBody;
                 return;
              }
              if(representations[i] === "amqp" && message.amqpDescr) {
                 ctrl.displayAs = "amqp";
                 message.theDescr = message.amqpDescr;
                 message.theBody = message.amqpBody;
                 return;
              }
              if(representations[i] === "large" && message.largeDescr) {
                 ctrl.displayAs = "large";
                 message.theDescr = message.largeDescr;
                 message.theBody = message.largeBody;
                 return;
              }
              if(representations[i] === "compressed" && message.compressedDescr) {
                 ctrl.displayAs = "compressed";
                 message.theDescr = message.compressedDescr;
                 message.theBody = message.compressedBody;
                 return;
              }
           }
           // no valid user preference, use the first available representation
           if (message.textDescr !== null) {
              ctrl.displayAs = "text";
              message.theDescr = message.textDescr;
              message.theBody = message.textBody;
           } else if (message.hexDescr !== null) {
              ctrl.displayAs = "hex";
              message.theDescr = message.hexDescr;
              message.theBody = message.hexBody;
           } else if (message.decimalDescr !== null) {
              ctrl.displayAs = "decimal";
              message.theDescr = message.decimalDescr;
              message.theBody = message.decimalBody;
           } else if (message.amqpDescr !== null) {
              ctrl.displayAs = "amqp";
              message.theDescr = message.amqpDescr;
              message.theBody = message.amqpBody;
           } else if (message.largeDescr !== null) {
              ctrl.displayAs = "large";
              message.theDescr = message.largeDescr;
              message.theBody = message.largeBody;
           } else if (message.compressedDescr !== null) {
              ctrl.displayAs = "compressed";
              message.theDescr = message.compressedDescr;
              message.theBody = message.compressedBody;
           } else {
              message.theDescr = "unsupported";
              message.theBody = "Unsupported message body type which cannot be displayed";
           }
        }

        function createHeaders(message) {
        var headers = [];
            angular.forEach(message, function (value, key) {
                if (!_.some(ignoreColumns, function (k) { return k === key; }) && !_.some(flattenColumns, function (k) { return k === key; })) {
                    if(key === "expiration") {
                        value += " (" + formatExpires(value, true) + ")";
                    } else if(key === "persistentSize") {
                        value += " (" + formatPersistentSize(value) + ")";
                    } else if(key === "timestamp") {
                        value += " (" + formatTimestamp(value) + ")";
                    } else if(key === "type") {
                        value += " (" + formatType(value) + ")";
                    }
                    headers.push({key: key, value: value});
                }
            });
            return headers;
        }


        var amqpEncodingLabels = [
            "amqp-unknown", "amqp-null", "amqp-data", "amqp-sequence", "amqp-value-null",
            "amqp-value-string", "amqp-value-binary", "amqp-value-map", "amqp-value-list"];
        function formatAmqpEncoding(enc) {
            if (isNaN(enc) || typeof enc !== "number") {
                return enc;
            }
            return enc > -1 && enc < amqpEncodingLabels.length ? amqpEncodingLabels[enc] : enc;
        }

        var routingTypes = ["multicast", "anycast"];
        function formatRoutingType(rt) {
            if (isNaN(rt) || typeof rt !== "number") {
                return enc;
            }
            return rt > -1 && rt < routingTypes.length ? routingTypes[rt] : rt;
        }

        function createProperties(message) {
            var properties = [];
            angular.forEach(message, function (value, key) {
                if (!_.some(ignoreColumns, function (k) { return k === key; }) && _.some(flattenColumns, function (k) { return k === key; })) {
                    Artemis.log.debug("key=" + key + " value=" + value);
                    angular.forEach(value, function (v2, k2) {
                    Artemis.log.debug("key=" + k2 + " value=" + v2);
                        if(k2 === "_AMQ_Binding_Type") {
                            v2 += " (" + formatBindingType(v2) + ")";
                        } else if(k2 === "JMS_AMQP_ORIGINAL_ENCODING") {
                            v2 += " (" + formatAmqpEncoding(v2) + ")";
                        } else if(k2 === "_AMQ_ACTUAL_EXPIRY") {
                            v2 += " (" + formatTimestamp(v2) + ")";
                        } else if(k2 === "_AMQ_NotifTimestamp") {
                            v2 += " (" + formatTimestamp(v2) + ")";
                        } else if(k2 === "_AMQ_ROUTING_TYPE") {
                            v2 += " (" + formatRoutingType(v2) + ")";
                        } else if(k2 === "_AMQ_ORIG_ROUTING_TYPE") {
                            v2 += " (" + formatRoutingType(v2) + ")";
                        } else if(k2 === "extraProperties._AMQ_ACTUAL_EXPIRY") {
                            v2 += " (" + formatTimestamp(v2) + ")";
                        } else if(k2 === "messageAnnotations.x-opt-jms-dest") {
                            v2 += " (" + formatDestType(v2) + ")";
                        } else if(k2 === "messageAnnotations.x-opt-jms-reply-to") {
                            v2 += " (" + formatDestType(v2) + ")";
                        } else if(k2 === "messageAnnotations.x-opt-jms-msg-type") {
                            v2 += " (" + formatJmsType(v2) + ")";
                        } else if(k2 === "messageAnnotations.x-opt-ACTUAL-EXPIRY") {
                            v2 += " (" + formatTimestamp(v2) + ")";
                        } else if(k2 === "properties.absoluteExpiryTime") {
                            v2 += " (" + formatTimestamp(v2) + ")";
                        } else if(k2 === "properties.creationTime") {
                            v2 += " (" + formatTimestamp(v2) + ")";
                        } else if(k2 === "__HDR_BROKER_IN_TIME") {
                            v2 += " (" + formatTimestamp(v2) + ")";
                        }
                        properties.push({key: k2, value: v2});
                    });
                }
            });
            return properties;
        }

        ctrl.loadTable = function() {
            Artemis.log.debug("loading table")
            ctrl.isLoading = true;
            var objName;
            if (workspace.selection) {
                objName = workspace.selection.objectName;
            } else {
                // in case of refresh
                var key = location.search()['nid'];
                var node = workspace.keyToNodeMap[key];
                objName = node.objectName;
            }
            if (objName) {
                ctrl.dlq = false;
                var addressName = jolokia.getAttribute(objName, "Address");
                var artemisDLQ = localStorage['artemisDLQ'] || "^DLQ$";
                var artemisExpiryQueue = localStorage['artemisExpiryQueue'] || "^ExpiryQueue$";
                Artemis.log.debug("loading table" + artemisExpiryQueue);
                if (addressName.match(artemisDLQ) != null || addressName.match(artemisExpiryQueue) != null) {
                    onDlq(true);
                } else {
                    onDlq(false);
                }
                //make sure to count only filtered messages
                if (ctrl.filter) {
                    jolokia.request({ type: 'exec', mbean: objName, operation: 'countMessages(java.lang.String)', arguments: [ctrl.filter] }, Core.onSuccess(function(response) { ctrl.pagination.page(response.value); }));
                } else {
                    jolokia.request({ type: 'exec', mbean: objName, operation: 'countMessages()'}, Core.onSuccess(function(response) { ctrl.pagination.page(response.value); }));
                }

                jolokia.request({ type: 'exec', mbean: objName, operation: 'browse(int, int, java.lang.String)', arguments: [ctrl.pagination.pageNumber, ctrl.pagination.pageSize, ctrl.filter] }, Core.onSuccess(populateTable));
            }
        }

        function onDlq(response) {
            Artemis.log.debug("onDLQ=" + response);
            ctrl.dlq = response;
            Core.$apply($scope);
        }

        function operationSuccess() {
            ctrl.messageDialog = false;
            Core.notification("success", ctrl.message);
            ctrl.pagination.load();
        }

        function intermediateResult() {
        }


        function moveSuccess() {
            operationSuccess();
            workspace.loadTree();
        }

        function filterMessages(filter) {
            var searchConditions = buildSearchConditions(filter);
            evalFilter(searchConditions);
        }

        function applyFilters(filters) {
            Artemis.log.debug("filters " + filters);
            ctrl.messages = [];
            if (filters && filters.length > 0) {
                ctrl.allMessages.forEach(function (message) {
                    if (matchesFilters(message, filters)) {
                        ctrl.messages.push(message);
                    }
                });
            } else {
                ctrl.messages = ctrl.allMessages;
            }
        };

        var matchesFilter = function (message, filter) {
            var match = true;

            if (filter.id === 'messageID') {
                match = message.messageID.match(filter.value) !== null;
            } else if (filter.id === 'body') {
                match = message.bodyText.match(filter.value) !== null;
            }  else if (filter.id === 'properties') {
                match = message.PropertiesText.match(filter.value) !== null;
            } else if (filter.id === 'priority') {
                match = message.priority == filter.value;
            } else if (filter.id === 'redelivered') {
                var filterTrue = filter.value == 'true';
                match = (message.redelivered == filterTrue);
            }
            return match;
        };

        var matchesFilters = function (message, filters) {
            var matches = true;

            filters.forEach(function(filter) {

                Artemis.log.debug("filter " + filter.id);
                if (!matchesFilter(message, filter)) {
                    matches = false;
                    return false;
                }
            });
            return matches;
        };

      function evalFilter(searchConditions) {
         if (!searchConditions || searchConditions.length === 0) {
            $scope.messages = ctrl.allMessages;
         } else {
            Artemis.log.debug("Filtering conditions:", searchConditions);
            $scope.messages = ctrl.allMessages.filter(function(message) {
               Artemis.log.debug("Message:", message);
               var matched = true;
               $.each(searchConditions, function(index, condition) {
                  if (!condition.column) {
                     matched = matched && evalMessage(message, condition.regex);
                  } else {
                     matched = matched && (message[condition.column] && condition.regex.test(message[condition.column])) || (message.StringProperties && message.StringProperties[condition.column] && condition.regex.test(message.StringProperties[condition.column]));
                  }
               });
               return matched;
            });
         }
      }

      function evalMessage(message, regex) {
         var jmsHeaders = ['JMSDestination', 'JMSDeliveryMode', 'JMSExpiration', 'JMSPriority', 'JMSmessageID', 'JMSTimestamp', 'JMSCorrelationID', 'JMSReplyTo', 'JMSType', 'JMSRedelivered'];
         for (var i = 0; i < jmsHeaders.length; i++) {
            var header = jmsHeaders[i];
            if (message[header] && regex.test(message[header])) {
               return true;
            }
         }
         if (message.StringProperties) {
            for ( var property in message.StringProperties) {
               if (regex.test(message.StringProperties[property])) {
                  return true;
               }
            }
         }
         if (message.bodyText && regex.test(message.bodyText)) {
            return true;
         }
         return false;
      }

      function getRegExp(str, modifiers) {
         try {
            return new RegExp(str, modifiers);
         } catch (err) {
            return new RegExp(str.replace(/(\^|\$|\(|\)|<|>|\[|\]|\{|\}|\\|\||\.|\*|\+|\?)/g, '\\$1'));
         }
      }

      function buildSearchConditions(filterText) {
         var searchConditions = [];
         var qStr;
         if (!(qStr = $.trim(filterText))) {
            return;
         }
         var columnFilters = qStr.split(";");
         for (var i = 0; i < columnFilters.length; i++) {
            var args = columnFilters[i].split(':');
            if (args.length > 1) {
               var columnName = $.trim(args[0]);
               var columnValue = $.trim(args[1]);
               if (columnName && columnValue) {
                  searchConditions.push({
                     column: columnName,
                     columnDisplay: columnName.replace(/\s+/g, '').toLowerCase(),
                     regex: getRegExp(columnValue, 'i')
                  });
               }
            } else {
               var val = $.trim(args[0]);
               if (val) {
                  searchConditions.push({
                     column: '',
                     regex: getRegExp(val, 'i')
                  });
               }
            }
         }
         return searchConditions;
      }
      ctrl.pagination.setOperation(ctrl.loadTable);
      ctrl.pagination.load();
   }
    BrowseQueueController.$inject = ['$scope', 'workspace', 'jolokia', 'localStorage', 'artemisMessage', '$location', '$timeout', '$filter', 'pagination'];

})(Artemis || (Artemis = {}));
