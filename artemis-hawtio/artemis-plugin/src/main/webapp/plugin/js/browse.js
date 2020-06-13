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
var ARTEMIS = (function(ARTEMIS) {

   ARTEMIS.BrowseQueueController = function($scope, workspace, ARTEMISService, jolokia, localStorage, artemisMessage, $location, $timeout) {

      var defaultAttributes = [{
         field: 'messageID',
         displayName: 'Message ID',
         cellTemplate: '<div class="ngCellText"><a ng-click="openMessageDialog(row)">{{row.entity.messageID}}</a></div>',
         // for ng-grid
         width: '100px'
      }, {
         field: 'type',
         displayName: 'Type',
         cellTemplate: '<div class="ngCellText" title="{{row.entity.type}}">{{formatType(row.entity.type)}}</div>',
         width: '90px'
      }, {
         field: 'durable',
         displayName: 'Durable',
         width: '70px'
      }, {
         field: 'priority',
         displayName: 'Priority',
         width: '70px'
      }, {
         field: 'timestamp',
         displayName: 'Timestamp',
         cellTemplate: '<div class="ngCellText" title="{{row.entity.timestamp}}">{{formatTimestamp(row.entity.timestamp)}}</div>',
         width: '160px'
      }, {
         field: 'expiration',
         displayName: 'Expires',
         cellTemplate: '<div class="ngCellText" title="{{row.entity.expiration}}">{{formatExpires(row.entity.expiration)}}</div>',
         width: '160px'
      }, {
         field: 'redelivered',
         displayName: 'Redelivered',
         width: '100px'
      }, {
         field: 'largeMessage',
         displayName: 'Large',
         width: '50px'
      }, {
         field: 'persistentSize',
         displayName: 'Persistent Size',
         cellTemplate: '<div class="ngCellText" title="{{row.entity.persistentSize.toLocaleString()}} bytes">{{formatPersistentSize(row.entity.persistentSize)}}</div>',
         width: '120px'
      }, {
         field: 'userID',
         displayName: 'User ID',
         width: '15%'
      }, {
         displayName: 'Validated User',
         cellTemplate: '<div class="ngCellText">{{row.entity.StringProperties._AMQ_VALIDATED_USER}}</div>',
         width: '*'
      }];

      var attributes = defaultAttributes;
      if (sessionStorage.getItem('browseColumnDefs')) {
         attributes = JSON.parse(sessionStorage.getItem('browseColumnDefs'));
      }

      $scope.$on('ngGridEventColumns', function(newColumns) {
         ARTEMIS.log.debug('ngGridEventColumns:', newColumns);
         var visibles = newColumns.targetScope.columns.reduce(function(visibles, column) {
            visibles[column.field] = column.visible;
            return visibles;
         }, {});
         ARTEMIS.log.debug('ngGridEventColumns: visibles =', visibles);
         attributes.forEach(function(attribute) {
            attribute.visible = visibles[attribute.field];
         });
         sessionStorage.setItem('browseColumnDefs', JSON.stringify(attributes));
      });

      $scope.pagingOptions = {
         pageSizes: [50, 100, 200],
         pageSize: 100,
         currentPage: 1
      };
      $scope.totalServerItems = 0;
      $scope.searchText = '';
      $scope.allMessages = [];
      $scope.messages = [];
      $scope.headers = {};
      $scope.mode = 'text';
      $scope.deleteDialog = false;
      $scope.moveDialog = false;
      $scope.gridOptions = {
         pagingOptions: $scope.pagingOptions,
         enablePaging: true,
         totalServerItems: 'totalServerItems',
         showFooter: true,
         selectedItems: [],
         data: 'messages',
         displayFooter: false,
         showFilter: false,
         showColumnMenu: true,
         enableColumnResize: true,
         enableColumnReordering: true,
         enableHighlighting: true,
         filterOptions: {
            filterText: '',
            useExternalFilter: true
         },
         selectWithCheckboxOnly: true,
         showSelectionCheckbox: true,
         maintainColumnRatios: false,
         columnDefs: attributes,
         afterSelectionChange: afterSelectionChange
      };
      $scope.showMessageDetails = false;

      var ignoreColumns = ["PropertiesText", "BodyPreview", "text"];
      var flattenColumns = ["BooleanProperties", "ByteProperties", "ShortProperties", "IntProperties", "LongProperties", "FloatProperties", "DoubleProperties", "StringProperties"];

      $scope.$watch('workspace.selection', function() {
         if (workspace.moveIfViewInvalid()) {
            return;
         }
         // lets defer execution as we may not have the selection just yet
         setTimeout(loadTable, 50);
      });
      $scope.$watch('gridOptions.filterOptions.filterText', function(filterText) {
         filterMessages(filterText);
      });
      $scope.$watch('pagingOptions', function(newVal, oldVal) {
         if (parseInt(newVal.currentPage) && newVal !== oldVal && newVal.currentPage !== oldVal.currentPage) {
            loadTable();
         }
         if (parseInt(newVal.pageSize) && newVal !== oldVal && newVal.pageSize !== oldVal.pageSize) {
            $scope.pagingOptions.currentPage = 1;
            loadTable();
         }
      }, true);
      $scope.openMessageDialog = function(message) {
         ARTEMIS.selectCurrentMessage(message, "messageID", $scope);
         if ($scope.row) {
            $scope.mode = CodeEditor.detectTextFormat($scope.row.Text);
            $scope.showMessageDetails = true;
         }
      };
      $scope.refresh = loadTable;
      ARTEMIS.decorate($scope);

      var MS_PER_SEC  = 1000;
      var MS_PER_MIN  = 60 * MS_PER_SEC;
      var MS_PER_HOUR = 60 * MS_PER_MIN;
      var MS_PER_DAY  = 24 * MS_PER_HOUR;

      function pad2(value) {
         return (value < 10 ? '0' : '') + value;
      }

      $scope.formatExpires = function(timestamp) {
         if (isNaN(timestamp)) {
            return timestamp;
         }
         var expiresIn = timestamp - Date.now();
         if (Math.abs(expiresIn) < MS_PER_DAY) {
            var duration = expiresIn < 0 ? -expiresIn : expiresIn;
            var hours = pad2(Math.floor((duration / MS_PER_HOUR) % 24));
            var mins  = pad2(Math.floor((duration / MS_PER_MIN) % 60));
            var secs  = pad2(Math.floor((duration / MS_PER_SEC) % 60));
            if (expiresIn < 0) {
               // "HH:mm:ss ago"
               return hours + ":" + mins + ":" + secs + " ago";
            }
            // "in HH:mm:ss ago"
            return "in " + hours + ":" + mins + ":" + secs;
         }
         return $scope.formatTimestamp(timestamp);
      }

      $scope.formatTimestamp = function(timestamp) {
         if (isNaN(timestamp)) {
            return timestamp;
         }
         var d = new Date(timestamp);
         // "yyyy-MM-dd HH:mm:ss"
         //add 1 to month as getmonth returns the position not the actual month
         return d.getFullYear() + "-" + pad2(d.getMonth() + 1) + "-" + pad2(d.getDate()) + " " + pad2(d.getHours()) + ":" + pad2(d.getMinutes()) + ":" + pad2(d.getSeconds());
      }

      var typeLabels = ["default", "1", "object", "text", "bytes", "map", "stream", "embedded"];
      $scope.formatType = function(type) {
         if (isNaN(type)) {
            return type;
         }
         return type > -1 && type < 8 ? typeLabels[type] : type
      }

      $scope.formatPersistentSize = function(bytes) {
         if(isNaN(bytes) || bytes < 0) return "n/a";
         if(bytes < 10240) return bytes.toLocaleString() + " Bytes";
         if(bytes < 1048576) return (bytes / 1024).toFixed(2) + " KB";
         if(bytes < 1073741824) return (bytes / 1048576).toFixed(2) + " MB";
         return (bytes / 1073741824).toFixed(2) + " GB";
      }

      $scope.moveMessages = function() {
         var selection = workspace.selection;
         var mbean = selection.objectName;
         if (mbean && selection) {
            var selectedItems = $scope.gridOptions.selectedItems;
            $scope.message = "Moved " + Core.maybePlural(selectedItems.length, "message" + " to " + $scope.queueName);
            angular.forEach(selectedItems, function(item, idx) {
               var id = item.messageID;
               if (id) {
                  var callback = (idx + 1 < selectedItems.length) ? intermediateResult : moveSuccess;
                  ARTEMISService.artemisConsole.moveMessage(mbean, jolokia, id, $scope.queueName, onSuccess(callback));
               }
            });
         }
      };
      $scope.resendMessage = function() {
         var selection = workspace.selection;
         var mbean = selection.objectName;
         if (mbean && selection) {
            var selectedItems = $scope.gridOptions.selectedItems;
            // always assume a single message
            artemisMessage.message = selectedItems[0];
            $location.path('artemis/sendMessage');
         }
      };
      $scope.deleteMessages = function() {
         var selection = workspace.selection;
         var mbean = selection.objectName;
         if (mbean && selection) {
            var selectedItems = $scope.gridOptions.selectedItems;
            $scope.message = "Deleted " + Core.maybePlural(selectedItems.length, "message");
            angular.forEach(selectedItems, function(item, idx) {
               var id = item.messageID;
               if (id) {
                  var callback = (idx + 1 < selectedItems.length) ? intermediateResult : operationSuccess;
                  ARTEMISService.artemisConsole.deleteMessage(mbean, jolokia, id, onSuccess(callback));
               }
            });
         }
      };
      $scope.retryMessages = function() {
         var selection = workspace.selection;
         var mbean = selection.objectName;
         if (mbean && selection) {
            var selectedItems = $scope.gridOptions.selectedItems;
            $scope.message = "Retry " + Core.maybePlural(selectedItems.length, "message");
            angular.forEach(selectedItems, function(item, idx) {
               var id = item.messageID;
               if (id) {
                  var callback = (idx + 1 < selectedItems.length) ? intermediateResult : operationSuccess;
                  ARTEMISService.artemisConsole.retryMessage(mbean, jolokia, id, onSuccess(callback));
               }
            });
         }
      };
      $scope.queueNames = function(completionText) {
         var queuesFolder = ARTEMIS.getSelectionQueuesFolder(workspace);
         if (queuesFolder) {
            var selectedQueue = workspace.selection.key;
            var otherQueues = queuesFolder.children.exclude(function(child) {
               return child.key == selectedQueue;
            });
            return (otherQueues) ? otherQueues.map(function(n) {
               return n.title;
            }) : [];
         } else {
            return [];
         }
      };

      function populateTable(response) {
         var data = response.value;
         ARTEMIS.log.info("loading data:" + data);

         if (!angular.isArray(data)) {
            $scope.allMessages = [];
            angular.forEach(data, function(value, idx) {
               $scope.allMessages.push(value);
            });
         } else {
            $scope.allMessages = data;
         }
         angular.forEach($scope.allMessages, function(message) {
            message.headerHtml = createHeaderHtml(message);
            message.bodyText = createBodyText(message);
         });
         filterMessages($scope.gridOptions.filterOptions.filterText);
         Core.$apply($scope);
      }

      /*
       * For some reason using ng-repeat in the modal dialog doesn't work so lets
       * just create the HTML in code :)
       */
      function createBodyText(message) {
         ARTEMIS.log.info("loading message:" + message);
         if (message.text) {
            var body = message.text;
            var lenTxt = "" + body.length;
            message.textMode = "text (" + lenTxt + " chars)";
            return body;
         } else if (message.BodyPreview) {
            var code = Core.parseIntValue(localStorage["ARTEMISBrowseBytesMessages"] || "1", "browse bytes messages");
            var body;
            message.textMode = "bytes (turned off)";
            if (code != 99) {
               var bytesArr = [];
               var textArr = [];
               message.BodyPreview.forEach(function(b) {
                  if (code === 1 || code === 2) {
                     // text
                     textArr.push(String.fromCharCode(b));
                  }
                  if (code === 1 || code === 4) {
                     // hex and must be 2 digit so they space out evenly
                     var s = b.toString(16);
                     if (s.length === 1) {
                        s = "0" + s;
                     }
                     bytesArr.push(s);
                  } else {
                     // just show as is without spacing out, as that is usually more used for hex than decimal
                     var s = b.toString(10);
                     bytesArr.push(s);
                  }
               });
               var bytesData = bytesArr.join(" ");
               var textData = textArr.join("");
               if (code === 1 || code === 2) {
                  // bytes and text
                  var len = message.BodyPreview.length;
                  var lenTxt = "" + textArr.length;
                  body = "bytes:\n" + bytesData + "\n\ntext:\n" + textData;
                  message.textMode = "bytes (" + len + " bytes) and text (" + lenTxt + " chars)";
               } else {
                  // bytes only
                  var len = message.BodyPreview.length;
                  body = bytesData;
                  message.textMode = "bytes (" + len + " bytes)";
               }
            }
            return body;
         } else {
            message.textMode = "unsupported";
            return "Unsupported message body type which cannot be displayed by hawtio";
         }
      }

      /*
       * For some reason using ng-repeat in the modal dialog doesn't work so lets
       * just create the HTML in code :)
       */
      function createHeaderHtml(message) {
         var headers = createHeaders(message);
         var properties = createProperties(message);
         var headerKeys = Object.extended(headers).keys();

         function sort(a, b) {
            if (a > b)
               return 1;
            if (a < b)
               return -1;
            return 0;
         }

         var propertiesKeys = Object.extended(properties).keys().sort(sort);
         var jmsHeaders = headerKeys.filter(function(key) {
            return key.startsWith("JMS");
         }).sort(sort);
         var remaining = headerKeys.subtract(jmsHeaders, propertiesKeys).sort(sort);
         var buffer = [];

         function appendHeader(key) {
            var value = headers[key];
            if (value === null) {
               value = '';
            }
            if (key == "expiration" || key == "timestamp") {
               value = $scope.formatTimestamp(value) + " (" + value + ")";
            } else if (key == "type") {
               value = $scope.formatType(value) + " (" + value + ")";
            }
            buffer.push('<tr><td class="propertyName"><span class="green">Header</span> - ' + key + '</td><td class="property-value">' + value + '</td></tr>');
         }

         function appendProperty(key) {
            var value = properties[key];
            if (value === null) {
               value = '';
            }
            buffer.push('<tr><td class="propertyName">' + key + '</td><td class="property-value">' + value + '</td></tr>');
         }

         jmsHeaders.forEach(appendHeader);
         remaining.forEach(appendHeader);
         propertiesKeys.forEach(appendProperty);
         return buffer.join("\n");
      }

      function createHeaders(row) {
         var answer = {};
         angular.forEach(row, function(value, key) {
            if (!ignoreColumns.any(key) && !flattenColumns.any(key)) {
               answer[Core.escapeHtml(key)] = Core.escapeHtml(value);
            }
         });
         return answer;
      }

      function createProperties(row) {
         ARTEMIS.log.debug("properties: ", row);
         var answer = {};
         angular.forEach(row, function(value, key) {
            if (!ignoreColumns.any(key) && flattenColumns.any(key)) {
               angular.forEach(value, function(v2, k2) {
                  answer['<span class="green">' + key.replace('Properties', ' Property') + '</span> - ' + Core.escapeHtml(k2)] = Core.escapeHtml(v2);
               });
            }
         });
         return answer;
      }

      function loadTable() {
         ARTEMIS.log.info("loading table")
         var objName;
         $scope.gridOptions.selectedItems.length = 0;
         if (workspace.selection) {
            objName = workspace.selection.objectName;
         } else {
            // in case of refresh
            var key = location.search()['nid'];
            var node = workspace.keyToNodeMap[key];
            objName = node.objectName;
         }
         if (objName) {
            $scope.dlq = false;
            var queueName = jolokia.getAttribute(objName, "Name");

            var artemisDLQ = localStorage['artemisDLQ'] || "DLQ";
            var artemisExpiryQueue = localStorage['artemisExpiryQueue'] || "ExpiryQueue";
            ARTEMIS.log.info("loading table" + artemisExpiryQueue);
            if (queueName == artemisDLQ || queueName == artemisExpiryQueue) {
               onDlq(true);
            } else {
               onDlq(false);
            }
            jolokia.request({ type: 'exec', mbean: objName, operation: 'countMessages()'}, onSuccess(function(response) { $scope.totalServerItems = response.value; }));
            jolokia.request({ type: 'exec', mbean: objName, operation: 'browse(int, int)', arguments: [$scope.pagingOptions.currentPage, $scope.pagingOptions.pageSize] }, onSuccess(populateTable));
         }
      }

      function onDlq(response) {
         ARTEMIS.log.info("onDLQ=" + response);
         $scope.dlq = response;
         Core.$apply($scope);
      }

      function intermediateResult() {
      }

      function operationSuccess() {
         $scope.messageDialog = false;
         deselectAll();
         Core.notification("success", $scope.message);
         loadTable();
         setTimeout(loadTable, 50);
      }

      function moveSuccess() {
         operationSuccess();
         workspace.loadTree();
      }

      function filterMessages(filter) {
         var searchConditions = buildSearchConditions(filter);
         evalFilter(searchConditions);
      }

      function evalFilter(searchConditions) {
         if (!searchConditions || searchConditions.length === 0) {
            $scope.messages = $scope.allMessages;
         } else {
            ARTEMIS.log.debug("Filtering conditions:", searchConditions);
            $scope.messages = $scope.allMessages.filter(function(message) {
               ARTEMIS.log.debug("Message:", message);
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

      function afterSelectionChange(rowItem, checkAll) {
         if (checkAll === void 0) {
            // then row was clicked, not select-all checkbox
            $scope.gridOptions['$gridScope'].allSelected = rowItem.config.selectedItems.length == $scope.messages.length;
         } else {
            $scope.gridOptions['$gridScope'].allSelected = checkAll;
         }
      }

      function deselectAll() {
         $scope.gridOptions['$gridScope'].allSelected = false;
      }
   }

   return ARTEMIS;
}(ARTEMIS || {}));
