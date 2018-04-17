/*
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 Architecture
 */
function ArtemisConsole() {

   this.getServerAttributes = function (jolokia, mBean) {
      var req1 = { type: "read", mbean: mBean};
      return jolokia.request(req1, {method: "post"});
   };

   this.createAddress = function (mbean, jolokia, name, routingType,  method) {
      jolokia.execute(mbean, "createAddress(java.lang.String,java.lang.String)", name, routingType,  method);
   };

   this.deleteAddress = function (mbean, jolokia, name, method) {
      jolokia.execute(mbean, "deleteAddress(java.lang.String)", name,  method);
   };

   this.createQueue = function (mbean, jolokia, address, routingType, name, durable, filter, maxConsumers, purgeWhenNoConsumers, method) {
      jolokia.execute(mbean, "createQueue(java.lang.String,java.lang.String,java.lang.String,java.lang.String,boolean,int,boolean,boolean)", address, routingType, name, filter, durable, maxConsumers, purgeWhenNoConsumers, true, method);
   };

   this.deleteQueue = function (mbean, jolokia, name, method) {
      jolokia.execute(mbean, "destroyQueue(java.lang.String)", name,  method);
   };

   this.purgeQueue = function (mbean, jolokia, method) {
	  jolokia.execute(mbean, "removeAllMessages()", method);
   };

   this.browse = function (mbean, jolokia, method) {
      jolokia.request({ type: 'exec', mbean: mbean, operation: 'browse()' }, method);
   };

   this.deleteMessage = function (mbean, jolokia, id,  method) {
      ARTEMIS.log.info("executing on " + mbean);
      jolokia.execute(mbean, "removeMessage(long)", id, method);
   };

   this.moveMessage = function (mbean, jolokia, id, queueName,  method) {
      jolokia.execute(mbean, "moveMessage(long,java.lang.String)", id, queueName, method);
   };

   this.retryMessage = function (mbean, jolokia, id, method) {
      jolokia.execute(mbean, "retryMessage(java.lang.String)", id,  method);
   };

   this.sendMessage = function (mbean, jolokia, headers, type, body, durable, user, pwd, method) {
      jolokia.execute(mbean, "sendMessage(java.util.Map, int, java.lang.String, boolean, java.lang.String, java.lang.String)", headers, type, body, durable, user, pwd,  method);
   };

   this.getConsumers = function (mbean, jolokia, method) {
      jolokia.request({ type: 'exec', mbean: mbean, operation: 'listAllConsumersAsJSON()' }, method);
   };

   this.getRemoteBrokers = function (mbean, jolokia, method) {
      jolokia.request({ type: 'exec', mbean: mbean, operation: 'listNetworkTopology()' }, method);
   };

   this.ownUnescape = function (name) {
      //simple return unescape(name); does not work for this :(
      return name.replace(/\\\\/g, "\\").replace(/\\\*/g, "*").replace(/\\\?/g, "?");
   };
}

function getServerAttributes() {
   var console = new ArtemisConsole();
   return console.getVersion(new Jolokia("http://localhost:8161/jolokia/"));
}




