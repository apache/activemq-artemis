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
    function artemisJmxDomain() {
        return localStorage['artemisJmxDomain'] || "org.apache.activemq.artemis";
    }
    Artemis.artemisJmxDomain = artemisJmxDomain;;

    function ownUnescape(name) {
        //simple return unescape(name); does not work for this :(
        return name.replace(/\\\\/g, "\\").replace(/\\\*/g, "*").replace(/\\\?/g, "?");
    };
    Artemis.ownUnescape = ownUnescape;

    function getBrokerMBean(workspace, jolokia) {
        var mbean = null;
        var selection = workspace.selection;
        var folderNames = selection.folderNames;
        mbean = "" + folderNames[0] + ":broker=\"" + folderNames[1] + "\"";
        return mbean;
    }
    Artemis.getBrokerMBean = getBrokerMBean;

})(Artemis || (Artemis = {}));