# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import httplib
import urlparse

conn = httplib.HTTPConnection("localhost:8080")
conn.request("HEAD", "/queues/orders")
res = conn.getresponse()
consumersLink = res.getheader("msg-pull-consumers")
consumersParsed = urlparse.urlparse(consumersLink)
conn = httplib.HTTPConnection(consumersParsed.netloc)
conn.request("POST", consumersParsed.path)
res = conn.getresponse()
consumeLink = res.getheader("msg-consume-next")
session = res.getheader("Location")
print consumeLink
conn.close()

headers = {"Accept-Wait": "3", "Accept": "application/xml"}

try:
    print "Waiting..."
    while True:
        createParsed = urlparse.urlparse(consumeLink)
        conn = httplib.HTTPConnection(createParsed.netloc)
        conn.request("POST", createParsed.path, None, headers)
        res = conn.getresponse()
        if res.status == 503:
            consumeLink = res.getheader("msg-consume-next")
        elif res.status == 200:
            print "Success!"
            data = res.read()
            print data
            consumeLink = res.getheader("msg-consume-next")
            print "Waiting"
        else:
            raise Exception('failed')
finally:
    if session is not None:
        print "deleting activemq session..."
        createParsed = urlparse.urlparse(session)
        conn = httplib.HTTPConnection(createParsed.netloc)
        conn.request("DELETE", createParsed.path)
        res = conn.getresponse()











