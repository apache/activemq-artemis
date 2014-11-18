/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
import org.apache.activemq.rest.queue.push.xml.Authentication;
import org.apache.activemq.rest.queue.push.xml.BasicAuth;
import org.apache.activemq.rest.queue.push.xml.PushRegistration;
import org.apache.activemq.rest.queue.push.xml.XmlLink;
import org.jboss.resteasy.client.ClientRequest;
import org.jboss.resteasy.client.ClientResponse;
import org.jboss.resteasy.spi.Link;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public class PushReg
{
   public static void main(String[] args) throws Exception
   {
      // get the push consumers factory resource
      ClientRequest request = new ClientRequest("http://localhost:9095/queues/jms.queue.orders");
      ClientResponse res = request.head();
      Link pushConsumers = res.getHeaderAsLink("msg-push-consumers");

      // next create the XML document that represents the registration
      // Really, just create a link with the shipping URL and the type you want posted
      PushRegistration reg = new PushRegistration();
      BasicAuth authType = new BasicAuth();
      authType.setUsername("guest");
      authType.setPassword("guest");
      Authentication auth = new Authentication();
      auth.setType(authType);
      reg.setAuthenticationMechanism(auth);
      XmlLink target = new XmlLink();
      target.setHref("http://localhost:9095/queues/jms.queue.shipping");
      target.setType("application/xml");
      target.setRelationship("destination");
      reg.setTarget(target);

      res = pushConsumers.request().body("application/xml", reg).post();
      System.out.println("Create push registration.  Resource URL: " + res.getLocationLink().getHref());
   }
}
