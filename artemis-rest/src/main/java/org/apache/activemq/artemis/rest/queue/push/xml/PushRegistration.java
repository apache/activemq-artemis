/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.rest.queue.push.xml;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementRef;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@XmlRootElement(name = "push-registration")
@XmlAccessorType(XmlAccessType.PROPERTY)
@XmlType(propOrder = {"enabled", "destination", "durable", "selector", "target", "maxRetries", "retryWaitMillis", "disableOnFailure", "authenticationMechanism", "headers", "sessionCount"})
public class PushRegistration implements Serializable {

   private static final long serialVersionUID = -2749818399978544262L;
   private String id;
   private boolean durable;
   private XmlLink target;
   private Authentication authenticationMechanism;
   private List<XmlHttpHeader> headers = new ArrayList<>();
   private String destination;
   private Object loadedFrom;
   private String selector;
   private long retryWaitMillis = 1000;
   private boolean disableOnFailure;
   private int maxRetries = 10;
   private boolean enabled = true;
   private int sessionCount = 1;

   @XmlElement
   public int getMaxRetries() {
      return maxRetries;
   }

   public void setMaxRetries(int maxRetries) {
      this.maxRetries = maxRetries;
   }

   @XmlElement
   public long getRetryWaitMillis() {
      return retryWaitMillis;
   }

   public void setRetryWaitMillis(long retryWaitMillis) {
      this.retryWaitMillis = retryWaitMillis;
   }

   @XmlElement
   public boolean isDisableOnFailure() {
      return disableOnFailure;
   }

   public void setDisableOnFailure(boolean disableOnFailure) {
      this.disableOnFailure = disableOnFailure;
   }

   @XmlElement
   public boolean isEnabled() {
      return enabled;
   }

   public void setEnabled(boolean enabled) {
      this.enabled = enabled;
   }

   @XmlTransient
   public Object getLoadedFrom() {
      return loadedFrom;
   }

   public void setLoadedFrom(Object loadedFrom) {
      this.loadedFrom = loadedFrom;
   }

   @XmlAttribute
   public String getId() {
      return id;
   }

   public void setId(String id) {
      this.id = id;
   }

   @XmlElement
   public String getDestination() {
      return destination;
   }

   public void setDestination(String destination) {
      this.destination = destination;
   }

   @XmlElement
   public boolean isDurable() {
      return durable;
   }

   public void setDurable(boolean durable) {
      this.durable = durable;
   }

   public String getSelector() {
      return selector;
   }

   public void setSelector(String selector) {
      this.selector = selector;
   }

   @XmlElementRef
   public XmlLink getTarget() {
      return target;
   }

   public void setTarget(XmlLink target) {
      this.target = target;
   }

   @XmlElementRef
   public Authentication getAuthenticationMechanism() {
      return authenticationMechanism;
   }

   public void setAuthenticationMechanism(Authentication authenticationMechanism) {
      this.authenticationMechanism = authenticationMechanism;
   }

   @XmlElementRef
   public List<XmlHttpHeader> getHeaders() {
      return headers;
   }

   public void setHeaders(List<XmlHttpHeader> headers) {
      this.headers = headers;
   }

   @XmlElement
   public int getSessionCount() {
      return sessionCount;
   }

   public void setSessionCount(int sessionCount) {
      this.sessionCount = sessionCount;
   }

   @Override
   public String toString() {
      return "PushRegistration{" +
         "id='" + id + '\'' +
         ", durable=" + durable +
         ", target=" + target +
         ", authenticationMechanism=" + authenticationMechanism +
         ", headers=" + headers +
         ", destination='" + destination + '\'' +
         ", selector='" + selector + '\'' +
         ", retryWaitMillis=" + retryWaitMillis +
         ", disableOnFailure=" + disableOnFailure +
         ", maxRetries=" + maxRetries +
         ", sessionCount=" + sessionCount +
         ", enabled=" + enabled +
         '}';
   }
}
