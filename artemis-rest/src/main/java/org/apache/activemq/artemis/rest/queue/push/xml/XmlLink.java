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
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;

@XmlRootElement(name = "link")
@XmlAccessorType(XmlAccessType.PROPERTY)
public class XmlLink implements Serializable {

   private static final long serialVersionUID = -6517264072911034419L;
   protected String method;
   protected String className;
   protected String rel;
   protected String type;
   protected String href;

   @XmlAttribute(name = "class")
   public String getClassName() {
      return className;
   }

   public void setClassName(String className) {
      this.className = className;
   }

   @XmlAttribute
   public String getMethod() {
      return method;
   }

   public void setMethod(String method) {
      this.method = method;
   }

   @XmlAttribute(name = "rel")
   public String getRelationship() {
      return rel;
   }

   public void setRelationship(String relationship) {
      rel = relationship;
   }

   @XmlAttribute
   public String getHref() {
      return href;
   }

   public void setHref(String href) {
      this.href = href;
   }

   @XmlAttribute
   public String getType() {
      return type;
   }

   public void setType(String type) {
      this.type = type;
   }

   @Override
   public String toString() {
      return "XmlLink{" +
         "className='" + className + '\'' +
         ", rel='" + rel + '\'' +
         ", href='" + href + '\'' +
         ", type='" + type + '\'' +
         ", method='" + method + '\'' +
         '}';
   }
}
