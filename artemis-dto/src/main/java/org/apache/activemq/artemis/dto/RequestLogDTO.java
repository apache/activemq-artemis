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
package org.apache.activemq.artemis.dto;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "request-log")
@XmlAccessorType(XmlAccessType.FIELD)
public class RequestLogDTO {

   /**
    * the output file name of the request log
    */
   @XmlAttribute(required = true)
   public String filename;

   /**
    * append to log
    */
   @XmlAttribute
   public Boolean append;

   /**
    * the extended request log format flag
    */
   @XmlAttribute
   public Boolean extended;

   /**
    * logging of the request cookies
    */
   @XmlAttribute
   public Boolean logCookies;

   /**
    * the output file name of the request log
    */
   @XmlAttribute
   public String logTimeZone;

   /**
    * the log file name date format
    */
   @XmlAttribute
   public String filenameDateFormat;

   /**
    * the number of days before rotated log files are deleted
    */
   @XmlAttribute
   public Integer retainDays;

   /**
    * request paths that will not be logged
    */
   @XmlAttribute
   public String ignorePaths;

   /**
    * the timestamp format string for request log entries
    */
   @XmlAttribute
   public String logDateFormat;

   /**
    * the locale of the request log
    */
   @XmlAttribute
   public String logLocale;

   /**
    * logging of request processing time
    */
   @XmlAttribute
   public Boolean logLatency;

   /**
    * logging of the request hostname
    */
   @XmlAttribute
   public Boolean logServer;

   /**
    * whether the actual IP address of the connection or the IP address from the X-Forwarded-For header will be logged
    */
   @XmlAttribute
   public Boolean preferProxiedForAddress;
}
