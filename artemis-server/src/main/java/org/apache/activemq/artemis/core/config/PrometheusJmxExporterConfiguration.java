/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.core.config;

import javax.management.ObjectName;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import io.prometheus.client.Collector;

public class PrometheusJmxExporterConfiguration implements Serializable {

   private static final long serialVersionUID = 9174674625543070014L;
   private boolean lowercaseOutputName = true;
   private boolean lowercaseOutputLabelNames = true;
   private List<ObjectName> whitelistObjectNames = new ArrayList<>();
   private List<ObjectName> blacklistObjectNames = new ArrayList<>();
   public final List<Rule> DEFAULT_RULES = Arrays.asList(
      new PrometheusJmxExporterConfiguration.Rule()
         .setPattern("^org.apache.activemq.artemis<broker=\"([^\"]*)\"><>([^:]*):\\s(.*)")
         .setName("artemis_$2")
         .setAttrNameSnakeCase(true)
         .setType(Collector.Type.COUNTER),
      new PrometheusJmxExporterConfiguration.Rule()
         .setPattern("^org.apache.activemq.artemis<broker=\"([^\"]*)\",\\s*component=addresses,\\s*address=\"([^\"]*)\"><>([^:]*):\\s(.*)")
         .setName("artemis_$3")
         .setAttrNameSnakeCase(true)
         .setType(Collector.Type.COUNTER)
         .addLabelName("address")
         .addLabelValue("$2"),
      new PrometheusJmxExporterConfiguration.Rule()
         .setPattern("^org.apache.activemq.artemis<broker=\"([^\"]*)\",\\s*component=addresses,\\s*address=\"([^\"]*)\",\\s*subcomponent=(queue|topic)s,\\s*routing-type=\"([^\"]*)\",\\s*(queue|topic)=\"([^\"]*)\"><>([^: ]*):\\s(.*)")
         .setName("artemis_$7")
         .setAttrNameSnakeCase(true)
         .setType(Collector.Type.COUNTER)
         .addLabelName("address")
         .addLabelValue("$2")
         .addLabelName("$5")
         .addLabelValue("$6")
   );
   private List<Rule> rules = DEFAULT_RULES;

   public boolean isLowercaseOutputName() {
      return lowercaseOutputName;
   }

   public PrometheusJmxExporterConfiguration setLowercaseOutputName(boolean lowercaseOutputName) {
      this.lowercaseOutputName = lowercaseOutputName;
      return this;
   }

   public boolean isLowercaseOutputLabelNames() {
      return lowercaseOutputLabelNames;
   }

   public PrometheusJmxExporterConfiguration setLowercaseOutputLabelNames(boolean lowercaseOutputLabelNames) {
      this.lowercaseOutputLabelNames = lowercaseOutputLabelNames;
      return this;
   }

   public List<ObjectName> getWhitelistObjectNames() {
      return whitelistObjectNames;
   }

   public PrometheusJmxExporterConfiguration setWhitelistObjectNames(List<ObjectName> whitelistObjectNames) {
      this.whitelistObjectNames = whitelistObjectNames;
      return this;
   }

   public PrometheusJmxExporterConfiguration addWhitelistObjectName(ObjectName whitelistObjectName) {
      this.whitelistObjectNames.add(whitelistObjectName);
      return this;
   }

   public List<ObjectName> getBlacklistObjectNames() {
      return blacklistObjectNames;
   }

   public PrometheusJmxExporterConfiguration setBlacklistObjectNames(List<ObjectName> blacklistObjectNames) {
      this.blacklistObjectNames = blacklistObjectNames;
      return this;
   }

   public PrometheusJmxExporterConfiguration addBlacklistObjectName(ObjectName blacklistObjectName) {
      this.blacklistObjectNames.add(blacklistObjectName);
      return this;
   }

   public List<Rule> getRules() {
      return rules;
   }

   public PrometheusJmxExporterConfiguration setRules(List<Rule> rules) {
      this.rules = rules;
      return this;
   }

   public PrometheusJmxExporterConfiguration addRule(Rule rule) {
      this.rules.add(rule);
      return this;
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() + "[lowercaseOutputName=" + lowercaseOutputName + ", lowercaseOutputLabelNames=" + lowercaseOutputLabelNames + ", whitelistObjectNames=" + whitelistObjectNames + ", blacklistObjectNames=" + blacklistObjectNames + ", DEFAULT_RULES=" + DEFAULT_RULES + ", rules=" + rules + '}';
   }

   public static class Rule implements Serializable {

      private static final long serialVersionUID = 5591373460394255494L;
      private Pattern pattern;
      private String name;
      private String value;
      private Double valueFactor = 1.0;
      private String help;
      private boolean attrNameSnakeCase;
      private Collector.Type type = Collector.Type.UNTYPED;
      private ArrayList<String> labelNames = new ArrayList<>();
      private ArrayList<String> labelValues = new ArrayList<>();

      public Pattern getPattern() {
         return pattern;
      }

      public Rule setPattern(Pattern pattern) {
         this.pattern = pattern;
         return this;
      }

      public Rule setPattern(String pattern) {
         this.pattern = Pattern.compile("^.*(?:" + pattern + ").*$");
         return this;
      }

      public String getName() {
         return name;
      }

      public Rule setName(String name) {
         this.name = name;
         return this;
      }

      public String getValue() {
         return value;
      }

      public Rule setValue(String value) {
         this.value = value;
         return this;
      }

      public Double getValueFactor() {
         return valueFactor;
      }

      public Rule setValueFactor(Double valueFactor) {
         this.valueFactor = valueFactor;
         return this;
      }

      public String getHelp() {
         return help;
      }

      public Rule setHelp(String help) {
         this.help = help;
         return this;
      }

      public boolean isAttrNameSnakeCase() {
         return attrNameSnakeCase;
      }

      public Rule setAttrNameSnakeCase(boolean attrNameSnakeCase) {
         this.attrNameSnakeCase = attrNameSnakeCase;
         return this;
      }

      public Collector.Type getType() {
         return type;
      }

      public Rule setType(Collector.Type type) {
         this.type = type;
         return this;
      }

      public ArrayList<String> getLabelNames() {
         return labelNames;
      }

      public Rule setLabelNames(ArrayList<String> labelNames) {
         this.labelNames = labelNames;
         return this;
      }

      public Rule addLabelName(String labelName) {
         this.labelNames.add(labelName);
         return this;
      }

      public ArrayList<String> getLabelValues() {
         return labelValues;
      }

      public Rule setLabelValues(ArrayList<String> labelValues) {
         this.labelValues = labelValues;
         return this;
      }

      public Rule addLabelValue(String labelValue) {
         this.labelValues.add(labelValue);
         return this;
      }

      @Override
      public String toString() {
         return "Rule{" + "pattern=" + pattern + ", name='" + name + '\'' + ", value='" + value + '\'' + ", valueFactor=" + valueFactor + ", help='" + help + '\'' + ", attrNameSnakeCase=" + attrNameSnakeCase + ", type=" + type + ", labelNames=" + labelNames + ", labelValues=" + labelValues + '}';
      }
   }
}
