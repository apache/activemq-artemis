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

package org.apache.activemq.artemis.core.server.prometheus;

import javax.management.MBeanServer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

import io.prometheus.client.Collector;
import org.apache.activemq.artemis.core.config.PrometheusJmxExporterConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.management.ArtemisMBeanServerBuilder;
import org.jboss.logging.Logger;

import static java.lang.String.format;

public class JmxCollector extends Collector {

   private static final Logger logger = Logger.getLogger(JmxCollector.class.getName());

   private final JmxMBeanPropertyCache jmxMBeanPropertyCache = new JmxMBeanPropertyCache();
   private MBeanServer mBeanServer;
   private PrometheusJmxExporterConfiguration config;

   public JmxCollector(MBeanServer mBeanServer, PrometheusJmxExporterConfiguration prometheusJmxExporterConfiguration) {
      config = prometheusJmxExporterConfiguration;
      this.mBeanServer = mBeanServer;
   }

   @Override
   public List<MetricFamilySamples> collect() {
      if (logger.isTraceEnabled()) {
         logger.trace("Collecting Prometheus metrics using: " + config);
      }
      // set this key to bypass the server guard which implements RBAC for MBeans
      ArtemisMBeanServerBuilder.GUARD_BYPASS.set(ArtemisMBeanServerBuilder.KEY);

      Receiver receiver = new Receiver();
      JmxScraper scraper = new JmxScraper(mBeanServer, receiver, jmxMBeanPropertyCache, config.getWhitelistObjectNames(), config.getBlacklistObjectNames());
      long start = System.nanoTime();
      double error = 0;

      try {
         scraper.doScrape();
      } catch (Exception e) {
         error = 1;
         ActiveMQServerLogger.LOGGER.jmxScrapeFailed(e);
      } finally {
         ArtemisMBeanServerBuilder.GUARD_BYPASS.remove();
      }
      List<MetricFamilySamples> mfsList = new ArrayList<>();
      mfsList.addAll(receiver.metricFamilySamplesMap.values());
      List<MetricFamilySamples.Sample> samples = new ArrayList<>();
      samples.add(new MetricFamilySamples.Sample("jmx_scrape_duration_seconds", new ArrayList<>(), new ArrayList<>(), (System.nanoTime() - start) / 1.0E9));
      mfsList.add(new MetricFamilySamples("jmx_scrape_duration_seconds", Type.GAUGE, "Time this JMX scrape took, in seconds.", samples));

      samples = new ArrayList<>();
      samples.add(new MetricFamilySamples.Sample("jmx_scrape_error", new ArrayList<>(), new ArrayList<>(), error));
      mfsList.add(new MetricFamilySamples("jmx_scrape_error", Type.GAUGE, "Non-zero if this scrape failed.", samples));
      return mfsList;
   }

   static String toSnakeAndLowerCase(String attrName) {
      if (attrName == null || attrName.isEmpty()) {
         return attrName;
      }
      char firstChar = attrName.subSequence(0, 1).charAt(0);
      boolean prevCharIsUpperCaseOrUnderscore = Character.isUpperCase(firstChar) || firstChar == '_';
      StringBuilder resultBuilder = new StringBuilder(attrName.length()).append(Character.toLowerCase(firstChar));
      for (char attrChar : attrName.substring(1).toCharArray()) {
         boolean charIsUpperCase = Character.isUpperCase(attrChar);
         if (!prevCharIsUpperCaseOrUnderscore && charIsUpperCase) {
            resultBuilder.append("_");
         }
         resultBuilder.append(Character.toLowerCase(attrChar));
         prevCharIsUpperCaseOrUnderscore = charIsUpperCase || attrChar == '_';
      }
      return resultBuilder.toString();
   }

   /**
    * Change invalid chars to underscore, and merge underscores.
    *
    * @param name Input string
    * @return
    */
   static String safeName(String name) {
      if (name == null) {
         return null;
      }
      boolean prevCharIsUnderscore = false;
      StringBuilder safeNameBuilder = new StringBuilder(name.length());
      if (!name.isEmpty() && Character.isDigit(name.charAt(0))) {
         // prevent a numeric prefix.
         safeNameBuilder.append("_");
      }
      for (char nameChar : name.toCharArray()) {
         boolean isUnsafeChar = !(Character.isLetterOrDigit(nameChar) || nameChar == ':' || nameChar == '_');
         if ((isUnsafeChar || nameChar == '_')) {
            if (prevCharIsUnderscore) {
               continue;
            } else {
               safeNameBuilder.append("_");
               prevCharIsUnderscore = true;
            }
         } else {
            safeNameBuilder.append(nameChar);
            prevCharIsUnderscore = false;
         }
      }

      return safeNameBuilder.toString();
   }

   class Receiver implements JmxScraper.MBeanReceiver {

      Map<String, MetricFamilySamples> metricFamilySamplesMap = new HashMap<>();

      private static final char SEP = '_';

      // [] and () are special in regexes, so swtich to <>.
      private String angleBrackets(String s) {
         return "<" + s.substring(1, s.length() - 1) + ">";
      }

      void addSample(MetricFamilySamples.Sample sample, Type type, String help) {
         MetricFamilySamples mfs = metricFamilySamplesMap.get(sample.name);
         if (mfs == null) {
            // JmxScraper.MBeanReceiver is only called from one thread,
            // so there's no race here.
            mfs = new MetricFamilySamples(sample.name, type, help, new ArrayList<>());
            metricFamilySamplesMap.put(sample.name, mfs);
         }
         mfs.samples.add(sample);
      }

      private void defaultExport(String domain,
                                 LinkedHashMap<String, String> beanProperties,
                                 LinkedList<String> attrKeys,
                                 String attrName,
                                 String help,
                                 Object value,
                                 Type type) {
         StringBuilder name = new StringBuilder();
         name.append(domain);
         if (beanProperties.size() > 0) {
            name.append(SEP);
            name.append(beanProperties.values().iterator().next());
         }
         for (String k : attrKeys) {
            name.append(SEP);
            name.append(k);
         }
         name.append(SEP);
         name.append(attrName);
         String fullname = safeName(name.toString());

         if (config.isLowercaseOutputName()) {
            fullname = fullname.toLowerCase();
         }

         List<String> labelNames = new ArrayList<>();
         List<String> labelValues = new ArrayList<>();
         if (beanProperties.size() > 1) {
            Iterator<Map.Entry<String, String>> iter = beanProperties.entrySet().iterator();
            // Skip the first one, it's been used in the name.
            iter.next();
            while (iter.hasNext()) {
               Map.Entry<String, String> entry = iter.next();
               String labelName = safeName(entry.getKey());
               if (config.isLowercaseOutputLabelNames()) {
                  labelName = labelName.toLowerCase();
               }
               labelNames.add(labelName);
               labelValues.add(entry.getValue());
            }
         }

         addSample(new MetricFamilySamples.Sample(fullname, labelNames, labelValues, ((Number) value).doubleValue()), type, help);
      }

      @Override
      public void recordBean(String domain,
                             LinkedHashMap<String, String> beanProperties,
                             LinkedList<String> attrKeys,
                             String attrName,
                             String attrType,
                             String attrDescription,
                             Object beanValue) {

         String beanName = domain + angleBrackets(beanProperties.toString()) + angleBrackets(attrKeys.toString());
         // attrDescription tends not to be useful, so give the fully qualified name too.
         String help = attrDescription + " (" + beanName + attrName + ")";
         String attrNameSnakeCase = toSnakeAndLowerCase(attrName);

         for (PrometheusJmxExporterConfiguration.Rule rule : config.getRules()) {
            Matcher matcher = null;
            String matchName = beanName + (rule.isAttrNameSnakeCase() ? attrNameSnakeCase : attrName);
            if (rule.getPattern() != null) {
               matcher = rule.getPattern().matcher(matchName + ": " + beanValue);
               if (!matcher.matches()) {
                  continue;
               }
            }

            Number value;
            if (rule.getValue() != null && !rule.getValue().isEmpty()) {
               String val = matcher.replaceAll(rule.getValue());

               try {
                  beanValue = Double.valueOf(val);
               } catch (NumberFormatException e) {
                  ActiveMQServerLogger.LOGGER.trace("Unable to parse configured value '" + val + "' to number for bean: " + beanName + attrName + ": " + beanValue);
                  return;
               }
            }
            if (beanValue instanceof Number) {
               value = ((Number) beanValue).doubleValue() * rule.getValueFactor();
            } else if (beanValue instanceof Boolean) {
               value = (Boolean) beanValue ? 1 : 0;
            } else {
               ActiveMQServerLogger.LOGGER.trace("Ignoring unsupported bean: " + beanName + attrName + ": " + beanValue);
               return;
            }

            // If there's no name provided, use default export format.
            if (rule.getName() == null) {
               defaultExport(domain, beanProperties, attrKeys, rule.isAttrNameSnakeCase() ? attrNameSnakeCase : attrName, help, value, rule.getType());
               return;
            }

            // Matcher is set below here due to validation in the constructor.
            String name = safeName(matcher.replaceAll(rule.getName()));
            if (name.isEmpty()) {
               return;
            }
            if (config.isLowercaseOutputName()) {
               name = name.toLowerCase();
            }

            // Set the help.
            if (rule.getHelp() != null) {
               help = matcher.replaceAll(rule.getHelp());
            }

            // Set the labels.
            ArrayList<String> labelNames = new ArrayList<>();
            ArrayList<String> labelValues = new ArrayList<>();
            if (rule.getLabelNames() != null) {
               for (int i = 0; i < rule.getLabelNames().size(); i++) {
                  final String unsafeLabelName = rule.getLabelNames().get(i);
                  final String labelValReplacement = rule.getLabelValues().get(i);
                  try {
                     String labelName = safeName(matcher.replaceAll(unsafeLabelName));
                     String labelValue = matcher.replaceAll(labelValReplacement);
                     if (config.isLowercaseOutputLabelNames()) {
                        labelName = labelName.toLowerCase();
                     }
                     if (!labelName.isEmpty() && !labelValue.isEmpty()) {
                        labelNames.add(labelName);
                        labelValues.add(labelValue);
                     }
                  } catch (Exception e) {
                     throw new RuntimeException(format("Matcher '%s' unable to use: '%s' value: '%s'", matcher, unsafeLabelName, labelValReplacement), e);
                  }
               }
            }

            // Add to samples.
            ActiveMQServerLogger.LOGGER.trace("add metric sample: " + name + " " + labelNames + " " + labelValues + " " + value.doubleValue());
            addSample(new MetricFamilySamples.Sample(name, labelNames, labelValues, value.doubleValue()), rule.getType(), help);
            return;
         }
      }
   }
}
