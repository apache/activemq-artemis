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

package org.apache.activemq.artemis.utils.bean;

import java.lang.invoke.MethodHandles;
import java.util.Objects;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.json.JsonObject;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetaBeanTest {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   @Test
   public void testToJson() throws Exception {

      MYClass sourceObject = new MYClass();
      sourceObject.setA(RandomUtil.randomString());
      sourceObject.setB(RandomUtil.randomInt());
      sourceObject.setC(RandomUtil.randomInt());
      sourceObject.setD(null);
      sourceObject.setIdCacheSize(333);
      sourceObject.setSimpleString(SimpleString.toSimpleString("mySimpleString"));
      sourceObject.setFloatValue(33.33f);
      sourceObject.setDoubleValue(11.11);
      sourceObject.setBoolValue(true);
      sourceObject.setMyEnum(MyEnum.TWO);


      JsonObject jsonObject = MYClass.metaBean.toJSON(sourceObject, false);
      Assert.assertFalse(jsonObject.containsKey("gated"));

      logger.debug("Result::" + jsonObject.toString());

      MYClass result = new MYClass();
      MYClass.metaBean.fromJSON(result, jsonObject.toString());
      Assert.assertEquals(sourceObject, result);


      Assert.assertEquals(null, result.getD());
      Assert.assertNotNull(result.getIdCacheSize());
      Assert.assertEquals(333, result.getIdCacheSize().intValue());
      Assert.assertEquals(33.33f, result.getFloatValue().floatValue(), 0);
      Assert.assertEquals(11.11, result.getDoubleValue().doubleValue(), 0);
      Assert.assertEquals(MyEnum.TWO, result.getMyEnum());
      Assert.assertTrue(result.getBoolValue());

      sourceObject.setGated(SimpleString.toSimpleString("gated-now-has-value"));
      jsonObject = MYClass.metaBean.toJSON(sourceObject, false);
      Assert.assertTrue(jsonObject.containsKey("gated"));
      Assert.assertEquals("gated-now-has-value", jsonObject.getString("gated"));
   }

   @Test
   public void testRandom() throws Exception {
      MYClass randomObject = new MYClass();
      MYClass.metaBean.setRandom(randomObject);
      String json = MYClass.metaBean.toJSON(randomObject, false).toString();
      MYClass target = new MYClass();
      MYClass.metaBean.fromJSON(target, json);
      Assert.assertEquals(randomObject, target);
      MYClass copy = new MYClass();
      MYClass.metaBean.copy(randomObject, copy);
      Assert.assertEquals(randomObject, copy);
   }

   public enum MyEnum {
      ONE, TWO, TRHEE
   }

   public static class MYClass {

      public static MetaBean<MYClass> metaBean = new MetaBean<>();

      {
         metaBean.add(String.class, "a", (theInstance, parameter) -> theInstance.a = parameter, theInstance -> theInstance.a);
      }
      String a;

      {
         metaBean.add(Integer.class, "b", (theInstance, parameter) -> theInstance.b = parameter, theInstance -> theInstance.b);
      }
      int b;

      {
         metaBean.add(Integer.class, "c", (theInstance, parameter) -> theInstance.c = parameter, theInstance -> theInstance.c);
      }
      Integer c;

      {
         metaBean.add(String.class, "d", (theInstance, parameter) -> theInstance.d = parameter, theInstance -> theInstance.d);
      }
      String d = "defaultString";

      {
         metaBean.add(Integer.class, "IdCacheSize", (obj, value) -> obj.setIdCacheSize(value), obj -> obj.getIdCacheSize());
      }
      Integer idCacheSize;

      {
         metaBean.add(SimpleString.class, "simpleString", (obj, value) -> obj.setSimpleString(value), obj -> obj.getSimpleString());
      }
      SimpleString simpleString;

      {
         metaBean.add(SimpleString.class, "gated", (obj, value) -> obj.setGated((SimpleString) value), obj -> obj.getGated(), obj -> obj.gated != null);
      }
      SimpleString gated;

      {
         metaBean.add(Long.class, "longValue", (obj, value) -> obj.setLongValue(value), obj -> obj.getLongValue());
      }
      Long longValue;
      {
         metaBean.add(Double.class, "doubleValue", (obj, value) -> obj.setDoubleValue(value), obj -> obj.getDoubleValue());
      }
      Double doubleValue;

      {
         metaBean.add(Float.class, "floatValue", (obj, value) -> obj.setFloatValue(value), obj -> obj.getFloatValue());
      }
      Float floatValue;

      {
         metaBean.add(Boolean.class, "boolValue", (obj, value) -> obj.boolValue = value, obj -> obj.boolValue);
      }
      boolean boolValue;

      {
         metaBean.add(MyEnum.class, "myEnum", (o, v) -> o.myEnum = v, o -> o.myEnum);
      }
      MyEnum myEnum;

      public MyEnum getMyEnum() {
         return myEnum;
      }

      public MYClass setMyEnum(MyEnum myEnum) {
         this.myEnum = myEnum;
         return this;
      }

      public String getA() {
         return a;
      }

      public MYClass setA(String a) {
         this.a = a;
         return this;
      }

      public int getB() {
         return b;
      }

      public MYClass setB(int b) {
         this.b = b;
         return this;
      }

      public Integer getC() {
         return c;
      }

      public MYClass setC(Integer c) {
         this.c = c;
         return this;
      }

      public String getD() {
         return d;
      }

      public MYClass setD(String d) {
         this.d = d;
         return this;
      }

      public Long getLongValue() {
         return longValue;
      }

      public MYClass setLongValue(Long longValue) {
         this.longValue = longValue;
         return this;
      }

      public Double getDoubleValue() {
         return doubleValue;
      }

      public MYClass setDoubleValue(Double doubleValue) {
         this.doubleValue = doubleValue;
         return this;
      }

      public Float getFloatValue() {
         return floatValue;
      }

      public MYClass setFloatValue(Float floatValue) {
         this.floatValue = floatValue;
         return this;
      }

      public Integer getIdCacheSize() {
         return idCacheSize;
      }

      public MYClass setIdCacheSize(Integer idCacheSize) {
         this.idCacheSize = idCacheSize;
         return this;
      }

      public SimpleString getSimpleString() {
         return simpleString;
      }

      public MYClass setSimpleString(SimpleString simpleString) {
         this.simpleString = simpleString;
         return this;
      }

      public SimpleString getGated() {
         return gated;
      }

      public MYClass setGated(SimpleString gated) {
         this.gated = gated;
         return this;
      }

      public boolean getBoolValue() {
         return boolValue;
      }

      public MYClass setBoolValue(boolean boolValue) {
         this.boolValue = boolValue;
         return this;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o)
            return true;
         if (o == null || getClass() != o.getClass())
            return false;

         MYClass myClass = (MYClass) o;

         if (b != myClass.b)
            return false;
         if (boolValue != myClass.boolValue)
            return false;
         if (!Objects.equals(a, myClass.a))
            return false;
         if (!Objects.equals(c, myClass.c))
            return false;
         if (!Objects.equals(d, myClass.d))
            return false;
         if (!Objects.equals(idCacheSize, myClass.idCacheSize))
            return false;
         if (!Objects.equals(simpleString, myClass.simpleString))
            return false;
         if (!Objects.equals(gated, myClass.gated))
            return false;
         if (!Objects.equals(longValue, myClass.longValue))
            return false;
         if (!Objects.equals(doubleValue, myClass.doubleValue))
            return false;
         if (!Objects.equals(floatValue, myClass.floatValue))
            return false;
         return myEnum == myClass.myEnum;
      }

      @Override
      public int hashCode() {
         int result = a != null ? a.hashCode() : 0;
         result = 31 * result + b;
         result = 31 * result + (c != null ? c.hashCode() : 0);
         result = 31 * result + (d != null ? d.hashCode() : 0);
         result = 31 * result + (idCacheSize != null ? idCacheSize.hashCode() : 0);
         result = 31 * result + (simpleString != null ? simpleString.hashCode() : 0);
         result = 31 * result + (gated != null ? gated.hashCode() : 0);
         result = 31 * result + (longValue != null ? longValue.hashCode() : 0);
         result = 31 * result + (doubleValue != null ? doubleValue.hashCode() : 0);
         result = 31 * result + (floatValue != null ? floatValue.hashCode() : 0);
         result = 31 * result + (boolValue ? 1 : 0);
         result = 31 * result + (myEnum != null ? myEnum.hashCode() : 0);
         return result;
      }

      @Override
      public String toString() {
         return "MYClass{" + "a='" + a + '\'' + ", b=" + b + ", c=" + c + ", d='" + d + '\'' + ", idCacheSize=" + idCacheSize + ", simpleString=" + simpleString + ", gated=" + gated + ", longValue=" + longValue + ", doubleValue=" + doubleValue + ", floatValue=" + floatValue + ", boolValue=" + boolValue + ", myEnum=" + myEnum + '}';
      }

   }

}
