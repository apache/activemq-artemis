/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.extensions.parameterized;

import static org.apache.activemq.artemis.tests.extensions.parameterized.Parameters.INDEX_TEMPLATE;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;
import org.junit.platform.commons.support.AnnotationSupport;
import org.junit.platform.commons.support.HierarchyTraversalMode;

import java.lang.reflect.Executable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * This extension is used to implement parameterized tests classes for JUnit 5 via the TestTemplate mechanism, replace
 * parameterized test runner from JUnit4 while avoiding the per-method parameterization currently available from Junit
 * 5.
 * <p>
 * When using this extension, all tests must be annotated by {@link TestTemplate}.
 */
public class ParameterizedTestExtension implements TestTemplateInvocationContextProvider {

   private static final ExtensionContext.Namespace NAMESPACE = ExtensionContext.Namespace.create("parameterized");
   private static final String PARAMETERS_STORE_KEY = "parameters";
   private static final String PARAMETER_FIELD_STORE_KEY_PREFIX = "parameterField_";

   @Override
   public boolean supportsTestTemplate(ExtensionContext context) {
      return true;
   }

   @Override
   public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(ExtensionContext context) {
      // Search method annotated with @Parameters
      final List<Method> parameterProviders = AnnotationSupport.findAnnotatedMethods(context.getRequiredTestClass(), Parameters.class, HierarchyTraversalMode.TOP_DOWN);
      if (parameterProviders.isEmpty()) {
         throw new IllegalStateException("Cannot find any parameter provider");
      }

      if (parameterProviders.size() > 1) {
         throw new IllegalStateException("Multiple parameter providers are found");
      }

      Method parameterProvider = parameterProviders.get(0);
      // Get potential test name
      String testNameTemplate = parameterProvider.getAnnotation(Parameters.class).name();

      // Get parameter values
      final Object parameterValues;
      try {
         parameterProvider.setAccessible(true);
         parameterValues = parameterProvider.invoke(null);
         context.getStore(NAMESPACE).put(PARAMETERS_STORE_KEY, parameterValues);
      } catch (Exception e) {
         throw new IllegalStateException("Failed to invoke parameter provider", e);
      }

      Objects.requireNonNull(parameterValues, "No paramaters returned by @Parameters annotated method: " + parameterProvider.getName());

      // Parameter values could be Object[][]
      if (parameterValues instanceof Object[][] typedParameterValues) {
         return createContextForParameters(Arrays.stream(typedParameterValues), testNameTemplate, context);
      }

      // or a Collection
      if (parameterValues instanceof Collection<?> typedParameterValues) {
         final Stream<Object[]> parameterValueStream = typedParameterValues.stream()
               .map((Function<Object, Object[]>) parameterValue -> {
                  if (parameterValue instanceof Object[] objects) {
                     return objects;
                  } else {
                     return new Object[]{parameterValue};
                  }
               });
         return createContextForParameters(parameterValueStream, testNameTemplate, context);
      }

      throw new IllegalStateException(String.format(
            "Return type of @Parameters annotated method \"%s\" should be either Object[][] or Collection",
            parameterProvider));
   }

   private static class FieldInjectingInvocationContext implements TestTemplateInvocationContext {

      private final String testNameTemplate;
      private final Object[] parameterValues;

      FieldInjectingInvocationContext(String testNameTemplate, Object[] parameterValues) {
         this.testNameTemplate = testNameTemplate;
         this.parameterValues = parameterValues;
      }

      @Override
      public String getDisplayName(int invocationIndex) {
         return getDisplayNameForTemplate(invocationIndex, testNameTemplate, parameterValues);
      }

      @Override
      public List<Extension> getAdditionalExtensions() {
         return Collections.singletonList(new FieldInjectingHook(parameterValues));
      }

      private static class FieldInjectingHook implements BeforeEachCallback {

         private final Object[] parameterValues;

         FieldInjectingHook(Object[] parameterValues) {
            this.parameterValues = parameterValues;
         }

         @Override
         public void beforeEach(ExtensionContext context) throws Exception {
            for (int i = 0; i < parameterValues.length; i++) {
               getParameterField(i, context).setAccessible(true);
               getParameterField(i, context).set(context.getRequiredTestInstance(), parameterValues[i]);
            }
         }
      }
   }

   private static class ConstructorParameterResolverInvocationContext implements TestTemplateInvocationContext {

      private final String testNameTemplate;
      private final Object[] parameterValues;

      ConstructorParameterResolverInvocationContext(String testNameTemplate, Object[] parameterValues) {
         this.testNameTemplate = testNameTemplate;
         this.parameterValues = parameterValues;
      }

      @Override
      public String getDisplayName(int invocationIndex) {
         return getDisplayNameForTemplate(invocationIndex, testNameTemplate, parameterValues);
      }

      @Override
      public List<Extension> getAdditionalExtensions() {
         return Collections.singletonList(new ConstructorParameterResolver(parameterValues));
      }

      private static class ConstructorParameterResolver implements ParameterResolver {

         private final Object[] parameterValues;

         ConstructorParameterResolver(Object[] parameterValues) {
            this.parameterValues = parameterValues;
         }

         @Override
         public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
               throws ParameterResolutionException {
            Executable declaring = parameterContext.getDeclaringExecutable();
            return declaring instanceof java.lang.reflect.Constructor;
         }

         @Override
         public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
               throws ParameterResolutionException {
            return parameterValues[parameterContext.getIndex()];
         }
      }
   }

   private Stream<TestTemplateInvocationContext> createContextForParameters(Stream<Object[]> parameterValueStream,
         String testNameTemplate, ExtensionContext context) {
      // Search fields annotated by @Parameter
      final List<Field> parameterFields = AnnotationSupport.findAnnotatedFields(context.getRequiredTestClass(),
            Parameter.class);

      // Use constructor parameter style
      if (parameterFields.isEmpty()) {
         return parameterValueStream.map(
               parameterValue -> new ConstructorParameterResolverInvocationContext(testNameTemplate, parameterValue));
      }

      // Use field injection style
      for (Field parameterField : parameterFields) {
         final int index = parameterField.getAnnotation(Parameter.class).index();
         context.getStore(NAMESPACE).put(getParameterFieldStoreKey(index), parameterField);
      }
      return parameterValueStream
            .map(parameterValue -> new FieldInjectingInvocationContext(testNameTemplate, parameterValue));
   }

   private static String getParameterFieldStoreKey(int parameterIndex) {
      return PARAMETER_FIELD_STORE_KEY_PREFIX + parameterIndex;
   }

   private static Field getParameterField(int parameterIndex, ExtensionContext context) {
      return (Field) context.getStore(NAMESPACE).get(getParameterFieldStoreKey(parameterIndex));
   }

   private static String getDisplayNameForTemplate(int invocationIndex, String template, Object[] parameterValues) {
      if (INDEX_TEMPLATE.equals(template)) {
         return "[" + invocationIndex + "]";
      } else {
         if (template.contains(INDEX_TEMPLATE)) {
            template = template.replace(INDEX_TEMPLATE, "[" + invocationIndex + "]");
         }

         return MessageFormat.format(template, parameterValues);
      }
   }
}
