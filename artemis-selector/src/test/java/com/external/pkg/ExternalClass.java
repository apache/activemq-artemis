package com.external.pkg;
import static org.apache.activemq.artemis.api.core.SimpleString.toSimpleString;

import org.apache.activemq.artemis.selector.filter.BooleanExpression;
import org.apache.activemq.artemis.selector.filter.ConstantExpression;
import org.apache.activemq.artemis.selector.filter.Expression;
import org.apache.activemq.artemis.selector.filter.Filterable;

public class ExternalClass {

   private ExternalClass() {
   }

   public static Expression staticInvalidFunction(final String property,
      final Object expectedValue) {
      final var instance = new ExternalClass();
      return instance.externalFunction(property, expectedValue);
   }

   public static BooleanExpression staticFunction(final String property,
      final Object expectedValue) {
      final var instance = new ExternalClass();
      return instance.externalFunction(property, expectedValue);
   }

   public static BooleanExpression staticFunctionNoParams() {
      return ConstantExpression.TRUE;
   }

   public BooleanExpression externalFunction(final String property, final Object expectedValue) {
      return new BooleanExpression() {

         @Override
         public Boolean evaluate(final Filterable message) {
            return message.getProperty(toSimpleString(property)).equals(expectedValue);
         }

         @Override
         public boolean matches(final Filterable message) {
            return this.evaluate(message);
         }
      };
   }
}
