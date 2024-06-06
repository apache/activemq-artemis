package org.apache.activemq.artemis.selector.filter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class CallExpression implements BooleanExpression {
   private final String functionName;
   private final List<Object> parameters;
   private final BooleanExpression expression;

   CallExpression(final String functionName, final List<Object> parameters) {
      this.functionName = functionName;
      this.parameters = parameters;

      try {
         final var function = findFunction(functionName);
         expression = extractExpression(function, parameters);
      } catch (final Exception e) {
         throw new IllegalStateException("Failed to create CallableExpression filter", e);
      }
   }

   @Override
   public Object evaluate(final Filterable message) throws FilterException {
      return expression.evaluate(message);
   }

   @Override
   public boolean matches(final Filterable message) throws FilterException {
      return expression.matches(message);
   }

   @Override
   public String toString() {
      final var parametersStr = parameters.stream()
                                          .map(
                                             p -> p instanceof String ? "'" + p + "'" : p.toString())
                                          .collect(Collectors.joining(", "));
      return "CALL '" + functionName + "'(" + parametersStr + ")";
   }

   private static Method findFunction(final String fullName) {
      final var javaFunctionName = fullName.replace("__", ".");
      final var className = javaFunctionName.substring(0, javaFunctionName.lastIndexOf("."));
      final var functionName = javaFunctionName.substring(className.length() + 1);

      final Class<?> functionClass;
      try {
         functionClass = Class.forName(className);
      } catch (ClassNotFoundException e) {
         throw new IllegalArgumentException(String.format("Could not find class '%s'", className), e);
      }
      final var eligibleFunctions = Arrays.stream(functionClass.getDeclaredMethods())
                                          .filter(m -> Modifier.isPublic(m.getModifiers()))
                                          .filter(m -> Modifier.isStatic(m.getModifiers()))
                                          .filter(m -> m.getName().equals(functionName))
                                          .collect(Collectors.toList());

      if (eligibleFunctions.size() > 1) {
         throw new IllegalArgumentException(String.format(
            "Found %d eligible entries for public static function '%s' in class '%s'",
            eligibleFunctions.size(), functionName, className));
      }

      if (eligibleFunctions.isEmpty()) {
         throw new IllegalArgumentException(
            String.format("No eligible entry found for public static function '%s' in class '%s'",
               functionName, className));
      }

      return eligibleFunctions.get(0);
   }

   private static BooleanExpression extractExpression(final Method function,
      final List<Object> parameters) throws InvocationTargetException, IllegalAccessException {
      final var functionParams = function.getParameterTypes();
      if (functionParams.length != parameters.size()) {
         throw new IllegalArgumentException(
            String.format("Function '%s' expects '%d' parameters, but the filter provides '%d'",
               function.getName(), functionParams.length, parameters.size()));
      }

      final var returnType = function.getReturnType();
      if (!BooleanExpression.class.isAssignableFrom(returnType)) {
         throw new IllegalArgumentException(
            String.format("Function '%s' returns '%s', which cannot be assigned to '%s'",
               function.getName(), returnType, BooleanExpression.class.getName()));
      }

      for (var i = 0; i < parameters.size(); i++) {
         final var functionParamClass = functionParams[i];
         final var filterParam = parameters.get(i);

         if (!functionParamClass.isAssignableFrom(filterParam.getClass())) {
            throw new IllegalArgumentException(
               String.format("Parameter %d of '%s' expects '%s', but the filter provides a '%s'", i,
                  function.getName(), functionParamClass.getName(),
                  filterParam.getClass().getName()));
         }
      }

      final var expression = function.invoke(null, parameters.toArray());
      if (expression == null) {
         throw new IllegalArgumentException(
            String.format("Function '%s' returned a null expression", function.getName()));
      }
      return (BooleanExpression) expression;
   }
}
