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
package org.apache.activemq.artemis.selector.filter;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

/**
 * An expression which performs an operation on two expression values
 *
 * @version $Revision: 1.3 $
 */
public abstract class UnaryExpression implements Expression {

   private static final BigDecimal BD_LONG_MIN_VALUE = BigDecimal.valueOf(Long.MIN_VALUE);
   protected Expression right;

   public UnaryExpression(Expression left) {
      this.right = left;
   }

   public static Expression createNegate(Expression left) {
      return new UnaryExpression(left) {
         @Override
         public Object evaluate(Filterable message) throws FilterException {
            Object rvalue = right.evaluate(message);
            if (rvalue == null) {
               return null;
            }
            if (rvalue instanceof Number) {
               return negate((Number) rvalue);
            }
            return null;
         }

         @Override
         public String getExpressionSymbol() {
            return "-";
         }
      };
   }

   public static BooleanExpression createInExpression(PropertyExpression right,
                                                      List<Object> elements,
                                                      final boolean not) {

      // Use a HashSet if there are many elements.
      Collection<Object> t;
      if (elements.size() == 0) {
         t = null;
      } else if (elements.size() < 5) {
         t = elements;
      } else {
         t = new HashSet<>(elements);
      }
      final Collection<Object> inList = t;

      return new BooleanUnaryExpression(right) {
         @Override
         public Object evaluate(Filterable message) throws FilterException {

            Object rvalue = right.evaluate(message);
            if (rvalue == null) {
               return null;
            }
            if (rvalue.getClass() != String.class) {
               return null;
            }

            return (inList != null && inList.contains(rvalue)) ^ not;
         }

         @Override
         public String toString() {
            StringBuffer answer = new StringBuffer();
            answer.append(right);
            answer.append(" ");
            answer.append(getExpressionSymbol());
            answer.append(" ( ");

            int count = 0;
            for (Object o : inList) {
               if (count != 0) {
                  answer.append(", ");
               }
               answer.append(o);
               count++;
            }

            answer.append(" )");
            return answer.toString();
         }

         @Override
         public String getExpressionSymbol() {
            if (not) {
               return "NOT IN";
            } else {
               return "IN";
            }
         }
      };
   }

   abstract static class BooleanUnaryExpression extends UnaryExpression implements BooleanExpression {

      BooleanUnaryExpression(Expression left) {
         super(left);
      }

      @Override
      public boolean matches(Filterable message) throws FilterException {
         Object object = evaluate(message);
         return object == Boolean.TRUE;
      }
   }

   public static BooleanExpression createNOT(BooleanExpression left) {
      return new BooleanUnaryExpression(left) {
         @Override
         public Object evaluate(Filterable message) throws FilterException {
            Boolean lvalue = (Boolean) right.evaluate(message);
            if (lvalue == null) {
               return null;
            }
            return !lvalue.booleanValue();
         }

         @Override
         public String getExpressionSymbol() {
            return "NOT";
         }
      };
   }

   public static BooleanExpression createXPath(final String xpath) {
      return new XPathExpression(xpath);
   }

   public static BooleanExpression createXQuery(final String xpath) {
      return new XQueryExpression(xpath);
   }

   public static BooleanExpression createBooleanCast(Expression left) {
      return new BooleanUnaryExpression(left) {
         @Override
         public Object evaluate(Filterable message) throws FilterException {
            Object rvalue = right.evaluate(message);
            if (rvalue == null) {
               return null;
            }
            if (!rvalue.getClass().equals(Boolean.class)) {
               return Boolean.FALSE;
            }
            return ((Boolean) rvalue).booleanValue();
         }

         @Override
         public String toString() {
            return right.toString();
         }

         @Override
         public String getExpressionSymbol() {
            return "";
         }
      };
   }

   private static Number negate(Number left) {
      Class clazz = left.getClass();
      if (clazz == Integer.class) {
         return -left.intValue();
      } else if (clazz == Long.class) {
         return -left.longValue();
      } else if (clazz == Float.class) {
         return -left.floatValue();
      } else if (clazz == Double.class) {
         return -left.doubleValue();
      } else if (clazz == BigDecimal.class) {
         // We ussually get a big deciamal when we have Long.MIN_VALUE
         // constant in the
         // Selector. Long.MIN_VALUE is too big to store in a Long as a
         // positive so we store it
         // as a Big decimal. But it gets Negated right away.. to here we try
         // to covert it back
         // to a Long.
         BigDecimal bd = (BigDecimal) left;
         bd = bd.negate();

         if (BD_LONG_MIN_VALUE.compareTo(bd) == 0) {
            return Long.valueOf(Long.MIN_VALUE);
         }
         return bd;
      } else {
         throw new RuntimeException("Don't know how to negate: " + left);
      }
   }

   public Expression getRight() {
      return right;
   }

   public void setRight(Expression expression) {
      right = expression;
   }

   /**
    * @see java.lang.Object#toString()
    */
   @Override
   public String toString() {
      return "(" + getExpressionSymbol() + " " + right.toString() + ")";
   }

   /**
    * @see java.lang.Object#hashCode()
    */
   @Override
   public int hashCode() {
      int result = right.hashCode();
      result = 31 * result + getExpressionSymbol().hashCode();
      return result;
   }

   /**
    * @see java.lang.Object#equals(java.lang.Object)
    */
   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }

      if (o == null || getClass() != o.getClass()) {
         return false;
      }

      final UnaryExpression that = (UnaryExpression) o;

      if (!this.getExpressionSymbol().equals(that.getExpressionSymbol())) {
         return false;
      }

      if (right != null && !right.equals(that.right)) {
         return false;
      }

      return true;
   }

   /**
    * Returns the symbol that represents this binary expression. For example,
    * addition is represented by "+"
    *
    * @return
    */
   public abstract String getExpressionSymbol();

}
