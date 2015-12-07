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

/**
 * An expression which performs an operation on two expression values.
 *
 * @version $Revision: 1.2 $
 */
public abstract class BinaryExpression implements Expression {

   protected Expression left;
   protected Expression right;

   public BinaryExpression(Expression left, Expression right) {
      this.left = left;
      this.right = right;
   }

   public Expression getLeft() {
      return left;
   }

   public Expression getRight() {
      return right;
   }

   /**
    * @see java.lang.Object#toString()
    */
   @Override
   public String toString() {
      return "(" + left.toString() + " " + getExpressionSymbol() + " " + right.toString() + ")";
   }

   /**
    * @see java.lang.Object#hashCode()
    */
   @Override
   public int hashCode() {
      int result = left.hashCode();
      result = 31 * result + right.hashCode();
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

      final BinaryExpression that = (BinaryExpression) o;

      if (!this.getExpressionSymbol().equals(that.getExpressionSymbol())) {
         return false;
      }

      if (left != null && !left.equals(that.left)) {
         return false;
      }

      if (right != null && !right.equals(that.right)) {
         return false;
      }

      return true;
   }

   /**
    * Returns the symbol that represents this binary expression.  For example, addition is
    * represented by "+"
    *
    * @return
    */
   public abstract String getExpressionSymbol();

   /**
    * @param expression
    */
   public void setRight(Expression expression) {
      right = expression;
   }

   /**
    * @param expression
    */
   public void setLeft(Expression expression) {
      left = expression;
   }

}
