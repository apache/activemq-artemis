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

import java.util.Objects;

/**
 * An expression which performs an operation on two expression values.
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

   @Override
   public String toString() {
      return "(" + left.toString() + " " + getExpressionSymbol() + " " + right.toString() + ")";
   }

   @Override
   public int hashCode() {
      return Objects.hash(left, right, getExpressionSymbol());
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj) {
         return true;
      }
      if (!(obj instanceof BinaryExpression other)) {
         return false;
      }

      return Objects.equals(this.getExpressionSymbol(), other.getExpressionSymbol()) &&
             Objects.equals(left, other.left) &&
             Objects.equals(right, other.right);
   }

   /**
    * {@return the symbol that represents this binary expression.  For example, addition is represented by {@code +}}
    */
   public abstract String getExpressionSymbol();

   public void setRight(Expression expression) {
      right = expression;
   }

   public void setLeft(Expression expression) {
      left = expression;
   }

}
