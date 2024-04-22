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

package org.apache.activemq.artemis.utils;

import java.io.PrintStream;
import java.util.ArrayList;

public class TableOut {

   final String separator;
   final int[] columnSizes;
   final int indentation;
   final String indentationString;

   public TableOut(String separator, int indentation, int[] columnSizes) {
      this.separator = separator;
      this.columnSizes = columnSizes;
      this.indentation = indentation;

      // building the indentation String to be reused
      StringBuilder indentBuilder = new StringBuilder();
      for (int i = 0; i < indentation; i++) {
         indentBuilder.append(' ');
      }
      indentationString = indentBuilder.toString();
   }

   public void print(PrintStream stream, String[] columns) {
      print(stream, columns, null);
   }

   public void print(PrintStream stream, String[] columns, boolean[] center) {
      ArrayList<String>[] splitColumns = new ArrayList[columns.length];
      for (int i = 0; i < columns.length; i++) {
         splitColumns[i] = splitLine(columns[i], columnSizes[i]);
      }

      print(stream, splitColumns, center);
   }

   public void print(PrintStream stream, ArrayList<String>[] splitColumns) {
      print(stream, splitColumns, null);
   }

   public void print(PrintStream stream, ArrayList<String>[] splitColumns, boolean[] centralize) {
      boolean hasMoreLines;
      int lineNumber = 0;
      do {
         hasMoreLines = false;
         stream.print(separator);
         for (int column = 0; column < splitColumns.length; column++) {
            StringBuilder cell = new StringBuilder();

            String cellString;

            if (lineNumber < splitColumns[column].size()) {
               cellString = splitColumns[column].get(lineNumber);
            } else {
               cellString = "";
            }

            if (centralize != null && centralize[column] && cellString.length() > 0) {
               int centralAdd = (columnSizes[column] - cellString.length()) / 2;
               for (int i = 0; i < centralAdd; i++) {
                  cell.append(' ');
               }
            }

            cell.append(cellString);

            if (lineNumber + 1 < splitColumns[column].size()) {
               hasMoreLines = true;
            }
            while (cell.length() < columnSizes[column]) {
               cell.append(" ");
            }
            stream.print(cell);
            stream.print(separator);
         }
         stream.println();
         lineNumber++;
      }
      while (hasMoreLines);
   }

   public ArrayList<String> splitLine(final String column, int size) {
      ArrayList<String> cells = new ArrayList<>();

      for (int position = 0; position < column.length();) {
         int identationUsed;
         String identationStringUsed;
         if (position == 0 || indentation == 0) {
            identationUsed = 0;
            identationStringUsed = "";
         } else {
            identationUsed = indentation;
            identationStringUsed = this.indentationString;
         }
         int maxPosition = Math.min(size - identationUsed, column.length() - position);
         cells.add(identationStringUsed + column.substring(position, position + maxPosition));
         position += maxPosition;
      }

      return cells;
   }

}
