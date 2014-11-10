/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.hornetq.core.settings.impl;

import java.util.regex.Pattern;

import org.hornetq.core.server.HornetQMessageBundle;

/**
    a Match is the holder for the match string and the object to hold against it.
 */
public class Match<T>
{
   public static final String WORD_WILDCARD = "*";

   private static final String WORD_WILDCARD_REPLACEMENT = "[^.]+";

   public static final String WILDCARD = "#";

   public static final String DOT_WILDCARD = ".#";

   private static final String WILDCARD_REPLACEMENT = ".*";

   private static final String DOT = ".";

   private static final String DOT_REPLACEMENT = "\\.";

   private String match;

   private final Pattern pattern;

   private T value;

   public Match(final String match)
   {
      this.match = match;
      String actMatch = match;
      // replace any regex characters
      if (Match.WILDCARD.equals(match))
      {
         actMatch = Match.WILDCARD_REPLACEMENT;
      }
      else
      {
         // this is to match with what's documented
         actMatch = actMatch.replace(DOT_WILDCARD, WILDCARD);

         actMatch = actMatch.replace(Match.DOT, Match.DOT_REPLACEMENT);
         actMatch = actMatch.replace(Match.WORD_WILDCARD, Match.WORD_WILDCARD_REPLACEMENT);

         // this one has to be done by last as we are using .* and it could be replaced wrongly
         actMatch = actMatch.replace(Match.WILDCARD, Match.WILDCARD_REPLACEMENT);
      }
      pattern = Pattern.compile(actMatch);

   }

   public String getMatch()
   {
      return match;
   }

   public void setMatch(final String match)
   {
      this.match = match;
   }

   public Pattern getPattern()
   {
      return pattern;
   }

   public T getValue()
   {
      return value;
   }

   public void setValue(final T value)
   {
      this.value = value;
   }

   @Override
   public boolean equals(final Object o)
   {
      if (this == o)
      {
         return true;
      }
      if (o == null || getClass() != o.getClass())
      {
         return false;
      }

      @SuppressWarnings("rawtypes")
      Match that = (Match)o;

      return !(match != null ? !match.equals(that.match) : that.match != null);

   }

   @Override
   public int hashCode()
   {
      return match != null ? match.hashCode() : 0;
   }

   /**
    * utility method to verify consistency of match
    * @param match the match to validate
    * @throws IllegalArgumentException if a match isn't valid
    */
   public static void verify(final String match) throws IllegalArgumentException
   {
      if (match == null)
      {
         throw HornetQMessageBundle.BUNDLE.nullMatch();
      }
      if (match.contains("#") && match.indexOf("#") < match.length() - 1)
      {
         throw HornetQMessageBundle.BUNDLE.invalidMatch();
      }
   }

}
