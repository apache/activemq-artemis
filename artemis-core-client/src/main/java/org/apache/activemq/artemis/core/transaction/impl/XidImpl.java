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
package org.apache.activemq.artemis.core.transaction.impl;

import javax.transaction.xa.Xid;
import java.io.Serializable;
import java.util.Arrays;

import org.apache.activemq.artemis.utils.Base64;

/**
 * Xid implementation
 */
public class XidImpl implements Xid, Serializable {

   private static final long serialVersionUID = 407053232840068514L;

   private final byte[] branchQualifier;

   private final int formatId;

   private final byte[] globalTransactionId;

   private int hash;

   private boolean hashCalculated;


   public static String toBase64String(final Xid xid) {
      byte[] data = XidImpl.toByteArray(xid);
      return Base64.encodeBytes(data, true);
   }

   private static byte[] toByteArray(final Xid xid) {
      byte[] branchQualifier = xid.getBranchQualifier();
      byte[] globalTransactionId = xid.getGlobalTransactionId();
      int formatId = xid.getFormatId();

      byte[] hashBytes = new byte[branchQualifier.length + globalTransactionId.length + 4];
      System.arraycopy(branchQualifier, 0, hashBytes, 0, branchQualifier.length);
      System.arraycopy(globalTransactionId, 0, hashBytes, branchQualifier.length, globalTransactionId.length);
      byte[] intBytes = new byte[4];
      for (int i = 0; i < 4; i++) {
         intBytes[i] = (byte) ((formatId >> i * 8) % 0xFF);
      }
      System.arraycopy(intBytes, 0, hashBytes, branchQualifier.length + globalTransactionId.length, 4);
      return hashBytes;
   }



   /**
    * Standard constructor
    */
   public XidImpl(final byte[] branchQualifier, final int formatId, final byte[] globalTransactionId) {
      this.branchQualifier = branchQualifier;
      this.formatId = formatId;
      this.globalTransactionId = globalTransactionId;
   }

   /**
    * Copy constructor
    */
   public XidImpl(final Xid other) {
      branchQualifier = copyBytes(other.getBranchQualifier());
      formatId = other.getFormatId();
      globalTransactionId = copyBytes(other.getGlobalTransactionId());
   }

   @Override
   public byte[] getBranchQualifier() {
      return branchQualifier;
   }

   @Override
   public int getFormatId() {
      return formatId;
   }

   @Override
   public byte[] getGlobalTransactionId() {
      return globalTransactionId;
   }

   @Override
   public int hashCode() {
      if (!hashCalculated) {
         hash = Arrays.hashCode(XidImpl.toByteArray(this));
         hashCalculated = true;
      }
      return hash;
   }

   @Override
   public boolean equals(Object other) {
      if (this == other) {
         return true;
      }
      if (!(other instanceof Xid xother)) {
         return false;
      }

      return formatId == xother.getFormatId() &&
             Arrays.equals(branchQualifier, xother.getBranchQualifier()) &&
             Arrays.equals(globalTransactionId, xother.getGlobalTransactionId());
   }

   @Override
   public String toString() {
      return "XidImpl (" + System.identityHashCode(this) +
         " bq:" +
         stringRep(branchQualifier) +
         " formatID:" +
         formatId +
         " gtxid:" +
         stringRep(globalTransactionId) +
         " base64:" + toBase64String(this);
   }


   private String stringRep(final byte[] bytes) {
      StringBuilder builder = new StringBuilder();
      for (int i = 0; i < bytes.length; i++) {
         byte b = bytes[i];

         builder.append(b);

         if (i != bytes.length - 1) {
            builder.append('.');
         }
      }

      return builder.toString();
   }

   private byte[] copyBytes(final byte[] other) {
      byte[] bytes = new byte[other.length];

      System.arraycopy(other, 0, bytes, 0, other.length);

      return bytes;
   }
}