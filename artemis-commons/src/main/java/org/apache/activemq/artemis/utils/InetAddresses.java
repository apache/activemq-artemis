/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.utils;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Locale;

import static org.apache.activemq.artemis.utils.ByteUtil.intFromBytes;

/**
 * Static utility methods pertaining to {@link InetAddress} instances.
 *
 * <p><b>Important note:</b> Unlike {@code InetAddress.getByName()}, the
 * methods of this class never cause DNS services to be accessed. For
 * this reason, you should prefer these methods as much as possible over
 * their JDK equivalents whenever you are expecting to handle only
 * IP address string literals -- there is no blocking DNS penalty for a
 * malformed string.
 *
 * <p>When dealing with {@link Inet4Address} and {@link Inet6Address}
 * objects as byte arrays (vis. {@code InetAddress.getAddress()}) they
 * are 4 and 16 bytes in length, respectively, and represent the address
 * in network byte order.
 *
 * <p>Examples of IP addresses and their byte representations:
 * <ul>
 * <li>The IPv4 loopback address, {@code "127.0.0.1"}.<br>
 *   {@code 7f 00 00 01}
 *
 * <li>The IPv6 loopback address, {@code "::1"}.<br>
 *   {@code 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 01}
 *
 * <li>From the IPv6 reserved documentation prefix ({@code 2001:db8::/32}),
 *   {@code "2001:db8::1"}.<br>
 *   {@code 20 01 0d b8 00 00 00 00 00 00 00 00 00 00 00 01}
 *
 * <li>An IPv6 "IPv4 compatible" (or "compat") address,
 *   {@code "::192.168.0.1"}.<br>
 *   {@code 00 00 00 00 00 00 00 00 00 00 00 00 c0 a8 00 01}
 *
 * <li>An IPv6 "IPv4 mapped" address, {@code "::ffff:192.168.0.1"}.<br>
 *   {@code 00 00 00 00 00 00 00 00 00 00 ff ff c0 a8 00 01}
 * </ul>
 *
 * <p>A few notes about IPv6 "IPv4 mapped" addresses and their observed
 * use in Java.
 * <br><br>
 * "IPv4 mapped" addresses were originally a representation of IPv4
 * addresses for use on an IPv6 socket that could receive both IPv4
 * and IPv6 connections (by disabling the {@code IPV6_V6ONLY} socket
 * option on an IPv6 socket).  Yes, it's confusing.  Nevertheless,
 * these "mapped" addresses were never supposed to be seen on the
 * wire.  That assumption was dropped, some say mistakenly, in later
 * RFCs with the apparent aim of making IPv4-to-IPv6 transition simpler.
 *
 * <p>Technically one <i>can</i> create a 128bit IPv6 address with the wire
 * format of a "mapped" address, as shown above, and transmit it in an
 * IPv6 packet header.  However, Java's InetAddress creation methods
 * appear to adhere doggedly to the original intent of the "mapped"
 * address: all "mapped" addresses return {@link Inet4Address} objects.
 *
 * <p>For added safety, it is common for IPv6 network operators to filter
 * all packets where either the source or destination address appears to
 * be a "compat" or "mapped" address.  Filtering suggestions usually
 * recommend discarding any packets with source or destination addresses
 * in the invalid range {@code ::/3}, which includes both of these bizarre
 * address formats.  For more information on "bogons", including lists
 * of IPv6 bogon space, see:
 *
 * <ul>
 * <li><a target="_parent"
 *     href="http://en.wikipedia.org/wiki/Bogon_filtering"
 *     >http://en.wikipedia.org/wiki/Bogon_filtering</a>
 * <li><a target="_parent"
 *     href="http://www.cymru.com/Bogons/ipv6.txt"
 *     >http://www.cymru.com/Bogons/ipv6.txt</a>
 * <li><a target="_parent"
 *     href="http://www.cymru.com/Bogons/v6bogon.html"
 *     >http://www.cymru.com/Bogons/v6bogon.html</a>
 * <li><a target="_parent"
 *     href="http://www.space.net/~gert/RIPE/ipv6-filters.html"
 *     >http://www.space.net/~gert/RIPE/ipv6-filters.html</a>
 * </ul>
 *
 */
public final class InetAddresses {
   private static final int IPV4_PART_COUNT = 4;
   private static final int IPV6_PART_COUNT = 8;
   private static final Inet4Address LOOPBACK4 = (Inet4Address) forString("127.0.0.1");
   private static final Inet4Address ANY4 = (Inet4Address) forString("0.0.0.0");

   private InetAddresses() {
   }

   /**
    * Returns an {@link Inet4Address}, given a byte array representation of the IPv4 address.
    *
    * @param bytes byte array representing an IPv4 address (should be of length 4)
    * @return {@link Inet4Address} corresponding to the supplied byte array
    * @throws IllegalArgumentException if a valid {@link Inet4Address} can not be created
    */
   private static Inet4Address getInet4Address(byte[] bytes) {
      Preconditions.checkArgument(bytes.length == 4,
            "Byte array has invalid length for an IPv4 address: %s != 4.",
            bytes.length);

      // Given a 4-byte array, this cast should always succeed.
      return (Inet4Address) bytesToInetAddress(bytes);
   }

   /**
    * Returns the {@link InetAddress} having the given string representation.
    *
    * <p>This deliberately avoids all nameservice lookups (e.g. no DNS).
    *
    * @param ipString {@code String} containing an IPv4 or IPv6 string literal, e.g.
    *   {@code "192.168.0.1"} or {@code "2001:db8::1"}
    * @return {@link InetAddress} representing the argument
    * @throws IllegalArgumentException if the argument is not a valid IP string literal
    */
   public static InetAddress forString(String ipString) {
      byte[] addr = ipStringToBytes(ipString);

      // The argument was malformed, i.e. not an IP string literal.
      if (addr == null) {
         throw formatIllegalArgumentException("'%s' is not an IP string literal.", ipString);
      }

      return bytesToInetAddress(addr);
   }

   /**
    * Returns {@code true} if the supplied string is a valid IP string
    * literal, {@code false} otherwise.
    *
    * @param ipString {@code String} to evaluated as an IP string literal
    * @return {@code true} if the argument is a valid IP string literal
    */
   public static boolean isInetAddress(String ipString) {
      return ipStringToBytes(ipString) != null;
   }

   private static byte[] ipStringToBytes(String ipString) {
      // Make a first pass to categorize the characters in this string.
      boolean hasColon = false;
      boolean hasDot = false;
      for (int i = 0; i < ipString.length(); i++) {
         char c = ipString.charAt(i);
         if (c == '.') {
            hasDot = true;
         } else if (c == ':') {
            if (hasDot) {
               return null;  // Colons must not appear after dots.
            }
            hasColon = true;
         } else if (Character.digit(c, 16) == -1) {
            return null;  // Everything else must be a decimal or hex digit.
         }
      }

      // Now decide which address family to parse.
      if (hasColon) {
         if (hasDot) {
            ipString = convertDottedQuadToHex(ipString);
            if (ipString == null) {
               return null;
            }
         }
         return textToNumericFormatV6(ipString);
      } else if (hasDot) {
         return textToNumericFormatV4(ipString);
      }
      return null;
   }

   private static byte[] textToNumericFormatV4(String ipString) {
      String[] address = ipString.split("\\.", IPV4_PART_COUNT + 1);
      if (address.length != IPV4_PART_COUNT) {
         return null;
      }

      byte[] bytes = new byte[IPV4_PART_COUNT];
      try {
         for (int i = 0; i < bytes.length; i++) {
            bytes[i] = parseOctet(address[i]);
         }
      } catch (NumberFormatException ex) {
         return null;
      }

      return bytes;
   }

   private static byte[] textToNumericFormatV6(String ipString) {
      // An address can have [2..8] colons, and N colons make N+1 parts.
      String[] parts = ipString.split(":", IPV6_PART_COUNT + 2);
      if (parts.length < 3 || parts.length > IPV6_PART_COUNT + 1) {
         return null;
      }

      // Disregarding the endpoints, find "::" with nothing in between.
      // This indicates that a run of zeroes has been skipped.
      int skipIndex = -1;
      for (int i = 1; i < parts.length - 1; i++) {
         if (parts[i].length() == 0) {
            if (skipIndex >= 0) {
               return null;  // Can't have more than one ::
            }
            skipIndex = i;
         }
      }

      int partsHi;  // Number of parts to copy from above/before the "::"
      int partsLo;  // Number of parts to copy from below/after the "::"
      if (skipIndex >= 0) {
         // If we found a "::", then check if it also covers the endpoints.
         partsHi = skipIndex;
         partsLo = parts.length - skipIndex - 1;
         if (parts[0].length() == 0 && --partsHi != 0) {
            return null;  // ^: requires ^::
         }
         if (parts[parts.length - 1].length() == 0 && --partsLo != 0) {
            return null;  // :$ requires ::$
         }
      } else {
         // Otherwise, allocate the entire address to partsHi.  The endpoints
         // could still be empty, but parseHextet() will check for that.
         partsHi = parts.length;
         partsLo = 0;
      }

      // If we found a ::, then we must have skipped at least one part.
      // Otherwise, we must have exactly the right number of parts.
      int partsSkipped = IPV6_PART_COUNT - (partsHi + partsLo);
      if (!(skipIndex >= 0 ? partsSkipped >= 1 : partsSkipped == 0)) {
         return null;
      }

      // Now parse the hextets into a byte array.
      ByteBuffer rawBytes = ByteBuffer.allocate(2 * IPV6_PART_COUNT);
      try {
         for (int i = 0; i < partsHi; i++) {
            rawBytes.putShort(parseHextet(parts[i]));
         }
         for (int i = 0; i < partsSkipped; i++) {
            rawBytes.putShort((short) 0);
         }
         for (int i = partsLo; i > 0; i--) {
            rawBytes.putShort(parseHextet(parts[parts.length - i]));
         }
      } catch (NumberFormatException ex) {
         return null;
      }
      return rawBytes.array();
   }

   private static String convertDottedQuadToHex(String ipString) {
      int lastColon = ipString.lastIndexOf(':');
      String initialPart = ipString.substring(0, lastColon + 1);
      String dottedQuad = ipString.substring(lastColon + 1);
      byte[] quad = textToNumericFormatV4(dottedQuad);
      if (quad == null) {
         return null;
      }
      String penultimate = Integer.toHexString(((quad[0] & 0xff) << 8) | (quad[1] & 0xff));
      String ultimate = Integer.toHexString(((quad[2] & 0xff) << 8) | (quad[3] & 0xff));
      return initialPart + penultimate + ":" + ultimate;
   }

   private static byte parseOctet(String ipPart) {
      // Note: we already verified that this string contains only hex digits.
      int octet = Integer.parseInt(ipPart);
      // Disallow leading zeroes, because no clear standard exists on
      // whether these should be interpreted as decimal or octal.
      if (octet > 255 || (ipPart.startsWith("0") && ipPart.length() > 1)) {
         throw new NumberFormatException();
      }
      return (byte) octet;
   }

   private static short parseHextet(String ipPart) {
      // Note: we already verified that this string contains only hex digits.
      int hextet = Integer.parseInt(ipPart, 16);
      if (hextet > 0xffff) {
         throw new NumberFormatException();
      }
      return (short) hextet;
   }

   /**
    * Convert a byte array into an InetAddress.
    *
    * {@link InetAddress#getByAddress} is documented as throwing a checked
    * exception "if IP address is of illegal length."  We replace it with
    * an unchecked exception, for use by callers who already know that addr
    * is an array of length 4 or 16.
    *
    * @param addr the raw 4-byte or 16-byte IP address in big-endian order
    * @return an InetAddress object created from the raw IP address
    */
   private static InetAddress bytesToInetAddress(byte[] addr) {
      try {
         return InetAddress.getByAddress(addr);
      } catch (UnknownHostException e) {
         throw new AssertionError(e);
      }
   }

   /**
    * Returns the string representation of an {@link InetAddress}.
    *
    * <p>For IPv4 addresses, this is identical to
    * {@link InetAddress#getHostAddress()}, but for IPv6 addresses, the output
    * follows <a href="http://tools.ietf.org/html/rfc5952">RFC 5952</a>
    * section 4.  The main difference is that this method uses "::" for zero
    * compression, while Java's version uses the uncompressed form.
    *
    * <p>This method uses hexadecimal for all IPv6 addresses, including
    * IPv4-mapped IPv6 addresses such as "::c000:201".  The output does not
    * include a Scope ID.
    *
    * @param ip {@link InetAddress} to be converted to an address string
    * @return {@code String} containing the text-formatted IP address
    * @since 10.0
    */
   public static String toAddrString(InetAddress ip) {
      Preconditions.checkNotNull(ip);
      if (ip instanceof Inet4Address) {
         // For IPv4, Java's formatting is good enough.
         return ip.getHostAddress();
      }
      Preconditions.checkArgument(ip instanceof Inet6Address);
      byte[] bytes = ip.getAddress();
      int[] hextets = new int[IPV6_PART_COUNT];
      for (int i = 0; i < hextets.length; i++) {
         hextets[i] = intFromBytes((byte) 0, (byte) 0, bytes[2 * i], bytes[2 * i + 1]);
      }
      compressLongestRunOfZeroes(hextets);
      return hextetsToIPv6String(hextets);
   }

   /**
    * Identify and mark the longest run of zeroes in an IPv6 address.
    *
    * <p>Only runs of two or more hextets are considered.  In case of a tie, the
    * leftmost run wins.  If a qualifying run is found, its hextets are replaced
    * by the sentinel value -1.
    *
    * @param hextets {@code int[]} mutable array of eight 16-bit hextets
    */
   private static void compressLongestRunOfZeroes(int[] hextets) {
      int bestRunStart = -1;
      int bestRunLength = -1;
      int runStart = -1;
      for (int i = 0; i < hextets.length + 1; i++) {
         if (i < hextets.length && hextets[i] == 0) {
            if (runStart < 0) {
               runStart = i;
            }
         } else if (runStart >= 0) {
            int runLength = i - runStart;
            if (runLength > bestRunLength) {
               bestRunStart = runStart;
               bestRunLength = runLength;
            }
            runStart = -1;
         }
      }
      if (bestRunLength >= 2) {
         Arrays.fill(hextets, bestRunStart, bestRunStart + bestRunLength, -1);
      }
   }

   /**
    * Convert a list of hextets into a human-readable IPv6 address.
    *
    * <p>In order for "::" compression to work, the input should contain negative
    * sentinel values in place of the elided zeroes.
    *
    * @param hextets {@code int[]} array of eight 16-bit hextets, or -1s
    */
   private static String hextetsToIPv6String(int[] hextets) {
      /*
       * While scanning the array, handle these state transitions:
       *   start->num => "num"   start->gap => "::"
       *   num->num   => ":num"   num->gap   => "::"
       *   gap->num   => "num"   gap->gap   => ""
       */
      StringBuilder buf = new StringBuilder(39);
      boolean lastWasNumber = false;
      for (int i = 0; i < hextets.length; i++) {
         boolean thisIsNumber = hextets[i] >= 0;
         if (thisIsNumber) {
            if (lastWasNumber) {
               buf.append(':');
            }
            buf.append(Integer.toHexString(hextets[i]));
         } else {
            if (i == 0 || lastWasNumber) {
               buf.append("::");
            }
         }
         lastWasNumber = thisIsNumber;
      }
      return buf.toString();
   }

   /**
    * Returns the string representation of an {@link InetAddress} suitable
    * for inclusion in a URI.
    *
    * <p>For IPv4 addresses, this is identical to
    * {@link InetAddress#getHostAddress()}, but for IPv6 addresses it
    * compresses zeroes and surrounds the text with square brackets; for example
    * {@code "[2001:db8::1]"}.
    *
    * <p>Per section 3.2.2 of
    * <a target="_parent"
    *   href="http://tools.ietf.org/html/rfc3986#section-3.2.2"
    *  >http://tools.ietf.org/html/rfc3986</a>,
    * a URI containing an IPv6 string literal is of the form
    * {@code "http://[2001:db8::1]:8888/index.html"}.
    *
    * <p>Use of either {@link InetAddresses#toAddrString},
    * {@link InetAddress#getHostAddress()}, or this method is recommended over
    * {@link InetAddress#toString()} when an IP address string literal is
    * desired.  This is because {@link InetAddress#toString()} prints the
    * hostname and the IP address string joined by a "/".
    *
    * @param ip {@link InetAddress} to be converted to URI string literal
    * @return {@code String} containing URI-safe string literal
    */
   public static String toUriString(InetAddress ip) {
      if (ip instanceof Inet6Address) {
         return "[" + toAddrString(ip) + "]";
      }
      return toAddrString(ip);
   }

   /**
    * Returns an InetAddress representing the literal IPv4 or IPv6 host
    * portion of a URL, encoded in the format specified by RFC 3986 section 3.2.2.
    *
    * <p>This function is similar to {@link InetAddresses#forString(String)},
    * however, it requires that IPv6 addresses are surrounded by square brackets.
    *
    * <p>This function is the inverse of
    * {@link InetAddresses#toUriString(java.net.InetAddress)}.
    *
    * @param hostAddr A RFC 3986 section 3.2.2 encoded IPv4 or IPv6 address
    * @return an InetAddress representing the address in {@code hostAddr}
    * @throws IllegalArgumentException if {@code hostAddr} is not a valid
    *   IPv4 address, or IPv6 address surrounded by square brackets
    */
   public static InetAddress forUriString(String hostAddr) {
      Preconditions.checkNotNull(hostAddr);

      // Decide if this should be an IPv6 or IPv4 address.
      String ipString;
      int expectBytes;
      if (hostAddr.startsWith("[") && hostAddr.endsWith("]")) {
         ipString = hostAddr.substring(1, hostAddr.length() - 1);
         expectBytes = 16;
      } else {
         ipString = hostAddr;
         expectBytes = 4;
      }

      // Parse the address, and make sure the length/version is correct.
      byte[] addr = ipStringToBytes(ipString);
      if (addr == null || addr.length != expectBytes) {
         throw formatIllegalArgumentException("Not a valid URI IP literal: '%s'", hostAddr);
      }

      return bytesToInetAddress(addr);
   }

   /**
    * Returns {@code true} if the supplied string is a valid URI IP string
    * literal, {@code false} otherwise.
    *
    * @param ipString {@code String} to evaluated as an IP URI host string literal
    * @return {@code true} if the argument is a valid IP URI host
    */
   public static boolean isUriInetAddress(String ipString) {
      try {
         forUriString(ipString);
         return true;
      } catch (IllegalArgumentException e) {
         return false;
      }
   }

   /**
    * Evaluates whether the argument is an IPv6 "compat" address.
    *
    * <p>An "IPv4 compatible", or "compat", address is one with 96 leading
    * bits of zero, with the remaining 32 bits interpreted as an
    * IPv4 address.  These are conventionally represented in string
    * literals as {@code "::192.168.0.1"}, though {@code "::c0a8:1"} is
    * also considered an IPv4 compatible address (and equivalent to
    * {@code "::192.168.0.1"}).
    *
    * <p>For more on IPv4 compatible addresses see section 2.5.5.1 of
    * <a target="_parent"
    *   href="http://tools.ietf.org/html/rfc4291#section-2.5.5.1"
    *   >http://tools.ietf.org/html/rfc4291</a>
    *
    * <p>NOTE: This method is different from
    * {@link Inet6Address#isIPv4CompatibleAddress} in that it more
    * correctly classifies {@code "::"} and {@code "::1"} as
    * proper IPv6 addresses (which they are), NOT IPv4 compatible
    * addresses (which they are generally NOT considered to be).
    *
    * @param ip {@link Inet6Address} to be examined for embedded IPv4 compatible address format
    * @return {@code true} if the argument is a valid "compat" address
    */
   public static boolean isCompatIPv4Address(Inet6Address ip) {
      if (!ip.isIPv4CompatibleAddress()) {
         return false;
      }

      byte[] bytes = ip.getAddress();
      if ((bytes[12] == 0) && (bytes[13] == 0) && (bytes[14] == 0)
            && ((bytes[15] == 0) || (bytes[15] == 1))) {
         return false;
      }

      return true;
   }

   /**
    * Returns the IPv4 address embedded in an IPv4 compatible address.
    *
    * @param ip {@link Inet6Address} to be examined for an embedded IPv4 address
    * @return {@link Inet4Address} of the embedded IPv4 address
    * @throws IllegalArgumentException if the argument is not a valid IPv4 compatible address
    */
   public static Inet4Address getCompatIPv4Address(Inet6Address ip) {
      Preconditions.checkArgument(isCompatIPv4Address(ip),
            "Address '%s' is not IPv4-compatible.", toAddrString(ip));

      return getInet4Address(Arrays.copyOfRange(ip.getAddress(), 12, 16));
   }

   private static IllegalArgumentException formatIllegalArgumentException(
         String format, Object... args) {
      return new IllegalArgumentException(String.format(Locale.ROOT, format, args));
   }

}
