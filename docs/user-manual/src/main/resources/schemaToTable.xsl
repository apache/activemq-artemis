<?xml version="1.0" encoding="iso-8859-1"?>

<xsl:stylesheet version="2.0"
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                xmlns:amq="urn:org.apache.activemq"
                xmlns:fn="http://www.w3.org/2005/xpath-functions">

   <!--
    XSL to transform ActiveMQ schemas into DocBook tables to be used in our user manual reference.

    For each option element defined in the schema, it tries to create something like

 <row>
    <entry>
       <link linkend="DocBookIdReference">ELEMENT-NAME</link>
    </entry>
    <entry>TYPE</entry>
    <entry>DESCRIPTION</entry>
    <entry>DEFAULT_VALUE</entry>
 </row>

    TODO:
    - generate Java code with the defaults (instead of duplicating them there).
   -->

   <xsl:output method="xml" indent="yes"/>

   <xsl:function name="amq:name-or-ref">
      <xsl:param name="ename"/>
      <xsl:param name="eref"/>
      <xsl:choose>
         <xsl:when test="string-length($ename) > 0 ">
            <xsl:value-of select="$ename"/>
         </xsl:when>
         <xsl:otherwise>
            <xsl:value-of select="$eref"/>
         </xsl:otherwise>
      </xsl:choose>
   </xsl:function>

   <xsl:template match="/">
      <tbody>

         <xsl:comment>
            THIS IS A GENERATED FILE. DO NOT EDIT IT DIRECTLY!

            ActiveMQ options reference table generated from:
            <xsl:copy-of select="concat('activemq-', substring-after(base-uri(), 'activemq-'))"/>

            =======================================================================

            XSLT Version = <xsl:copy-of select="system-property('xsl:version')"/>
            XSLT Vendor = <xsl:copy-of select="system-property('xsl:vendor')"/>
            XSLT Vendor URL = <xsl:copy-of select="system-property('xsl:vendor-url')"/>
            <xsl:text>&#xa;</xsl:text>
         </xsl:comment>

         <!-- Add some new lines of text.. -->
         <xsl:text>&#xa;</xsl:text>
         <xsl:text>&#xa;</xsl:text>

         <!-- main 'for-each' for "activemq-configuration.xsd". Notice the attribute matching at the
              select which ensures this only matches "activemq-configuration.xsd". -->
         <xsl:for-each-group select="xsd:schema/xsd:element[@amq:schema='activemq-configuration']"
                             group-by="@name">
            <!-- the manual reference should list options in sorted order.
                 Notice that this is only OK because the first level of options is "xsd:all".

                 Options of sub-types are not ordered because they are often "xsd:sequence" (meaning that
                 their order matters), so those options should not have its order changed.
            -->
            <xsl:sort select="@name" data-type="text" />

            <!-- <xsl:if test="not(./xsd:annotation/@amq:linkend)"> -->
            <!--   <xsl:message terminate="yes"> -->
            <!--     Error: Lacks proper Docbook link: xsd:annotation/@amq:linkend -->
            <!--     <xsl:copy-of select="."/> -->
            <!--   </xsl:message> -->
            <!-- </xsl:if> -->
            <xsl:call-template name="xsd_element">
               <xsl:with-param name="prefix"/>
               <xsl:with-param name="parentLinkend" select="xsd:annotation/@amq:linkend"/>
            </xsl:call-template>

            <!-- Add 2 new lines between each option -->
            <xsl:text>&#xa;</xsl:text>
            <xsl:text>&#xa;</xsl:text>
         </xsl:for-each-group>

         <!-- JMS: main 'for-each' for "activemq-jms.xsd". Notice the attribute matching at the
              select which ensures this only matches "activemq-jms.xsd". -->
         <xsl:for-each select="xsd:schema/xsd:element[@amq:schema='activemq-jms-configuration']/xsd:complexType/xsd:sequence/xsd:element">

            <xsl:call-template name="xsd_element">
               <xsl:with-param name="prefix"/>
               <xsl:with-param name="parentLinkend" select="xsd:annotation/@amq:linkend"/>
            </xsl:call-template>

            <!-- Add 2 new lines between each option -->
            <xsl:text>&#xa;</xsl:text>
            <xsl:text>&#xa;</xsl:text>
         </xsl:for-each>

      </tbody>
   </xsl:template>

   <xsl:template name="xsd_complexType" match="xsd:complexType">
      <xsl:param name="prefix" />
      <xsl:param name="linkend" />

      <xsl:comment>
         &#xa;VALUE OF COMPLEX TYPE&#xa;
      </xsl:comment>

      <xsl:apply-templates select="xsd:attribute">
         <xsl:with-param name="prefix" select="$prefix"/>
         <xsl:with-param name="parentLinkend" select="$linkend"/>
      </xsl:apply-templates>

      <xsl:for-each select="(xsd:sequence|xsd:all)/xsd:element">
         <xsl:call-template name="xsd_element">
            <xsl:with-param name="prefix" select="$prefix"/>
            <xsl:with-param name="parentLinkend" select="$linkend"/>
         </xsl:call-template>
      </xsl:for-each>
   </xsl:template>

   <xsl:template name="entry-for-default-value">
      <entry>
         <xsl:value-of select="string-join((@default,xsd:annotation/@amq:default), ' ')"/>
      </entry>
   </xsl:template>

   <xsl:template name="entry-for-description">
      <entry>
         <xsl:value-of select="normalize-space(xsd:annotation/xsd:documentation)"/>
      </entry>
   </xsl:template>

   <xsl:template name="entry-for-simple-type">
      <entry>
         <xsl:value-of select="@type"/>
      </entry>
   </xsl:template>

   <xsl:template name="entry-type-of-complex-element">
      <entry>
         <xsl:choose>
            <xsl:when test="count(xsd:complexType/xsd:sequence/xsd:element)=1">
               <xsl:value-of
                       select="concat('Sequence of &lt;', xsd:complexType/xsd:sequence/xsd:element/( amq:name-or-ref(@name,@ref) ), '/&gt;')"/>
            </xsl:when>
            <xsl:otherwise>
               <xsl:text>Complex element</xsl:text>
            </xsl:otherwise>
         </xsl:choose>
      </entry>
   </xsl:template>

   <xsl:template match="xsd:attribute">
      <xsl:param name="prefix" />
      <xsl:param name="parentLinkend" />

      <xsl:variable name="linkend">
         <xsl:choose>
            <xsl:when test="xsd:annotation/@amq:linkend">
               <xsl:value-of select="xsd:annotation/@amq:linkend"/>
            </xsl:when>
            <xsl:otherwise>
               <xsl:value-of select="$parentLinkend"/>
            </xsl:otherwise>
         </xsl:choose>
      </xsl:variable>

      <row>
         <entry>
            <xsl:element name="link">
               <xsl:attribute name="linkend">
                  <xsl:value-of select="$linkend"/>
               </xsl:attribute>
               <xsl:value-of select="$prefix"/>
               <xsl:value-of select="@name"/>
               <xsl:text> (</xsl:text>
               <xsl:if test="@use = 'required'">
                  <xsl:text>required </xsl:text>
               </xsl:if>
               <xsl:text>attribute)</xsl:text>
            </xsl:element>
         </entry>
         <xsl:call-template name="entry-for-simple-type"/>
         <xsl:call-template name="entry-for-description"/>
         <xsl:call-template name="entry-for-default-value"/>
      </row>
   </xsl:template>


   <!-- ELEMENT -->
   <xsl:template name="xsd_element">
      <xsl:param name="prefix" />
      <xsl:param name="parentLinkend" />

      <xsl:comment>&#xa;ELEMENT TEMPLATE name=<xsl:value-of select="@name"/>
         <xsl:text>&#xa;</xsl:text>
      </xsl:comment>

      <xsl:text>&#xa;</xsl:text>
      <!--

  We handle the following situations (differently):
  1. <element name="foo" />                     find definition using 'name'
  2. <element name="foo>....</element>:         complexType,simpleType,etc
  3. <element name="foo" type="xsd:BUILTIN"/>   'simple' simpleType
  4. <element name="foo" type="fooType"/>       find definition using 'type'
  5. <element ref="foo">                        find definition using 'ref'

      -->

      <xsl:variable name="linkend">
         <xsl:choose>
            <xsl:when test="xsd:annotation/@amq:linkend">
               <xsl:value-of select="xsd:annotation/@amq:linkend"/>
            </xsl:when>
            <xsl:otherwise>
               <xsl:value-of select="$parentLinkend"/>
            </xsl:otherwise>
         </xsl:choose>
      </xsl:variable>

      <xsl:choose>

         <xsl:when test="@name">
            <!-- cases 1,2,3,4 -->

            <xsl:choose>
               <!-- XSD:BUILT-IN -->
               <xsl:when test="starts-with(string(@type), 'xsd:')">
                  <row>
                     <xsl:call-template name="entry-for-name">
                        <xsl:with-param name="prefix" select="$prefix"/>
                        <xsl:with-param name="linkend" select="$linkend"/>
                     </xsl:call-template>
                     <xsl:call-template name="entry-for-simple-type"/>
                     <xsl:call-template name="entry-for-description"/>
                     <xsl:call-template name="entry-for-default-value"/>
                  </row>
               </xsl:when>

               <xsl:when test="not(@type)">
                  <xsl:choose>
                     <xsl:when test="xsd:complexType">
                        <row>
                           <xsl:call-template name="entry-for-name">
                              <xsl:with-param name="prefix" select="$prefix"/>
                              <xsl:with-param name="linkend" select="$linkend"/>
                           </xsl:call-template>
                           <xsl:call-template name="entry-type-of-complex-element"/>
                           <xsl:call-template name="entry-for-description"/>
                           <entry/> <!-- complexTypes have no 'default' value. -->
                        </row>

                        <!-- backup-servers is a recursive type, which I need to take it out otherwise
                             there would be an infinite recursive loop -->
                        <xsl:if test="fn:not(@name='backup-servers')">
                           <!-- Process child nodes -->
                           <xsl:apply-templates select="xsd:complexType">
                              <xsl:with-param name="prefix" select="concat($prefix,@name,'.')"/>
                              <xsl:with-param name="linkend" select="$linkend"/>
                           </xsl:apply-templates>
                        </xsl:if>
                     </xsl:when>

                     <!-- simpleType. Normally string restrictions: A|B|C -->
                     <xsl:when test="xsd:simpleType">
                        <row>
                           <xsl:call-template name="entry-for-name">
                              <xsl:with-param name="prefix" select="$prefix"/>
                              <xsl:with-param name="linkend" select="$linkend"/>
                           </xsl:call-template>
                           <entry>
                              <xsl:if test="xsd:simpleType/xsd:restriction[@base='xsd:string']">
                                 <xsl:value-of select="fn:string-join(xsd:simpleType/xsd:restriction/xsd:enumeration/@value,'|')"/>
                              </xsl:if>
                           </entry>
                           <xsl:call-template name="entry-for-description"/>
                           <xsl:call-template name="entry-for-default-value"/>
                        </row>
                     </xsl:when>

                     <xsl:otherwise>
                        <!-- the NAME is a reference -->
                        <xsl:comment>&#xa;E.NAME-ref name=<xsl:copy-of select="@name"/><xsl:text>&#xa;</xsl:text></xsl:comment>
                        <xsl:variable name="my-type" select="@name"/>
                        <row>
                           <xsl:call-template name="entry-for-name">
                              <xsl:with-param name="prefix" select="$prefix"/>
                              <xsl:with-param name="linkend" select="$linkend"/>
                           </xsl:call-template>
                           <xsl:call-template name="entry-type-of-complex-element"/>
                           <entry>
                              <xsl:value-of select="normalize-space(xsd:annotation/xsd:documentation)"/>
                              <xsl:value-of select="normalize-space(/xsd:schema/xsd:complexType[@name=$my-type]/xsd:annotation/xsd:documentation)"/>
                           </entry>
                           <entry/> <!-- no default value -->
                        </row>
                        <xsl:apply-templates select="/xsd:schema/xsd:complexType[@name=$my-type]">
                           <xsl:with-param name="prefix" select="concat($prefix,@name,'.')"/>
                           <xsl:with-param name="linkend" select="$linkend"/>
                        </xsl:apply-templates>
                     </xsl:otherwise>
                  </xsl:choose>
               </xsl:when>

               <xsl:otherwise>
                  <!-- the TYPE is a reference -->
                  <!-- There is a type but it is not built-in, so we need to find its definition -->
                  <xsl:variable name="my-type" select="@type"/>
                  <row>
                     <xsl:call-template name="entry-for-name">
                        <xsl:with-param name="prefix" select="$prefix"/>
                        <xsl:with-param name="linkend" select="$linkend"/>
                     </xsl:call-template>
                     <xsl:call-template name="entry-type-of-complex-element"/>
                     <entry>
                        <xsl:value-of select="normalize-space(xsd:annotation/xsd:documentation)"/>
                        <xsl:value-of select="normalize-space(/xsd:schema/xsd:complexType[@name=$my-type]/xsd:annotation/xsd:documentation)"/>
                     </entry>
                     <entry/> <!-- no default value -->
                  </row>
                  <xsl:comment>&#xa;ELEMENT TYPE-ref TEMPLATE CHILDREN&#xa;</xsl:comment>
                  <xsl:text>&#xa;</xsl:text>
                  <xsl:apply-templates select="/xsd:schema/xsd:complexType[@name=$my-type]">
                     <xsl:with-param name="prefix" select="concat($prefix,@name,'.')"/>
                     <xsl:with-param name="linkend" select="$linkend"/>
                  </xsl:apply-templates>
               </xsl:otherwise>
            </xsl:choose>

         </xsl:when>

         <xsl:when test="@ref">
            <xsl:comment>&#xa;ELEMENT REF TEMPLATE ref=<xsl:copy-of select="@ref"/>&#xa;</xsl:comment>
            <xsl:text>&#xa;</xsl:text>

            <xsl:variable name="my-ref" select="@ref"/>
            <xsl:for-each select="/xsd:schema/xsd:element[ @name = string($my-ref) ]">
               <xsl:call-template name="xsd_element">
                  <xsl:with-param name="prefix" select="$prefix"/>
                  <xsl:with-param name="parentLinkend" select="$linkend"/>
               </xsl:call-template>
            </xsl:for-each>
         </xsl:when>
         <xsl:otherwise>
            <!-- FAIL? -->
         </xsl:otherwise>
      </xsl:choose>
   </xsl:template>

   <xsl:template name="entry-for-name">
      <xsl:param name="prefix" />
      <xsl:param name="linkend" />

      <entry>
         <xsl:element name="link">
            <xsl:attribute name="linkend">
               <xsl:value-of select="$linkend"/>
            </xsl:attribute>
            <xsl:value-of select="$prefix"/>
            <xsl:value-of select="@name"/>
         </xsl:element>
      </entry>
   </xsl:template>

</xsl:stylesheet>
