<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE xsl:stylesheet>
<xsl:stylesheet version="1.0"
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:sparql="http://www.w3.org/2005/sparql-results#" xmlns="http://www.w3.org/1999/xhtml">

    <xsl:include href="../locale/messages.xsl"/>

    <xsl:variable name="title">
        <xsl:value-of select="$repository-create.title"/>
    </xsl:variable>

    <xsl:include href="template.xsl"/>

    <xsl:template match="sparql:sparql">
        <form action="create" method="post">
            <table class="dataentry">
                <tbody>
                    <tr style="display: none">
                        <th>
                            <xsl:value-of select="$repository-type.label"/>
                        </th>
                        <td>
                            <select id="type" name="type">
                                <option value="rya">Apache Rya Store</option>
                            </select>
                        </td>
                    </tr>
                    <tr>
                        <th>
                            <xsl:value-of select="$repository-id.label"/>
                        </th>
                        <td>
                            <input id="id" name="Repository ID" size="16" value=""/>
                        </td>
                        <td></td>
                    </tr>
                    <tr>
                        <th>
                            <xsl:value-of select="$repository-title.label"/>
                        </th>
                        <td>
                            <input id="title" name="Repository title" size="48" value=""/>
                        </td>
                        <td></td>
                    </tr>
                    <tr>
                        <th>
                            Table Prefix
                        </th>
                        <td>
                            <input id="tablePrefix" name="Apache Rya Table Prefix" size="48" value="rya_"/>
                        </td>
                        <td></td>
                    </tr>
                    <tr>
                        <th>Accumulo Zookeeper Servers</th>
                        <td>
                            <input id="accumuloZookeeperServers" name="Accumulo Zookeeper Servers" size="48" value=""/>
                        </td>
                        <td></td>
                    </tr>
                    <tr>
                        <th>Accumulo Instance Name</th>
                        <td>
                            <input id="accumuloInstanceName" name="Accumulo Instance Name" size="48" value=""/>
                        </td>
                        <td></td>
                    </tr>
                    <tr>
                        <th>Accumulo Username</th>
                        <td>
                            <input id="accumuloUsername" name="Accumulo Username" size="48" value=""/>
                        </td>
                        <td></td>
                    </tr>
                    <tr>
                        <th>Accumulo Password</th>
                        <td>
                            <input id="accumuloPassword" name="Accumulo Password" size="48" value=""/>
                        </td>
                        <td></td>
                    </tr>
                    <tr>
                        <th>Elasticsearch Host (optional)</th>
                        <td>
                            <input id="elasticsearchHost" name="Elasticsearch Host" size="48" value=""/>
                        </td>
                        <td></td>
                    </tr>
                    <tr>
                        <th>Elasticsearch Max Documents (optional)</th>
                        <td>
                            <input type="number" id="elasticsearchMaxDocuments" name="Elasticsearch Max Documents"
                                   size="6" value="100"/>
                        </td>
                        <td></td>
                    </tr>
                    <tr>
                        <td></td>
                        <td>
                            <input type="button" value="{$cancel.label}" style="float:right"
                                   data-href="repositories"
                                   onclick="document.location.href=this.getAttribute('data-href')"/>
                            <input id="create" type="button" value="{$create.label}"
                                   onclick="checkOverwrite()"/>
                        </td>
                    </tr>
                </tbody>
            </table>
        </form>
        <script src="../../scripts/create.js" type="text/javascript">
        </script>
    </xsl:template>

</xsl:stylesheet>