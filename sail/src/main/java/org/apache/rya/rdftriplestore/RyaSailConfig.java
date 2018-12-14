/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.rdftriplestore;

import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.api.RdfCloudTripleStoreConstants;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.Models;
import org.eclipse.rdf4j.sail.config.AbstractSailImplConfig;
import org.eclipse.rdf4j.sail.config.SailConfigException;

public class RyaSailConfig extends AbstractSailImplConfig {

    private static final ValueFactory VF = SimpleValueFactory.getInstance();
    private static final String NAMESPACE = "http://rya.apache.org/sail#";
    private static final IRI TABLE_PREFIX = VF.createIRI(NAMESPACE + "tablePrefix");
    private static final IRI ACCUMULO_INSTANCE_NAME = VF.createIRI(NAMESPACE + "accumuloInstanceName");
    private static final IRI ACCUMULO_USERNAME = VF.createIRI(NAMESPACE + "accumuloUsername");
    private static final IRI ACCUMULO_PASSWORD = VF.createIRI(NAMESPACE + "accumuloPassword");
    private static final IRI ACCUMULO_ZOOKEEPER_SERVERS = VF.createIRI(NAMESPACE + "accumuloZookeeperServers");
    private static final IRI ELASTICSEARCH_HOST = VF.createIRI(NAMESPACE + "elasticsearchHost");
    private static final IRI ELASTICSEARCH_MAX_DOCUMENTS = VF.createIRI(NAMESPACE + "elasticsearchMaxDocuments");

    private AccumuloRdfConfiguration accumuloConf;
    private String elasticsearchHost;
    private int elasticsearchMaxDocuments;

    public AccumuloRdfConfiguration getAccumuloConf() {
        return accumuloConf;
    }

    public String getElasticsearchHost() {
        return elasticsearchHost;
    }

    @Override
    public Resource export(Model m) {
        Resource implNode = super.export(m);

        ValueFactory vf = SimpleValueFactory.getInstance();
        m.add(implNode, TABLE_PREFIX, vf.createLiteral(accumuloConf.getTablePrefix()));

        m.add(implNode, ACCUMULO_INSTANCE_NAME, vf.createLiteral(accumuloConf.getAccumuloInstance()));
        m.add(implNode, ACCUMULO_USERNAME, vf.createLiteral(accumuloConf.getAccumuloUser()));
        m.add(implNode, ACCUMULO_PASSWORD, vf.createLiteral(accumuloConf.getAccumuloPassword()));
        m.add(implNode, ACCUMULO_ZOOKEEPER_SERVERS, vf.createLiteral(accumuloConf.getAccumuloZookeepers()));

        if (elasticsearchHost != null) {
            m.add(implNode, ELASTICSEARCH_HOST, vf.createLiteral(elasticsearchHost));
            m.add(implNode, ELASTICSEARCH_MAX_DOCUMENTS, vf.createLiteral(elasticsearchMaxDocuments));
        }

        return implNode;
    }

    @Override
    public void parse(Model m, Resource implNode) throws SailConfigException {
        super.parse(m, implNode);

        accumuloConf = new AccumuloRdfConfiguration();

        String accumuloInstanceName = Models.getPropertyString(m, implNode, ACCUMULO_INSTANCE_NAME)
                .orElse(null);
        accumuloConf.setAccumuloInstance(accumuloInstanceName);

        String accumuloUsername = Models.getPropertyString(m, implNode, ACCUMULO_USERNAME).orElse(null);
        accumuloConf.setAccumuloUser(accumuloUsername);

        String accumuloPassword = Models.getPropertyString(m, implNode, ACCUMULO_PASSWORD).orElse(null);
        accumuloConf.setAccumuloPassword(accumuloPassword);

        String accumuloZookeeperServers = Models.getPropertyString(m, implNode, ACCUMULO_ZOOKEEPER_SERVERS)
                .orElse(null);
        accumuloConf.setAccumuloZookeepers(accumuloZookeeperServers);

        String tablePrefix = Models.getPropertyString(m, implNode, TABLE_PREFIX)
                .orElse(RdfCloudTripleStoreConstants.TBL_PRFX_DEF);
        accumuloConf.setTablePrefix(tablePrefix);

        elasticsearchHost = Models.getPropertyString(m, implNode, ELASTICSEARCH_HOST).orElse(null);
        if (elasticsearchHost != null) {
            if (elasticsearchHost.isEmpty()) {
                elasticsearchHost = null;
            } else {
                elasticsearchMaxDocuments = Models.getPropertyLiteral(m, implNode, ELASTICSEARCH_MAX_DOCUMENTS)
                        .orElse(SimpleValueFactory.getInstance().createLiteral(100)).intValue();
            }
        }

        accumuloConf.setDisplayQueryPlan(true);
        accumuloConf.setUseStats(true);
    }
}
