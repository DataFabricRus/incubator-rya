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

import org.apache.accumulo.core.client.Connector;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.accumulo.utils.ConnectorFactory;
import org.apache.rya.prospector.service.ProspectorServiceEvalStatsDAO;
import org.apache.rya.rdftriplestore.graphmining.GraphMiningSail;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.config.SailConfigException;
import org.eclipse.rdf4j.sail.config.SailFactory;
import org.eclipse.rdf4j.sail.config.SailImplConfig;
import org.eclipse.rdf4j.sail.elasticsearch.ElasticsearchIndex;
import org.eclipse.rdf4j.sail.evaluation.TupleFunctionEvaluationMode;
import org.eclipse.rdf4j.sail.lucene.LuceneSail;

public class RyaSailFactory implements SailFactory {

    public static final String SAIL_TYPE = "openrdf:ApacheRyaStore";

    @Override
    public String getSailType() {
        return SAIL_TYPE;
    }

    @Override
    public SailImplConfig getConfig() {
        return new RyaSailConfig();
    }

    @Override
    public Sail getSail(SailImplConfig config) throws SailConfigException {
        if (!SAIL_TYPE.equals(config.getType())) {
            throw new SailConfigException("Invalid Sail type: " + config.getType());
        }
        if (config instanceof RyaSailConfig) {
            try {
                RyaSailConfig ryaSailConfig = (RyaSailConfig) config;

                Connector connector = ConnectorFactory.connect(ryaSailConfig.getAccumuloConf());

                ProspectorServiceEvalStatsDAO<AccumuloRdfConfiguration> evalStatsDAO =
                        new ProspectorServiceEvalStatsDAO<>(connector, ryaSailConfig.getAccumuloConf());
                evalStatsDAO.init();

                AccumuloRyaDAO dao = new AccumuloRyaDAO();
                dao.setConnector(connector);
                dao.setConf(ryaSailConfig.getAccumuloConf());
                dao.init();

                RdfCloudTripleStore<AccumuloRdfConfiguration> store = new RdfCloudTripleStore<>();
                store.setConf(ryaSailConfig.getAccumuloConf());
                store.setRyaDAO(dao);
                store.setRdfEvalStatsDAO(evalStatsDAO);

                GraphMiningSail<AccumuloRdfConfiguration> gmSail = new GraphMiningSail<>(store);

                if (ryaSailConfig.getElasticsearchHost() != null) {
                    LuceneSail fullTextSail = new LuceneSail();
                    fullTextSail.setParameter(LuceneSail.INDEX_CLASS_KEY, ElasticsearchIndex.class.getName());
                    fullTextSail.setParameter(LuceneSail.EVALUATION_MODE_KEY, TupleFunctionEvaluationMode.NATIVE.name());
                    fullTextSail.setParameter(LuceneSail.MAX_DOCUMENTS_KEY, "100");
                    fullTextSail.setParameter(ElasticsearchIndex.TRANSPORT_KEY, ryaSailConfig.getElasticsearchHost());
                    fullTextSail.setParameter(ElasticsearchIndex.INDEX_NAME_KEY, ElasticsearchIndex.DEFAULT_INDEX_NAME);
                    fullTextSail.setParameter(ElasticsearchIndex.DOCUMENT_TYPE_KEY, ElasticsearchIndex.DEFAULT_DOCUMENT_TYPE);
                    fullTextSail.setBaseSail(gmSail);

                    return fullTextSail;
                } else {
                    return gmSail;
                }
            } catch (Exception e) {
                throw new SailConfigException(e);
            }
        } else {
            throw new SailConfigException("Invalid configuration: " + config);
        }
    }
}
