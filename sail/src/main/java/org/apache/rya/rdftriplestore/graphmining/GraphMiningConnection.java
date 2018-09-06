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
package org.apache.rya.rdftriplestore.graphmining;

import com.google.common.collect.Iterables;
import org.apache.commons.collections.CollectionUtils;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.persist.RyaDAO;
import org.apache.rya.rdftriplestore.RdfCloudTripleStoreConnection;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.EmptyIteration;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.BindingSetAssignment;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.helpers.NotifyingSailConnectionWrapper;
import org.eclipse.rdf4j.sail.helpers.SailConnectionWrapper;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ExecutionException;

public class GraphMiningConnection<C extends RdfCloudTripleStoreConfiguration> extends SailConnectionWrapper {

    private final GraphMiningQueryEngine<C> gmqEngine;

    /**
     * Creates a new {@link NotifyingSailConnectionWrapper} object that wraps the supplied connection.
     *
     * @param wrappedCon
     */
    public GraphMiningConnection(final SailConnection wrappedCon) {
        super(wrappedCon);

        throw new IllegalArgumentException("Only RdfCloudTripleStoreConnection as the wrapped connection is supported!");
    }

    public GraphMiningConnection(final RdfCloudTripleStoreConnection wrappedCon, final RyaDAO<C> dao) {
        super(wrappedCon);

        this.gmqEngine = new GraphMiningQueryEngine<>(dao);
    }

    @Override
    public CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluate(
            TupleExpr tupleExpr, final Dataset dataset, final BindingSet bindings, final boolean includeInferred
    ) throws SailException {
        try {
            tupleExpr = tupleExpr.clone();

            Collection<GraphMiningQuery> graphMiningQueries = parseGraphMiningQueries(tupleExpr);

            if (graphMiningQueries.size() > 1) {
                throw new IllegalArgumentException("Only one graph mining query per SPARQL query is supported!");
            }

            if (!graphMiningQueries.isEmpty()) {
                BindingSetAssignment assignment = evaluateGraphMiningQuery(graphMiningQueries.iterator().next());

                boolean haveCommonVars = !CollectionUtils
                        .intersection(assignment.getBindingNames(), tupleExpr.getBindingNames())
                        .isEmpty();

                if (haveCommonVars) {
                    if (Iterables.isEmpty(assignment.getBindingSets())) {
                        return new EmptyIteration<>();
                    }
                }

                new SingleSetQueryOptimizer().optimize(tupleExpr, dataset, bindings);

                tupleExpr = new Join(assignment, tupleExpr);

                return super.evaluate(tupleExpr, dataset, bindings, includeInferred);
            }
            return super.evaluate(tupleExpr, dataset, bindings, includeInferred);
        } catch (Exception ex) {
            throw new SailException(ex);
        }
    }

    @Override
    public void close() throws SailException {
        try {
            gmqEngine.close();
        } catch (IOException ex) {
            throw new SailException(ex);
        } finally {
            super.close();
        }
    }

    private BindingSetAssignment evaluateGraphMiningQuery(final GraphMiningQuery query)
            throws ExecutionException, InterruptedException {
        Iterable<BindingSet> bindings = gmqEngine.evaluate(query);

        BindingSetAssignment assignment = new BindingSetAssignment();
        assignment.setBindingNames(query.getBindingNames());
        assignment.setBindingSets(bindings);

        return assignment;
    }

    private Collection<GraphMiningQuery> parseGraphMiningQueries(final TupleExpr tupleExpr) throws Exception {
        GraphMiningQueryParser parser = new GraphMiningQueryParser();

        tupleExpr.visit(parser);

        parser.validate();

        return parser.getQueries();
    }
}
