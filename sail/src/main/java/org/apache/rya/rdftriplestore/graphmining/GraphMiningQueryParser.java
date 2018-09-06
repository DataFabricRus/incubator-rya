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

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.query.algebra.EmptySet;
import org.eclipse.rdf4j.query.algebra.SingletonSet;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class GraphMiningQueryParser extends AbstractQueryModelVisitor<Exception> {

    private final Map<Var, GraphMiningQuery> queries;

    public GraphMiningQueryParser() {
        this.queries = new HashMap<>();
    }

    public void validate() throws Exception {
        for (GraphMiningQuery query : queries.values()) {
            query.validate();
        }
    }

    public Collection<GraphMiningQuery> getQueries() {
        return queries.values();
    }

    @Override
    public void meet(StatementPattern node) throws Exception {
        if (node.getPredicateVar().isConstant()) {
            IRI predicateVar = (IRI) node.getPredicateVar().getValue();
            if (predicateVar == null) {
                return;
            }

            if (predicateVar.equals(GraphMiningQuery.PREDICATE_SEARCH)) {
                if (node.getSubjectVar().isConstant()) {
                    Var queryKey = node.getObjectVar();

                    GraphMiningQuery query = queries.get(queryKey);
                    if (query == null) {
                        query = new GraphMiningQuery();
                        queries.put(queryKey, query);
                    }

                    query.setParameter(GraphMiningQuery.PREDICATE_SOURCE, node.getSubjectVar().getValue());

                    node.replaceWith(new SingletonSet());
                }

                return;
            }

            if (GraphMiningQuery.PREDICATE_PARAMETERS.contains(predicateVar)) {
                Var queryKey = node.getSubjectVar();

                GraphMiningQuery query = queries.get(queryKey);
                if (query == null) {
                    query = new GraphMiningQuery();
                    queries.put(queryKey, query);
                }

                if (node.getObjectVar().hasValue()) {
                    query.setParameter(predicateVar, node.getObjectVar().getValue());
                } else {
                    query.setParameter(predicateVar, node.getObjectVar());
                }

                node.replaceWith(new SingletonSet());
            }
        }
    }
}
