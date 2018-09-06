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

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.BinaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.EmptySet;
import org.eclipse.rdf4j.query.algebra.Order;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;

public class SingleSetQueryOptimizer extends AbstractQueryModelVisitor<RuntimeException> implements QueryOptimizer {

    @Override
    public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
        tupleExpr.visit(this);
    }

    @Override
    protected void meetBinaryTupleOperator(BinaryTupleOperator node) throws RuntimeException {
        if (node.getLeftArg() instanceof EmptySet) {
            if (node.getRightArg() instanceof EmptySet) {
                node.replaceWith(new EmptySet());

                node.getParentNode().visit(this);
            } else {
                node.replaceWith(node.getRightArg());

                super.meetNode(node);
            }
        } else {
            if (node.getRightArg() instanceof EmptySet) {
                node.replaceWith(node.getLeftArg());

                super.meetNode(node);
            } else {
                super.meetBinaryTupleOperator(node);
            }
        }
    }
}
