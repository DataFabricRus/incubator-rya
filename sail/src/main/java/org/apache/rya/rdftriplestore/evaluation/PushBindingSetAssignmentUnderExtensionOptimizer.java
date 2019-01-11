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
package org.apache.rya.rdftriplestore.evaluation;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.BindingSetAssignment;
import org.eclipse.rdf4j.query.algebra.Extension;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.QueryJoinOptimizer;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;

/**
 * Makes sure that {@link BindingSetAssignment} is lower than {@link Extension} before entering {@link QueryJoinOptimizer},
 * otherwise it makes query suboptimal by putting {@link BindingSetAssignment} on the right side of the join.
 * <p>
 * Example:
 *
 * <pre>
 * Join
 *  BindingSetAssignment(...)
 *  Extension
 *      ExtensionElem (...)
 *          Var (name=...)
 * </pre>
 *
 * will be rewritten to:
 *
 * <pre>
 * Extension
 *  ExtensionElem (...)
 *      Var (name=...)
 *  Join
 *     BindingSetAssignment(...)
 * </pre>
 *
 * @author Maxim Kolchin (kolchinmax@gmail.com)
 * @see <a href="https://github.com/eclipse/rdf4j/issues/1229">RDF4J-1229</a>
 */
public class PushBindingSetAssignmentUnderExtensionOptimizer implements QueryOptimizer {
    @Override
    public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
        tupleExpr.visit(new JoinWithBSAAndExtensionVisitor());
    }

    private class JoinWithBSAAndExtensionVisitor extends AbstractQueryModelVisitor<RuntimeException> {

        @Override
        public void meet(Join node) throws RuntimeException {
            super.meet(node);

            TupleExpr leftArg = node.getLeftArg();
            TupleExpr rightArg = node.getRightArg();

            BindingSetAssignment bindingSet = null;
            Extension extension = null;
            if (leftArg instanceof BindingSetAssignment && rightArg instanceof Extension) {
                bindingSet = (BindingSetAssignment) leftArg;
                extension = (Extension) rightArg;
            } else if (leftArg instanceof Extension && rightArg instanceof BindingSetAssignment) {
                extension = (Extension) leftArg;
                bindingSet = (BindingSetAssignment) rightArg;
            }

            if (bindingSet != null && extension != null) {
                Join newJoin = new Join(bindingSet, extension.getArg().clone());

                extension.getArg().replaceWith(newJoin);

                node.replaceWith(extension);
            }
        }
    }
}
