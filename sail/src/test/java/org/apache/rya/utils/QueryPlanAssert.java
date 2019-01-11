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
package org.apache.rya.utils;

import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.junit.Assert;

import java.util.HashMap;
import java.util.Map;

public class QueryPlanAssert {

    public static void assertEquals(String expected, TupleExpr actual) throws AssertionError {
        TupleExpr clone = actual.clone();

        try {
            clone.visit(new BNodeNormalizer());
        } catch (Exception ex) {
            throw new AssertionError(ex);
        }

        Assert.assertEquals(expected, clone.toString());
    }

    private static class BNodeNormalizer extends AbstractQueryModelVisitor<Exception> {

        private Map<String, String> varIds = new HashMap<>();
        private int nextVarId = 0;

        @Override
        public void meet(Var node) throws Exception {
            if (node.isConstant()) {
                String varName = node.getName();

                if (!varIds.containsKey(varName)) {
                    String bnId = "_const_" + nextVarId + "_uri";
                    varIds.put(varName, bnId);
                    nextVarId++;
                }

                node.setName(varIds.get(varName));
            }

            super.meetNode(node);
        }
    }

}
