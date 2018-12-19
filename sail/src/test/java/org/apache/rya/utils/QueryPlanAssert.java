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
