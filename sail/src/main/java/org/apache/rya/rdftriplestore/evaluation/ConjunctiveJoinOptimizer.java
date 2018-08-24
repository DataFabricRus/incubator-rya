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
import org.eclipse.rdf4j.query.algebra.*;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.EvaluationStatistics;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.helpers.StatementPatternCollector;

import java.util.*;
import java.util.stream.Collectors;

/**
 * A copy-paste from {@link SimilarVarJoinOptimizer}.
 */
public class ConjunctiveJoinOptimizer extends AbstractQueryModelVisitor<RuntimeException> implements QueryOptimizer {

    private final EvaluationStatistics statistics;
    private final Set<Var> boundVars;

    public ConjunctiveJoinOptimizer() {
        this(new EvaluationStatistics());
    }

    public ConjunctiveJoinOptimizer(EvaluationStatistics statistics) {
        this.statistics = statistics;
        this.boundVars = new HashSet<>();
    }

    /**
     * Applies generally applicable optimizations: path expressions are sorted
     * from more to less specific.
     *
     * @param tupleExpr
     */
    @Override
    public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
        tupleExpr.visit(this);
    }

    @Override
    public void meet(LeftJoin leftJoin) {
        leftJoin.getLeftArg().visit(this);
        leftJoin.getRightArg().visit(this);
    }

    @Override
    public void meet(Join node) {
        // Recursively get the join arguments
        List<TupleExpr> joinArgs = getJoinArgs(node);

        // Build maps of cardinalities and vars per tuple expression
        Map<TupleExpr, Double> cardinalityMap = joinArgs
                .stream()
                .map(it -> new AbstractMap.SimpleEntry<>(it, statistics.getCardinality(it)))
                .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));

        // Reorder the (recursive) join arguments to a more optimal sequence
        List<TupleExpr> orderedJoinArgs = new ArrayList<>(joinArgs.size());
        while (!joinArgs.isEmpty()) {
            final TupleExpr tupleExpr = selectNextTupleExpr(joinArgs, cardinalityMap, boundVars);
            if (tupleExpr == null) {
                break;
            }

            joinArgs.remove(tupleExpr);
            orderedJoinArgs.add(tupleExpr);

            boundVars.addAll(getTupleExprNonConstantVars(tupleExpr));

            // Recursively optimize join arguments
            //@todo Is it really necessary?
            tupleExpr.visit(this);
        }

        // Build new join hierarchy
        // Note: generated hierarchy is right-recursive to help the
        // IterativeEvaluationOptimizer to factor out the left-most join
        // argument
        int i = 0;
        TupleExpr replacement = orderedJoinArgs.get(i);
        for (i++; i < orderedJoinArgs.size(); i++) {
            replacement = new Join(replacement, orderedJoinArgs.get(i));
        }

        // Replace old join hierarchy
        node.replaceWith(replacement);
    }

    private List<TupleExpr> getJoinArgs(final TupleExpr tupleExpr) {
        return getJoinArgs(tupleExpr, new ArrayList<>());
    }

    private List<TupleExpr> getJoinArgs(final TupleExpr tupleExpr, final List<TupleExpr> joinArgs) {
        if (tupleExpr instanceof Join) {
            Join join = (Join) tupleExpr;
            getJoinArgs(join.getLeftArg(), joinArgs);
            getJoinArgs(join.getRightArg(), joinArgs);
        } else {
            joinArgs.add(tupleExpr);
        }

        return joinArgs;
    }

    /**
     * @param tupleExpr the tuple
     * @return all vars in the tuple
     */
    private List<Var> getTupleExprNonConstantVars(final TupleExpr tupleExpr) {
        if (tupleExpr != null) {
            if (tupleExpr instanceof BindingSetAssignment) {
                return tupleExpr.getBindingNames()
                        .parallelStream()
                        .map(Var::new)
                        .collect(Collectors.toList());
            } else {
                return StatementPatternCollector.process(tupleExpr)
                        .parallelStream()
                        .map(StatementPattern::getVarList)
                        .flatMap(List::stream)
                        .filter(it -> !it.isConstant())
                        .distinct()
                        .collect(Collectors.toList());
            }
        }

        return Collections.emptyList();
    }

    /**
     * Selects from a list of tuple expressions the next tuple expression that
     * should be evaluated. This method selects the tuple expression with
     * highest number of bound variables, preferring variables that have been
     * bound in other tuple expressions over variables with a fixed value.
     */
    private TupleExpr selectNextTupleExpr(final List<TupleExpr> expressions,
                                          final Map<TupleExpr, Double> cardinalityMap,
                                          final Collection<Var> boundVars) {
        if (boundVars.isEmpty()) {
            return expressions.parallelStream()
                    .min(Comparator.comparingDouble(cardinalityMap::get))
                    .orElse(null);
        } else {
            final List<TupleExpr> filtered = expressions.parallelStream()
                    .filter(it -> !Collections.disjoint(getTupleExprNonConstantVars(it), boundVars))
                    .collect(Collectors.toList());

            if (!filtered.isEmpty()) {
                return filtered.parallelStream()
                        .min(Comparator.comparingDouble(cardinalityMap::get))
                        .orElse(null);
            } else {
                return expressions.parallelStream()
                        .min(Comparator.comparingDouble(cardinalityMap::get))
                        .orElse(null);
            }
        }
    }

}
