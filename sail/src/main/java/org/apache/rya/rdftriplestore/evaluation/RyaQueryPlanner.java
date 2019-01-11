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
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.*;

public class RyaQueryPlanner implements QueryOptimizer {

    private final EvaluationStrategy strategy;
    private final EvaluationStatistics statistics;

    public RyaQueryPlanner(final EvaluationStrategy strategy, final EvaluationStatistics statistics) {
        this.strategy = strategy;
        this.statistics = statistics;
    }

    @Override
    public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
        (new BindingAssigner()).optimize(tupleExpr, dataset, bindings);
        (new ConstantOptimizer(strategy)).optimize(tupleExpr, dataset,
                bindings);
        (new CompareOptimizer()).optimize(tupleExpr, dataset, bindings);
        (new ConjunctiveConstraintSplitter()).optimize(tupleExpr, dataset,
                bindings);
        (new DisjunctiveConstraintOptimizer()).optimize(tupleExpr, dataset,
                bindings);
        (new SameTermFilterOptimizer()).optimize(tupleExpr, dataset,
                bindings);
        (new QueryModelNormalizer()).optimize(tupleExpr, dataset, bindings);
        (new IterativeEvaluationOptimizer()).optimize(tupleExpr, dataset,
                bindings);
        (new FilterOptimizer()).optimize(tupleExpr, dataset, bindings);
        (new OrderLimitOptimizer()).optimize(tupleExpr, dataset, bindings);

        (new PushBindingSetAssignmentUnderExtensionOptimizer()).optimize(tupleExpr, dataset, bindings);

        (new QueryJoinOptimizer(statistics)).optimize(tupleExpr, dataset, bindings);
    }
}
