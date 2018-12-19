package org.apache.rya.rdftriplestore.evaluation;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.QueryJoinOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.*;

public class StaticQueryPlanner implements QueryOptimizer {

    private final EvaluationStrategy strategy;
    private final EvaluationStatistics statistics;

    public StaticQueryPlanner(final EvaluationStrategy strategy, final EvaluationStatistics statistics) {
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
        (new QueryJoinOptimizer(statistics)).optimize(tupleExpr, dataset, bindings);
//        (new ConjunctiveJoinOptimizer(this.statistics)).optimize(tupleExpr, dataset, bindings);
    }
}
