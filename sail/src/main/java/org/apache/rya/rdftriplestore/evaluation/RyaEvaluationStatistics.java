package org.apache.rya.rdftriplestore.evaluation;

import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.persist.RdfEvalStatsDAO;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.EvaluationStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class RyaEvaluationStatistics<C extends RdfCloudTripleStoreConfiguration> extends EvaluationStatistics {

    private static final Logger LOG = LoggerFactory.getLogger(RyaEvaluationStatistics.class);

    private final C conf;
    private final RdfEvalStatsDAO<C> statsDao;

    public RyaEvaluationStatistics(final C conf, final RdfEvalStatsDAO<C> statsDao) {
        this.conf = conf;
        this.statsDao = statsDao;
    }

    @Override
    protected CardinalityCalculator createCardinalityCalculator() {
        return new RyaCardinalityCalculator();
    }

    private class RyaCardinalityCalculator extends CardinalityCalculator {

        @Override
        protected double getCardinality(StatementPattern sp) {
            final Resource subj = (Resource) getConstantValue(sp.getSubjectVar());
            final IRI pred = (IRI) getConstantValue(sp.getPredicateVar());
            final Value obj = getConstantValue(sp.getObjectVar());
            final Resource context = (Resource) getConstantValue(sp.getContextVar());

            final List<Value> idxValue = new ArrayList<>();
            RdfEvalStatsDAO.CARDINALITY_OF card = null;
            if (subj != null) {
                idxValue.add(subj);
                card = RdfEvalStatsDAO.CARDINALITY_OF.SUBJECT;
            }
            if (pred != null) {
                idxValue.add(pred);
                if (card == RdfEvalStatsDAO.CARDINALITY_OF.SUBJECT) {
                    card = RdfEvalStatsDAO.CARDINALITY_OF.SUBJECTPREDICATE;
                } else {
                    card = RdfEvalStatsDAO.CARDINALITY_OF.PREDICATE;
                }
            }

            double cardinality = Double.MAX_VALUE;
            if (card == RdfEvalStatsDAO.CARDINALITY_OF.SUBJECTPREDICATE && obj != null) {
                // All S,P and O are known already
                cardinality = 1.0;
            } else {
                if (obj != null) {
                    idxValue.add(obj);

                    if (card == RdfEvalStatsDAO.CARDINALITY_OF.SUBJECT) {
                        card = RdfEvalStatsDAO.CARDINALITY_OF.SUBJECTOBJECT;
                    } else {
                        card = RdfEvalStatsDAO.CARDINALITY_OF.PREDICATEOBJECT;
                    }
                }

                if (card != null) {
                    cardinality = statsDao.getCardinality(conf, card, idxValue, context);
                }
            }

            LOG.debug("Cardinality of {} is {}", idxValue, cardinality);

            return cardinality;
        }

        private Value getConstantValue(final Var var) {
            return var != null ? var.getValue() : null;
        }
    }
}
