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
package org.apache.rya.rdftriplestore;

import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.RdfCloudTripleStoreConstants;
import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.StatementMetadata;
import org.apache.rya.api.persist.RdfEvalStatsDAO;
import org.apache.rya.api.persist.RyaDAO;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.api.persist.joinselect.SelectivityEvalDAO;
import org.apache.rya.api.persist.utils.RyaDAOHelper;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.rdftriplestore.evaluation.ParallelEvaluationStrategyImpl;
import org.apache.rya.rdftriplestore.evaluation.RyaEvaluationStatistics;
import org.apache.rya.rdftriplestore.evaluation.RyaQueryPlanner;
import org.apache.rya.rdftriplestore.inference.InferenceEngine;
import org.apache.rya.rdftriplestore.namespace.NamespaceManager;
import org.apache.rya.rdftriplestore.provenance.ProvenanceCollectionException;
import org.apache.rya.rdftriplestore.provenance.ProvenanceCollector;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.QueryRoot;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.EvaluationStatistics;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;
import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.sail.SailConnectionListener;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.helpers.AbstractSailConnection;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.Map.Entry;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class RdfCloudTripleStoreConnection<C extends RdfCloudTripleStoreConfiguration>
        extends AbstractSailConnection implements NotifyingSailConnection {

    private final RdfCloudTripleStore<C> store;
    private final C conf;
    private final List<SailConnectionListener> sailConnectionListeners;

    private RdfEvalStatsDAO<C> rdfEvalStatsDAO;
    private SelectivityEvalDAO<C> selectEvalDAO;
    private RyaDAO<C> ryaDAO;
    private InferenceEngine inferenceEngine;
    private NamespaceManager namespaceManager;
    private ProvenanceCollector provenanceCollector;

    public RdfCloudTripleStoreConnection(final RdfCloudTripleStore<C> sailBase, final C conf, final ValueFactory vf)
            throws SailException {
        super(sailBase);
        this.store = sailBase;
        this.conf = conf;
        this.sailConnectionListeners = new ArrayList<>();

        initialize();
    }

    protected void initialize() throws SailException {
        refreshConnection();
    }

    protected void refreshConnection() throws SailException {
        try {
            checkNotNull(store.getRyaDAO());
            checkArgument(store.getRyaDAO().isInitialized());
            checkNotNull(store.getNamespaceManager());

            this.ryaDAO = store.getRyaDAO();
            this.rdfEvalStatsDAO = store.getRdfEvalStatsDAO();
            this.selectEvalDAO = store.getSelectEvalDAO();
            this.inferenceEngine = store.getInferenceEngine();
            this.namespaceManager = store.getNamespaceManager();
            this.provenanceCollector = store.getProvenanceCollector();

        } catch (final Exception e) {
            throw new SailException(e);
        }
    }

    @Override
    protected void addStatementInternal(final Resource subject, final IRI predicate,
                                        final Value object, final Resource... contexts) throws SailException {
        try {
            final String cv_s = conf.getCv();
            final byte[] cv = cv_s == null ? null : cv_s.getBytes(StandardCharsets.UTF_8);
            final List<RyaStatement> ryaStatements = new ArrayList<>();
            if (contexts != null && contexts.length > 0) {
                for (final Resource context : contexts) {
                    final RyaStatement statement = new RyaStatement(
                            RdfToRyaConversions.convertResource(subject),
                            RdfToRyaConversions.convertIRI(predicate),
                            RdfToRyaConversions.convertValue(object),
                            RdfToRyaConversions.convertResource(context),
                            null, new StatementMetadata(), cv);

                    ryaStatements.add(statement);
                }
            } else {
                final RyaStatement statement = new RyaStatement(
                        RdfToRyaConversions.convertResource(subject),
                        RdfToRyaConversions.convertIRI(predicate),
                        RdfToRyaConversions.convertValue(object),
                        null, null, new StatementMetadata(), cv);

                ryaStatements.add(statement);
            }
            ryaDAO.add(ryaStatements.iterator());

            notifySailConnectionListenersAboutAddedStatement(subject, predicate, object, contexts);
        } catch (final RyaDAOException e) {
            throw new SailException(e);
        }
    }

    @Override
    protected void clearInternal(final Resource... aresource) throws SailException {
        try {
            final RyaIRI[] graphs = new RyaIRI[aresource.length];
            for (int i = 0; i < graphs.length; i++) {
                graphs[i] = RdfToRyaConversions.convertResource(aresource[i]);
            }
            ryaDAO.dropGraph(conf, graphs);
        } catch (final RyaDAOException e) {
            throw new SailException(e);
        }
    }

    @Override
    protected void clearNamespacesInternal() throws SailException {
        logger.error("Clear Namespace Repository method not implemented");
    }

    @Override
    protected void closeInternal() throws SailException {
        verifyIsOpen();
    }

    @Override
    protected void commitInternal() throws SailException {
        verifyIsOpen();
        //There is no transactional layer
    }

    @Override
    protected CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluateInternal(
            TupleExpr tupleExpr, final Dataset dataset, BindingSet bindings,
            final boolean flag) throws SailException {
        long startWatch = System.nanoTime();

        verifyIsOpen();
        logger.trace("Incoming query model:\n{}", tupleExpr.toString());
        if (provenanceCollector != null) {
            try {
                provenanceCollector.recordQuery(tupleExpr.toString());
            } catch (final ProvenanceCollectionException e) {
                logger.trace("Provenance failed to record query.", e);
            }
        }
        tupleExpr = tupleExpr.clone();

        final C queryConf = (C) store.getConf().clone();
        if (queryConf == null) {
            // Should not happen, but this is better than a null dereference error.
            throw new SailException("Cloning store.getConf() returned null, aborting.");
        }
        if (bindings != null) {
            final Binding dispPlan = bindings.getBinding(RdfCloudTripleStoreConfiguration.CONF_QUERYPLAN_FLAG);
            if (dispPlan != null) {
                queryConf.setDisplayQueryPlan(Boolean.parseBoolean(dispPlan.getValue().stringValue()));
            }

            final Binding authBinding = bindings.getBinding(RdfCloudTripleStoreConfiguration.CONF_QUERY_AUTH);
            if (authBinding != null) {
                queryConf.setAuths(authBinding.getValue().stringValue().split(","));
            }

            final Binding ttlBinding = bindings.getBinding(RdfCloudTripleStoreConfiguration.CONF_TTL);
            if (ttlBinding != null) {
                queryConf.setTtl(Long.valueOf(ttlBinding.getValue().stringValue()));
            }

            final Binding startTimeBinding = bindings.getBinding(RdfCloudTripleStoreConfiguration.CONF_STARTTIME);
            if (startTimeBinding != null) {
                queryConf.setStartTime(Long.valueOf(startTimeBinding.getValue().stringValue()));
            }

            final Binding performantBinding = bindings.getBinding(RdfCloudTripleStoreConfiguration.CONF_PERFORMANT);
            if (performantBinding != null) {
                queryConf.setBoolean(RdfCloudTripleStoreConfiguration.CONF_PERFORMANT, Boolean.parseBoolean(performantBinding.getValue().stringValue()));
            }

            final Binding inferBinding = bindings.getBinding(RdfCloudTripleStoreConfiguration.CONF_INFER);
            if (inferBinding != null) {
                queryConf.setInfer(Boolean.parseBoolean(inferBinding.getValue().stringValue()));
            }

            final Binding useStatsBinding = bindings.getBinding(RdfCloudTripleStoreConfiguration.CONF_USE_STATS);
            if (useStatsBinding != null) {
                queryConf.setUseStats(Boolean.parseBoolean(useStatsBinding.getValue().stringValue()));
            }

            final Binding offsetBinding = bindings.getBinding(RdfCloudTripleStoreConfiguration.CONF_OFFSET);
            if (offsetBinding != null) {
                queryConf.setOffset(Long.parseLong(offsetBinding.getValue().stringValue()));
            }

            final Binding limitBinding = bindings.getBinding(RdfCloudTripleStoreConfiguration.CONF_LIMIT);
            if (limitBinding != null) {
                queryConf.setLimit(Long.parseLong(limitBinding.getValue().stringValue()));
            }
        } else {
            bindings = new QueryBindingSet();
        }

        if (!(tupleExpr instanceof QueryRoot)) {
            tupleExpr = new QueryRoot(tupleExpr);
        }

        try {
            final EvaluationStrategy strategy = new ParallelEvaluationStrategyImpl(
                    new StoreTripleSource<C>(queryConf, ryaDAO), inferenceEngine, dataset, queryConf);
            final EvaluationStatistics statistics = new RyaEvaluationStatistics<C>(queryConf, rdfEvalStatsDAO);

            final RyaQueryPlanner queryPlanner = new RyaQueryPlanner(strategy, statistics);
            queryPlanner.optimize(tupleExpr, dataset, bindings);

            long stopWatch = System.nanoTime();
            logger.debug("Query plan built in {} ms", (stopWatch - startWatch) / 1000000);

            startWatch = System.nanoTime();
            final CloseableIteration<BindingSet, QueryEvaluationException> iter = strategy
                    .evaluate(tupleExpr, EmptyBindingSet.getInstance());

            stopWatch = System.nanoTime();
            logger.debug("BindingSet iterator prepared in {} ms", (stopWatch - startWatch) / 1000000);

            return new CloseableIteration<BindingSet, QueryEvaluationException>() {

                @Override
                public void remove() throws QueryEvaluationException {
                    iter.remove();
                }

                @Override
                public BindingSet next() throws QueryEvaluationException {
                    return iter.next();
                }

                @Override
                public boolean hasNext() throws QueryEvaluationException {
                    return iter.hasNext();
                }

                @Override
                public void close() throws QueryEvaluationException {
                    iter.close();
                    ((ParallelEvaluationStrategyImpl) strategy).shutdown();
                }
            };
        } catch (final Exception e) {
            throw new SailException(e);
        }
    }

    @Override
    protected CloseableIteration<? extends Resource, SailException> getContextIDsInternal()
            throws SailException {
        verifyIsOpen();

        // iterate through all contextids
        return null;
    }

    @Override
    protected String getNamespaceInternal(final String s) throws SailException {
        return namespaceManager.getNamespace(s);
    }

    @Override
    protected CloseableIteration<? extends Namespace, SailException> getNamespacesInternal()
            throws SailException {
        return namespaceManager.iterateNamespace();
    }

    @Override
    protected CloseableIteration<? extends Statement, SailException> getStatementsInternal(
            final Resource subject, final IRI predicate, final Value object, final boolean flag,
            final Resource... contexts) throws SailException {
//        try {
        //have to do this to get the inferred values
        //TODO: Will this method reduce performance?
        final Var subjVar = decorateValue(subject, "s");
        final Var predVar = decorateValue(predicate, "p");
        final Var objVar = decorateValue(object, "o");
        StatementPattern sp = null;
        final boolean hasContext = contexts != null && contexts.length > 0;
        final Resource context = (hasContext) ? contexts[0] : null;
        final Var cntxtVar = decorateValue(context, "c");
        //TODO: Only using one context here
        sp = new StatementPattern(subjVar, predVar, objVar, cntxtVar);
        //return new StoreTripleSource(store.getConf()).getStatements(resource, uri, value, contexts);
        final CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluate = evaluate(sp, null, null, false);
        return new CloseableIteration<Statement, SailException>() {  //TODO: Use a util class to do this
            private boolean isClosed = false;

            @Override
            public void close() throws SailException {
                isClosed = true;
                try {
                    evaluate.close();
                } catch (final QueryEvaluationException e) {
                    throw new SailException(e);
                }
            }

            @Override
            public boolean hasNext() throws SailException {
                try {
                    return evaluate.hasNext();
                } catch (final QueryEvaluationException e) {
                    throw new SailException(e);
                }
            }

            @Override
            public Statement next() throws SailException {
                if (!hasNext() || isClosed) {
                    throw new NoSuchElementException();
                }

                try {
                    final BindingSet next = evaluate.next();
                    final Resource bs_subj = (Resource) ((subjVar.hasValue()) ? subjVar.getValue() : next.getBinding(subjVar.getName()).getValue());
                    final IRI bs_pred = (IRI) ((predVar.hasValue()) ? predVar.getValue() : next.getBinding(predVar.getName()).getValue());
                    final Value bs_obj = (objVar.hasValue()) ? objVar.getValue() :
                            next.getBinding(objVar.getName()).getValue();
                    final Binding b_cntxt = next.getBinding(cntxtVar.getName());

                    //convert BindingSet to Statement
                    if (b_cntxt != null) {
                        return SimpleValueFactory.getInstance().createStatement(bs_subj, bs_pred, bs_obj, (Resource) b_cntxt.getValue());
                    } else {
                        return SimpleValueFactory.getInstance().createStatement(bs_subj, bs_pred, bs_obj);
                    }
                } catch (final QueryEvaluationException e) {
                    throw new SailException(e);
                }
            }

            @Override
            public void remove() throws SailException {
                try {
                    evaluate.remove();
                } catch (final QueryEvaluationException e) {
                    throw new SailException(e);
                }
            }
        };
//        } catch (QueryEvaluationException e) {
//            throw new SailException(e);
//        }
    }

    protected Var decorateValue(final Value val, final String name) {
        if (val == null) {
            return new Var(name);
        } else {
            return new Var(name, val);
        }
    }

    @Override
    protected void removeNamespaceInternal(final String s) throws SailException {
        namespaceManager.removeNamespace(s);
    }

    @Override
    protected void removeStatementsInternal(final Resource subject, final IRI predicate,
                                            final Value object, final Resource... contexts) throws SailException {
        if (!(subject instanceof IRI)) {
            throw new SailException("Subject[" + subject + "] must be URI");
        }

        try {
            if (contexts != null && contexts.length > 0) {
                for (final Resource context : contexts) {
                    if (!(context instanceof IRI)) {
                        throw new SailException("Context[" + context + "] must be URI");
                    }
                    final RyaStatement statement = new RyaStatement(
                            RdfToRyaConversions.convertResource(subject),
                            RdfToRyaConversions.convertIRI(predicate),
                            RdfToRyaConversions.convertValue(object),
                            RdfToRyaConversions.convertResource(context));

                    ryaDAO.delete(statement, conf);
                }
            } else {
                final RyaStatement statement = new RyaStatement(
                        RdfToRyaConversions.convertResource(subject),
                        RdfToRyaConversions.convertIRI(predicate),
                        RdfToRyaConversions.convertValue(object),
                        null);

                ryaDAO.delete(statement, conf);
            }

            notifySailConnectionListenersAboutRemovedStatement(subject, predicate, object, contexts);
        } catch (final RyaDAOException e) {
            throw new SailException(e);
        }
    }

    @Override
    protected void rollbackInternal() throws SailException {
        //TODO: No transactional layer as of yet
    }

    @Override
    protected void setNamespaceInternal(final String s, final String s1)
            throws SailException {
        namespaceManager.addNamespace(s, s1);
    }

    @Override
    protected long sizeInternal(final Resource... contexts) throws SailException {
        logger.error("Cannot determine size as of yet");

        return 0;
    }

    @Override
    protected void startTransactionInternal() throws SailException {
        //TODO: ?
    }

    @Override
    public boolean pendingRemovals() {
        return false;
    }

    public static class StoreTripleSource<C extends RdfCloudTripleStoreConfiguration> implements TripleSource {

        private final C conf;
        private final RyaDAO<C> ryaDAO;

        public StoreTripleSource(final C conf, final RyaDAO<C> ryaDAO) {
            this.conf = conf;
            this.ryaDAO = ryaDAO;
        }

        @Override
        public CloseableIteration<Statement, QueryEvaluationException> getStatements(
                final Resource subject, final IRI predicate, final Value object,
                final Resource... contexts) throws QueryEvaluationException {
            return RyaDAOHelper.query(ryaDAO, subject, predicate, object, conf, contexts);
        }

        public CloseableIteration<? extends Entry<Statement, BindingSet>, QueryEvaluationException> getStatements(
                final Collection<Map.Entry<Statement, BindingSet>> statements,
                final Resource... contexts) throws QueryEvaluationException {

            return RyaDAOHelper.query(ryaDAO, statements, conf);
        }

        @Override
        public ValueFactory getValueFactory() {
            return RdfCloudTripleStoreConstants.VALUE_FACTORY;
        }
    }

    public InferenceEngine getInferenceEngine() {
        return inferenceEngine;
    }

    public C getConf() {
        return conf;
    }

    @Override
    public void addConnectionListener(SailConnectionListener listener) {
        sailConnectionListeners.add(listener);
    }

    @Override
    public void removeConnectionListener(SailConnectionListener listener) {
        sailConnectionListeners.remove(listener);
    }

    private void notifySailConnectionListenersAboutAddedStatement(
            final Resource subject, final IRI predicate, final Value object, final Resource... contexts) {
        final ValueFactory vf = SimpleValueFactory.getInstance();
        if (contexts != null && contexts.length > 0) {
            Arrays.stream(contexts).forEach(context -> sailConnectionListeners.forEach(listener -> {
                listener.statementAdded(vf.createStatement(subject, predicate, object, context));
            }));
        } else {
            sailConnectionListeners.forEach(listener ->
                    listener.statementAdded(vf.createStatement(subject, predicate, object, null)));
        }
    }

    private void notifySailConnectionListenersAboutRemovedStatement(
            final Resource subject, final IRI predicate, final Value object, final Resource... contexts) {
        final ValueFactory vf = SimpleValueFactory.getInstance();
        if (contexts != null && contexts.length > 0) {
            Arrays.stream(contexts).forEach(context -> sailConnectionListeners.forEach(listener -> {
                listener.statementRemoved(vf.createStatement(subject, predicate, object, context));
            }));
        } else {
            sailConnectionListeners.forEach(listener ->
                    listener.statementRemoved(vf.createStatement(subject, predicate, object, null)));
        }
    }
}
