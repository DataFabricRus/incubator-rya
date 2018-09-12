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

import com.google.common.base.Preconditions;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.persist.RyaDAO;
import org.apache.rya.api.persist.RyaDAOException;
import org.calrissian.mango.collect.CloseableIterable;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.jetbrains.annotations.NotNull;
import org.mapdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

public class GraphMiningQueryEngine<C extends RdfCloudTripleStoreConfiguration> implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(GraphMiningQueryEngine.class);

    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final RyaDAO<C> dao;
    private final DB database;

    GraphMiningQueryEngine(final RyaDAO<C> dao) {
        this.dao = dao;
        this.database = DBMaker.tempFileDB()
                .fileMmapEnable()
                .concurrencyDisable()
                .closeOnJvmShutdown()
                .make();
    }

    public Iterable<BindingSet> evaluate(final GraphMiningQuery query)
            throws ExecutionException, InterruptedException {
        Future<Iterable<BindingSet>> future = executor.submit(new GraphMiningQueryCallable(query));

        return future.get();
    }

    @Override
    public void close() throws IOException {
        try {
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
            executor.shutdownNow();
        } catch (InterruptedException ex) {
            throw new IOException(ex);
        } finally {
            database.close();
        }
    }

    private class GraphMiningQueryCallable implements Callable<Iterable<BindingSet>>, Closeable {

        private final ValueFactory VF = SimpleValueFactory.getInstance();
        private final GraphMiningQuery query;
        private final List<RyaIRI> frontier;
        private final HTreeMap<String, VertexState> visitedVertices;

        GraphMiningQueryCallable(final GraphMiningQuery query) {
            this.query = query;
            this.frontier = new ArrayList<>();
            this.visitedVertices = database.hashMap("visitedVertices-" + System.currentTimeMillis())
                    .keySerializer(Serializer.STRING)
                    .valueSerializer(new VertexStateSerializer())
                    .create();
        }

        @Override
        public Iterable<BindingSet> call() throws Exception {
            try {
                long start = System.currentTimeMillis();

                boolean reachedTarget = searchForTargetVertices();

                LOG.debug("Search for relations took {} ms", System.currentTimeMillis() - start);

                if (reachedTarget) {
                    start = System.currentTimeMillis();
                    try {
                        return extractResultantTree();
                    } finally {
                        LOG.debug("Extraction of the resultant tree took {} ms", System.currentTimeMillis() - start);
                    }
                } else {
                    return Collections.emptyList();
                }
            } finally {
                close();
            }
        }

        @Override
        public void close() throws IOException {
            visitedVertices.close();
        }

        private boolean searchForTargetVertices() throws RyaDAOException {
            boolean reachedTarget = false;
            long currentDepth = 0;
            long numberVisited = 0;

            frontier.add(query.getSource());
            visitedVertices.put(query.getSource().getData(), new VertexState(currentDepth));

            while (!frontier.isEmpty() && numberVisited < query.getMaxVisited()) {
                currentDepth++;

                long start = System.currentTimeMillis();
                CloseableIterable<RyaStatement> adjacentVertices = dao.getQueryEngine().queryAdjacentSubjects(
                        frontier, query.getPropertyFunction(), query.getProperties(), query.getMaxNumThreads());

                LOG.debug("Depth #{}. Search took {} ms", currentDepth, System.currentTimeMillis() - start);

                frontier.clear();

                start = System.currentTimeMillis();
                for (RyaStatement statement : adjacentVertices) {
                    RyaIRI adjacentVertex = (RyaIRI) statement.getObject();

                    boolean alreadyVisited = visitedVertices.containsKey(adjacentVertex.getData());

                    if (!alreadyVisited) {
                        frontier.add(adjacentVertex);

                        visitedVertices.put(
                                adjacentVertex.getData(),
                                new VertexState(currentDepth, statement.getSubject(), statement.getPredicate())
                        );
                    }

                    numberVisited++;
                }

                LOG.debug("Depth #{}. In total, visited {} vertices. The check took {} ms",
                        currentDepth, numberVisited, System.currentTimeMillis() - start);

                if (visitedVertices.containsKey(query.getTarget().getData())) {
                    LOG.debug("Found target! Visited {} vertices.", numberVisited);

                    reachedTarget = true;
                    break;
                }
            }

            if (numberVisited >= query.getMaxVisited()) {
                LOG.info("Exceeded the max number of visited vertices - [{}]", query.getMaxVisited());
            }

            return reachedTarget;
        }

        private Iterable<BindingSet> extractResultantTree() {
            List<BindingSet> bindingSets = new ArrayList<>();

            String currentVertex = query.getTarget().getData();
            long currentDepth = Long.MAX_VALUE;
            while (!currentVertex.equals(query.getSource().getData())) {
                final MapBindingSet bindingSet = new MapBindingSet();

                VertexState vertexState = visitedVertices.get(currentVertex);

                Preconditions.checkArgument(currentDepth > vertexState.depth, "Depth should always decrease!");

                bindingSet.addBinding(query.getMedium().getName(), VF.createIRI(currentVertex));
                bindingSet.addBinding(query.getDepth().getName(), VF.createLiteral(vertexState.depth));
                bindingSet.addBinding(query.getPredecessor().getName(), VF.createIRI(vertexState.predecessor));
                bindingSet.addBinding(query.getLinkingProperty().getName(), VF.createIRI(vertexState.linkingProperty));

                bindingSets.add(bindingSet);

                currentVertex = vertexState.predecessor;
                currentDepth = vertexState.depth;
            }

            return bindingSets;
        }
    }

    private class VertexState {

        long depth;
        String predecessor;
        String linkingProperty;

        VertexState(final long depth, final String predecessor, final String linkingProperty) {
            this.depth = depth;
            this.predecessor = predecessor;
            this.linkingProperty = linkingProperty;
        }

        VertexState(final long depth, final RyaIRI predecessor, final RyaIRI linkingProperty) {
            this(
                    depth,
                    predecessor == null ? null : predecessor.getData(),
                    linkingProperty == null ? null : linkingProperty.getData()
            );
        }

        VertexState(final long depth) {
            this(depth, (String) null, null);
        }

        @Override
        public String toString() {
            return "Vertex State {" +
                    "depth=" + depth + ";" +
                    "predecessor=" + predecessor + ";" +
                    "linkingProperty=" + linkingProperty + "}";
        }
    }

    private class VertexStateSerializer implements Serializer<VertexState> {

        @Override
        public void serialize(@NotNull DataOutput2 out, @NotNull VertexState value) throws IOException {
            out.writeLong(value.depth);
            writeNullableUTF(out, value.predecessor);
            writeNullableUTF(out, value.linkingProperty);
        }

        @Override
        public VertexState deserialize(@NotNull DataInput2 input, int available) throws IOException {
            long depth = input.readLong();
            String predecessor = readNullableUTF(input);
            String linkingProperty = readNullableUTF(input);

            return new VertexState(depth, predecessor, linkingProperty);
        }

        private void writeNullableUTF(DataOutput2 out, String value) throws IOException {
            if (value == null) {
                out.writeBoolean(true);
            } else {
                out.writeBoolean(false);
                out.writeUTF(value);
            }
        }

        private String readNullableUTF(DataInput2 input) throws IOException {
            return input.readBoolean() ? null : input.readUTF();
        }
    }
}
