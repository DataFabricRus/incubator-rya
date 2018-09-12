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
import com.google.common.collect.Sets;
import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.api.persist.query.RyaQueryEngine;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleLiteral;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.algebra.Var;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

class GraphMiningQuery {

    private static final ValueFactory VF = SimpleValueFactory.getInstance();
    public static final String NAMESPACE = "http://w3id.org/datafabric.cc/ontologies/graphmining#";
    public static final IRI PREDICATE_SEARCH = VF.createIRI(NAMESPACE, "search");
    public static final IRI PREDICATE_SOURCE = VF.createIRI(NAMESPACE, "source");
    public static final IRI PREDICATE_TARGET = VF.createIRI(NAMESPACE, "target");
    public static final IRI PREDICATE_MEDIUM = VF.createIRI(NAMESPACE, "medium");
    public static final IRI PREDICATE_DEPTH = VF.createIRI(NAMESPACE, "depth");
    public static final IRI PREDICATE_PREDECESSOR = VF.createIRI(NAMESPACE, "predecessor");
    public static final IRI PREDICATE_INCLUDING_PROPERTY = VF.createIRI(NAMESPACE, "includingProperty");
    public static final IRI PREDICATE_EXCLUDING_PROPERTY = VF.createIRI(NAMESPACE, "excludingProperty");
    public static final IRI PREDICATE_LINKING_PROPERTY = VF.createIRI(NAMESPACE, "linkingProperty");
    public static final IRI PREDICATE_MAX_VISITED = VF.createIRI(NAMESPACE, "maxVisited");
    public static final IRI PREDICATE_NUM_THREADS = VF.createIRI(NAMESPACE, "maxNumThreads");

    public static final List<IRI> PREDICATE_PARAMETERS = Arrays.asList(
            PREDICATE_TARGET, PREDICATE_MAX_VISITED, PREDICATE_INCLUDING_PROPERTY, PREDICATE_MEDIUM, PREDICATE_DEPTH,
            PREDICATE_PREDECESSOR, PREDICATE_EXCLUDING_PROPERTY, PREDICATE_LINKING_PROPERTY
    );

    private RyaIRI source;
    private RyaIRI target;
    private Var medium;
    private Var depth;
    private Var predecessor;
    private Var linkingProperty;
    private RyaQueryEngine.PropertyFunction propertyFunction;
    private List<RyaIRI> properties;
    private long maxVisited = 10000;
    private int maxNumThreads = 1;

    GraphMiningQuery() {
        this.properties = new ArrayList<>();
    }

    RyaIRI getSource() {
        return source;
    }

    RyaIRI getTarget() {
        return target;
    }

    RyaQueryEngine.PropertyFunction getPropertyFunction() {
        return propertyFunction;
    }

    List<RyaIRI> getProperties() {
        return properties;
    }

    long getMaxVisited() {
        return maxVisited;
    }

    int getMaxNumThreads() {
        return maxNumThreads;
    }

    Set<String> getBindingNames() {
        return Sets.newHashSet(medium.getName(), depth.getName(), predecessor.getName(), linkingProperty.getName());
    }

    Var getMedium() {
        return medium;
    }

    Var getDepth() {
        return depth;
    }

    Var getPredecessor() {
        return predecessor;
    }

    Var getLinkingProperty() {
        return linkingProperty;
    }

    void setParameter(IRI parameter, Value value) {
        if (parameter.equals(PREDICATE_SOURCE)) {
            source = new RyaIRI(value.stringValue());
            return;
        }
        if (parameter.equals(PREDICATE_TARGET)) {
            target = new RyaIRI(value.stringValue());
            return;
        }
        if (parameter.equals(PREDICATE_INCLUDING_PROPERTY)) {
            Preconditions.checkArgument(
                    propertyFunction == null | propertyFunction == RyaQueryEngine.PropertyFunction.INCLUDING,
                    "Can't set both including and excluding properties!");

            properties.add(new RyaIRI(value.stringValue()));
            propertyFunction = RyaQueryEngine.PropertyFunction.INCLUDING;
            return;
        }
        if (parameter.equals(PREDICATE_EXCLUDING_PROPERTY)) {
            Preconditions.checkArgument(
                    propertyFunction == null | propertyFunction == RyaQueryEngine.PropertyFunction.EXCLUDING,
                    "Can't set both including and excluding properties!");

            properties.add(new RyaIRI(value.stringValue()));
            propertyFunction = RyaQueryEngine.PropertyFunction.EXCLUDING;
            return;
        }
        if (parameter.equals(PREDICATE_MAX_VISITED)) {
            if (value instanceof SimpleLiteral) {
                maxVisited = ((SimpleLiteral) value).longValue();
                return;
            }
        }
        if (parameter.equals(PREDICATE_NUM_THREADS)) {
            if (value instanceof SimpleLiteral) {
                maxNumThreads = ((SimpleLiteral) value).intValue();
                return;
            }
        }

        throw new IllegalArgumentException("Parameter " + parameter + " with " + value + " value isn't supported!");
    }

    void setParameter(IRI parameter, Var value) {
        if (parameter.equals(PREDICATE_MEDIUM)) {
            medium = value;
            return;
        }
        if (parameter.equals(PREDICATE_DEPTH)) {
            depth = value;
            return;
        }
        if (parameter.equals(PREDICATE_PREDECESSOR)) {
            predecessor = value;
            return;
        }
        if (parameter.equals(PREDICATE_LINKING_PROPERTY)) {
            linkingProperty = value;
            return;
        }

        throw new IllegalArgumentException("Parameter " + parameter + " with " + value + " value isn't supported!");
    }

    void validate() throws Exception {
        Preconditions.checkNotNull(source);
        Preconditions.checkNotNull(target);
        Preconditions.checkNotNull(medium);
        Preconditions.checkNotNull(depth);
        Preconditions.checkNotNull(predecessor);
        Preconditions.checkArgument(maxVisited > 0);
    }
}
