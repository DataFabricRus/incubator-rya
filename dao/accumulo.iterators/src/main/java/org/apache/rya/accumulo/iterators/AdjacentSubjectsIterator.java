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
package org.apache.rya.accumulo.iterators;

import com.google.common.primitives.Bytes;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.rya.api.persist.query.RyaQueryEngine;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.rya.api.RdfCloudTripleStoreConstants.DELIM_BYTE;
import static org.apache.rya.api.resolver.impl.RyaIRIResolver.URI_MARKER;

public class AdjacentSubjectsIterator implements SortedKeyValueIterator<Key, Value> {

    public static final String PROPERTIES = "properties";
    public static final String PROPERTY_FUNCTION = "propertyFunction";
    public static final String ARRAY_DELIMITER = ",";

    private Set<ByteBuffer> properties;
    private RyaQueryEngine.PropertyFunction propertyFunction = RyaQueryEngine.PropertyFunction.INCLUDING;
    private SortedKeyValueIterator<Key, Value> sourceIterator;
    private boolean hasTop = false;

    public AdjacentSubjectsIterator() {
        this.properties = Collections.emptySet();
    }

    private AdjacentSubjectsIterator(
            final SortedKeyValueIterator<Key, Value> sourceIterator, final Set<ByteBuffer> properties,
            final RyaQueryEngine.PropertyFunction propertyFunction, final boolean hasTop
    ) {
        this.sourceIterator = sourceIterator;
        this.properties = properties;
        this.propertyFunction = propertyFunction;
        this.hasTop = hasTop;
    }

    @Override
    public void init(final SortedKeyValueIterator<Key, Value> source, final Map<String, String> options,
                     final IteratorEnvironment env) {
        sourceIterator = source;

        if (options.containsKey(PROPERTY_FUNCTION) && options.containsKey(PROPERTIES)) {
            propertyFunction = RyaQueryEngine.PropertyFunction.valueOf(options.get(PROPERTY_FUNCTION));
            properties = Arrays.stream(options.get(PROPERTIES)
                    .split(ARRAY_DELIMITER))
                    .map(it -> ByteBuffer.wrap(it.getBytes(StandardCharsets.UTF_8)).asReadOnlyBuffer())
                    .collect(Collectors.toCollection(HashSet::new));
        }
    }

    @Override
    public boolean hasTop() {
        return sourceIterator.hasTop() && hasTop;
    }

    @Override
    public void next() throws IOException {
        sourceIterator.next();

        findTop();
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        sourceIterator.seek(range, columnFamilies, inclusive);

        findTop();
    }

    @Override
    public Key getTopKey() {
        return sourceIterator.getTopKey();
    }

    @Override
    public Value getTopValue() {
        return sourceIterator.getTopValue();
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        return new AdjacentSubjectsIterator(
                sourceIterator.deepCopy(env), new HashSet<>(properties), propertyFunction, hasTop);
    }

    private void findTop() throws IOException {
        hasTop = false;
        while (sourceIterator.hasTop()) {
            final Key candidateKey = sourceIterator.getTopKey();
            final ByteBuffer candidateProperty = extractProperty(candidateKey);
            final byte objectType = candidateObjectType(candidateKey);

            if (isAllowedPropertyAndObjectType(candidateProperty, objectType)) {
                hasTop = true;
                break;
            }

            // Called only on `hasTop == false`.
            sourceIterator.next();
        }
    }

    private boolean isAllowedPropertyAndObjectType(final ByteBuffer property, final byte objectType) {
        final boolean allowed;
        if (propertyFunction == RyaQueryEngine.PropertyFunction.INCLUDING) {
            allowed = properties.isEmpty() || properties.contains(property);
        } else {
            allowed = properties.isEmpty() || !properties.contains(property);
        }

        return allowed && objectType == URI_MARKER;
    }

    private ByteBuffer extractProperty(Key key) {
        byte[] row = key.getRow().getBytes();
        final int indexFrom = Bytes.indexOf(row, DELIM_BYTE) + 1;
        final int indexTo = Bytes.lastIndexOf(row, DELIM_BYTE) - indexFrom;

        return ByteBuffer.wrap(row, indexFrom, indexTo).asReadOnlyBuffer();
    }

    private byte candidateObjectType(Key key) {
        byte[] row = key.getRow().getBytes();

        return row[row.length - 1];
    }
}
