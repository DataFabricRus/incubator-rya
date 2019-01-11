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

import com.google.common.base.Preconditions;
import com.google.common.primitives.Bytes;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.rya.api.RdfCloudTripleStoreConstants.DELIM_BYTE;

public class RDFPropertyFilter extends Filter {

    private static final Logger LOG = LoggerFactory.getLogger(RDFPropertyFilter.class);

    public static final String OPTION_PROPERTIES = "properties";
    public static final String DELIMITER = ",";

    private HashSet<ByteBuffer> properties;

    public RDFPropertyFilter() {
        super();
    }

    private RDFPropertyFilter(final SortedKeyValueIterator<Key, Value> source, final HashSet<ByteBuffer> properties) {
        super();
        setSource(source);

        this.properties = properties;
    }

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env)
            throws IOException {
        super.init(source, options, env);

        Preconditions.checkArgument(options.containsKey(OPTION_PROPERTIES));

        properties = Arrays.stream(options.get(OPTION_PROPERTIES).split(DELIMITER))
                .map(it -> ByteBuffer.wrap(it.getBytes(StandardCharsets.UTF_8)))
                .collect(Collectors.toCollection(HashSet::new));
    }

    @Override
    @SuppressWarnings("unchecked")
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        return new RDFPropertyFilter(getSource().deepCopy(env), (HashSet<ByteBuffer>) properties.clone());
    }

    @Override
    public boolean accept(Key key, Value value) {
        try {
            if (key.isDeleted()) {
                return false;
            }

            ByteBuffer candidateProperty = extractProperty(key);

            return properties.contains(candidateProperty);
        } catch (IndexOutOfBoundsException __) {
            LOG.warn("Found a wrong key! Key: {}", key.toStringNoTruncate());

            return false;
        } catch (Exception ex) {
            LOG.error("Failed to filter {}", key.toStringNoTruncate());

            throw ex;
        }
    }

    @Override
    public IteratorOptions describeOptions() {
        IteratorOptions options = super.describeOptions();
        options.addNamedOption(OPTION_PROPERTIES, "properties to accept");

        return options;
    }

    @Override
    public boolean validateOptions(Map<String, String> options) {
        return super.validateOptions(options) && options.containsKey(OPTION_PROPERTIES);
    }

    private ByteBuffer extractProperty(Key key) {
        byte[] row = key.getRow().getBytes();
        final int indexFrom = Bytes.indexOf(row, DELIM_BYTE) + 1;
        final int indexTo = Bytes.lastIndexOf(row, DELIM_BYTE) - indexFrom;

        return ByteBuffer.wrap(row, indexFrom, indexTo).asReadOnlyBuffer();
    }

}
