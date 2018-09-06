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
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.rya.api.persist.query.RyaQueryEngine;
import org.apache.rya.api.resolver.impl.RyaIRIResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.rya.api.RdfCloudTripleStoreConstants.DELIM_BYTE;
import static org.apache.rya.api.RdfCloudTripleStoreConstants.DELIM_BYTES;

/**
 * @todo implement {@link org.apache.accumulo.core.iterators.OptionDescriber}.
 */
public class AdjacentSubjectsIterator implements SortedKeyValueIterator<Key, Value> {

    public static final String SUBJECTS = "subjects";
    public static final String PROPERTIES = "properties";
    public static final String PROPERTY_FUNCTION = "propertyFunction";
    public static final String START_PROPERTY = "startProperty";
    public static final String END_PROPERTY = "endProperty";
    public static final String ARRAY_DELIMITER = ",";

    private static final Logger LOG = LoggerFactory.getLogger(AdjacentSubjectsIterator.class);
    private static final int UNDEFINED_INDEX = -1;

    private int logId;
    private List<ByteBuffer> subjects;
    private int currentSubjectIdx = UNDEFINED_INDEX;
    private HashSet<ByteBuffer> properties;
    private RyaQueryEngine.PropertyFunction propertyFunction = RyaQueryEngine.PropertyFunction.INCLUDING;
    private ByteBuffer startProperty;
    private ByteBuffer endProperty;
    private SortedKeyValueIterator<Key, Value> sourceIterator;
    private Range currentRange;
    private Collection<ByteSequence> currentColumnFamilies;
    private boolean currentRangeInclusive;

    public AdjacentSubjectsIterator() {
        this.subjects = new ArrayList<>();
        this.properties = new HashSet<>();
    }

    private AdjacentSubjectsIterator(
            SortedKeyValueIterator<Key, Value> sourceIterator, List<ByteBuffer> subjects,
            int currentSubjectIdx, HashSet<ByteBuffer> properties, RyaQueryEngine.PropertyFunction propertyFunction,
            ByteBuffer startProperty, ByteBuffer endProperty, Range currentRange,
            Collection<ByteSequence> currentColumnFamilies, boolean currentRangeInclusive
    ) {
        this.sourceIterator = sourceIterator;
        this.subjects = subjects;
        this.currentSubjectIdx = currentSubjectIdx;
        this.properties = properties;
        this.propertyFunction = propertyFunction;
        this.startProperty = startProperty;
        this.endProperty = endProperty;
        this.currentRange = currentRange;
        this.currentColumnFamilies = currentColumnFamilies;
        this.currentRangeInclusive = currentRangeInclusive;
    }

    @Override
    public void init(final SortedKeyValueIterator<Key, Value> source, final Map<String, String> options,
                     final IteratorEnvironment env) {
        logId = new Random().nextInt();
        LOG.error("[{}] Options: {}", logId, options);
        sourceIterator = source;

        Preconditions.checkArgument(options.containsKey(SUBJECTS), "Option 'subjects' is required!");

        subjects = Collections.unmodifiableList(
                Arrays.stream(options.get(SUBJECTS).split(ARRAY_DELIMITER))
                        .sorted()
                        .map(it -> ByteBuffer.wrap(it.getBytes(StandardCharsets.UTF_8)))
                        .collect(Collectors.toList()));

        if (options.containsKey(PROPERTY_FUNCTION) && options.containsKey(PROPERTIES)) {
            propertyFunction = RyaQueryEngine.PropertyFunction.valueOf(options.get(PROPERTY_FUNCTION));
            properties = Arrays.stream(options.get(PROPERTIES)
                    .split(ARRAY_DELIMITER))
                    .map(it -> ByteBuffer.wrap(it.getBytes(StandardCharsets.UTF_8)))
                    .collect(Collectors.toCollection(HashSet::new));
        }
        if (options.containsKey(START_PROPERTY)) {
            startProperty = ByteBuffer.wrap(options.get(START_PROPERTY).getBytes(StandardCharsets.UTF_8));
        }
        if (options.containsKey(END_PROPERTY)) {
            endProperty = ByteBuffer.wrap(options.get(END_PROPERTY).getBytes(StandardCharsets.UTF_8));
        }
    }

    @Override
    public boolean hasTop() {
        LOG.error("[{}] hasTop = {}", logId, sourceIterator.hasTop() && currentSubjectIdx != UNDEFINED_INDEX);
        return sourceIterator.hasTop() && currentSubjectIdx != UNDEFINED_INDEX;
    }

    @Override
    public void next() throws IOException {
        LOG.error("[{}] next", logId);
        sourceIterator.next();

        findTop();
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        LOG.error("[{}] seek: {} {} {}", logId, rangeToString(range), columnFamilies, inclusive);
        if (range.isInfiniteStartKey()) {
            // Current subject should be less than the end of range
            if (range.isInfiniteStopKey() ||
                    subjects.get(0).compareTo(ByteBuffer.wrap(range.getEndKey().getRow().getBytes())) < 0) {
                currentSubjectIdx = 0;

                // Never null, because of above conditions
                final Range cutDownRange = cutDownRangeUpToSubject(range, currentSubjectIdx);

                LOG.error("[{}] cut down {}", logId, rangeToString(cutDownRange));
                seekSourceIterator(cutDownRange, columnFamilies, inclusive);
            }
        } else {
            final ByteBuffer startKeySubject = extractSubject(range.getStartKey());
            currentSubjectIdx = getCeilingSubject(currentSubjectIdx, startKeySubject);

            if (currentSubjectIdx != UNDEFINED_INDEX) {
                if (subjects.get(currentSubjectIdx).compareTo(startKeySubject) == 0) {
                    seekSourceIterator(range, columnFamilies, inclusive);
                } else {
                    final Range cutDownRange = cutDownRangeUpToSubject(range, currentSubjectIdx);

                    if (cutDownRange != null) {
                        LOG.error("[{}] cut down {}", logId, rangeToString(cutDownRange));
                        seekSourceIterator(cutDownRange, columnFamilies, inclusive);
                    } else {
                        // There is no KV in range which could satisfy iterator's settings
                        seekSourceIterator(range, columnFamilies, inclusive);

                        currentSubjectIdx = UNDEFINED_INDEX;
                    }
                }
            } else {
                LOG.error("[{}] there's no subject in range", logId);
                // There is no KV in the range which could satisfy iterator's settings
                seekSourceIterator(range, columnFamilies, inclusive);
            }
        }

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
        LOG.error("[{}] deepCopy", logId);
        return new AdjacentSubjectsIterator(
                sourceIterator.deepCopy(env),
                currentSubjectIdx != UNDEFINED_INDEX ?
                        Collections.unmodifiableList(subjects.subList(currentSubjectIdx, subjects.size())) : subjects,
                currentSubjectIdx, properties, propertyFunction,
                startProperty == null ? null : startProperty.duplicate(),
                endProperty == null ? null : endProperty.duplicate(),
                currentRange == null ? null : new Range(currentRange),
                currentColumnFamilies, currentRangeInclusive);
    }

    private void seekSourceIterator(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
            throws IOException {
        currentRange = range;
        currentColumnFamilies = columnFamilies;
        currentRangeInclusive = inclusive;
        sourceIterator.seek(range, columnFamilies, inclusive);
    }

    /**
     * When can't find next top, then currentSubject is null.
     *
     * @throws IOException
     */
    private void findTop() throws IOException {
        while (sourceIterator.hasTop() && currentSubjectIdx != UNDEFINED_INDEX) {
            final Key candidateKey = sourceIterator.getTopKey();

            LOG.error("[{}] candidate {}", logId, candidateKey.toStringNoTruncate());

            final ByteBuffer candidateSubject = extractSubject(candidateKey);
            int compareToCandidateSubject = subjects.get(currentSubjectIdx).compareTo(candidateSubject);

            if (compareToCandidateSubject == 0) {
                if (endProperty == null) {
                    final ByteBuffer candidateProperty = extractProperty(candidateKey);
                    final byte typeMarker = extractObjectType(candidateKey);

                    if (isAllowedPropertyAndObjectType(candidateProperty, typeMarker)) {
                        break;
                    } else {
                        sourceIterator.next();
                    }
                } else {
                    final ByteBuffer candidateProperty = extractProperty(candidateKey);

                    int compareToCandidateProperty = endProperty.compareTo(candidateProperty);
                    if (compareToCandidateProperty == 0) {
                        final byte candidateObjectType = extractObjectType(candidateKey);
                        if (isAllowedObjectType(candidateObjectType)) {
                            break;
                        } else {
                            sourceIterator.next();
                        }
                    } else if (compareToCandidateProperty > 0) {
                        // Candidate property is before the end property
                        final byte candidateObjectType = extractObjectType(candidateKey);
                        if (isAllowedPropertyAndObjectType(candidateProperty, candidateObjectType)) {
                            break;
                        } else {
                            sourceIterator.next();
                        }
                    } else {
                        // Candidate property is after the end property
                        int nextSubjectIdx = getCeilingSubject(currentSubjectIdx + 1, candidateSubject);
                        if (nextSubjectIdx != UNDEFINED_INDEX) {
                            final Range cutDownRange = cutDownRangeUpToSubject(currentRange, nextSubjectIdx);
                            if (cutDownRange != null) {
                                LOG.error("[{}] advance and cut down {}", logId, rangeToString(cutDownRange));
                                seekSourceIterator(cutDownRange, currentColumnFamilies, currentRangeInclusive);

                                currentSubjectIdx = nextSubjectIdx;
                            } else {
                                LOG.error("[{}] cut down out of the range", logId);
                                currentSubjectIdx = UNDEFINED_INDEX;
                            }
                        } else {
                            LOG.error("[{}] there's no subject anymore", logId);
                            currentSubjectIdx = UNDEFINED_INDEX;
                        }
                    }
                }
            } else if (compareToCandidateSubject < 0) {
                int nextSubjectIdx = getCeilingSubject(currentSubjectIdx + 1, candidateSubject);
                if (nextSubjectIdx != UNDEFINED_INDEX) {
                    if (subjects.get(nextSubjectIdx).compareTo(candidateSubject) == 0) {
                        final ByteBuffer candidateProperty = extractProperty(candidateKey);
                        final byte candidateObjectType = extractObjectType(candidateKey);

                        if (isAllowedPropertyAndObjectType(candidateProperty, candidateObjectType)) {
                            break;
                        } else {
                            sourceIterator.next();
                        }
                    } else {
                        final Range cutDownRange = cutDownRangeUpToSubject(currentRange, nextSubjectIdx);
                        if (cutDownRange != null) {
                            LOG.error("[{}] advance and cut down {}", logId, rangeToString(cutDownRange));
                            seekSourceIterator(cutDownRange, currentColumnFamilies, currentRangeInclusive);

                            currentSubjectIdx = nextSubjectIdx;
                        } else {
                            LOG.error("[{}] cut down out of the range", logId);
                            currentSubjectIdx = UNDEFINED_INDEX;
                        }
                    }
                } else {
                    LOG.error("[{}] there's no subject anymore", logId);
                    currentSubjectIdx = UNDEFINED_INDEX;
                }
            } else {
                sourceIterator.next();
            }
        }
    }

    private int getCeilingSubject(final int fromIndex, final ByteBuffer subject) {
        for (int index = fromIndex != UNDEFINED_INDEX ? fromIndex : 0; index < subjects.size(); index++) {
            ByteBuffer nextSubject = subjects.get(index);
            if (nextSubject.compareTo(subject) > -1) {
                return index;
            }
        }

        return UNDEFINED_INDEX;
    }

    private boolean isAllowedPropertyAndObjectType(final ByteBuffer property, final byte objectTypeMarker) {
        boolean allowed;
        if (propertyFunction == RyaQueryEngine.PropertyFunction.INCLUDING) {
            allowed = properties.isEmpty() || properties.contains(property);
        } else {
            allowed = properties.isEmpty() || !properties.contains(property);
        }

        return allowed && isAllowedObjectType(objectTypeMarker);
    }

    private boolean isAllowedObjectType(final byte objectTypeMarker) {
        return objectTypeMarker == RyaIRIResolver.URI_MARKER;
    }

    private Range cutDownRangeUpToSubject(final Range original, final int subjectIdx) {
        Key startKey;
        if (startProperty != null) {
            startKey = new Key(Bytes.concat(subjects.get(subjectIdx).array(),
                    DELIM_BYTES, startProperty.array(), DELIM_BYTES));
        } else {
            startKey = new Key(Bytes.concat(subjects.get(subjectIdx).array(), DELIM_BYTES));
        }

        if (original.isInfiniteStopKey() || startKey.compareTo(original.getEndKey()) < 0) {
            return new Range(startKey, true, original.getEndKey(), original.isEndKeyInclusive());
        } else {
            return null;
        }
    }

    private ByteBuffer extractSubject(Key key) {
        byte[] row = key.getRow().getBytes();
        final int indexFrom = Bytes.indexOf(row, DELIM_BYTE);

        return ByteBuffer.wrap(row, 0, indexFrom).asReadOnlyBuffer();
    }

    private ByteBuffer extractProperty(Key key) {
        final byte[] row = key.getRow().getBytes();

        final int indexFrom = Bytes.indexOf(row, DELIM_BYTE) + 1;
        final int indexTo = Bytes.lastIndexOf(row, DELIM_BYTE) - indexFrom;

        return ByteBuffer.wrap(row, indexFrom, indexTo).asReadOnlyBuffer();
    }

    private byte extractObjectType(Key key) {
        final byte[] row = key.getRow().getBytes();

        return row[row.length - 1];
    }

    private String rangeToString(Range range) {
        return ((range.isStartKeyInclusive() && range.getStartKey() != null) ? "[" : "(") +
                (range.getStartKey() == null ? "-inf" : range.getStartKey().toStringNoTruncate())
                + "," + (range.getEndKey() == null ? "+inf" : range.getEndKey().toStringNoTruncate()) +
                ((range.isEndKeyInclusive() && range.getEndKey() != null) ? "]" : ")");
    }
}
