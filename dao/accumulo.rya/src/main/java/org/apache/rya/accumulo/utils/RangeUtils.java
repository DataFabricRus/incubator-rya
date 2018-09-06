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
package org.apache.rya.accumulo.utils;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Bytes;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.api.domain.RyaType;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.rya.api.RdfCloudTripleStoreConstants.DELIM_BYTES;
import static org.apache.rya.api.RdfCloudTripleStoreConstants.LAST_BYTES;

public class RangeUtils {

    public static Range createRange(final RyaIRI fromSubject, final RyaIRI toSubject) {
        return createRange(fromSubject, toSubject, null, null);
    }

    /**
     * @param fromSubject
     * @param toSubject
     * @param fromProperty
     * @param toProperty
     * @return
     */
    public static Range createRange(
            final RyaIRI fromSubject, final RyaIRI toSubject, final RyaIRI fromProperty, final RyaIRI toProperty) {
        final byte[] startKey;
        final byte[] stopKey;

        if (fromSubject != null) {
            if (fromProperty != null) {
                startKey = Bytes.concat(
                        fromSubject.getData().getBytes(StandardCharsets.UTF_8),
                        DELIM_BYTES,
                        fromProperty.getData().getBytes(StandardCharsets.UTF_8),
                        DELIM_BYTES);
            } else {
                startKey = Bytes.concat(fromSubject.getData().getBytes(StandardCharsets.UTF_8), DELIM_BYTES);
            }
        } else {
            startKey = null;
        }

        if (toSubject != null) {
            if (toProperty != null) {
                stopKey = Bytes.concat(
                        toSubject.getData().getBytes(StandardCharsets.UTF_8),
                        DELIM_BYTES,
                        toProperty.getData().getBytes(StandardCharsets.UTF_8),
                        DELIM_BYTES,
                        LAST_BYTES);
            } else {
                stopKey = Bytes.concat(toSubject.getData().getBytes(StandardCharsets.UTF_8), LAST_BYTES);
            }
        } else {
            if (fromSubject != null && toProperty != null) {
                stopKey = Bytes.concat(
                        fromSubject.getData().getBytes(StandardCharsets.UTF_8),
                        DELIM_BYTES,
                        toProperty.getData().getBytes(StandardCharsets.UTF_8),
                        DELIM_BYTES,
                        LAST_BYTES);
            } else {
                stopKey = null;
            }
        }

        return new Range(startKey == null ? null : new Key(startKey), stopKey == null ? null : new Key(stopKey));
    }

    /**
     * @param subjects must be sorted by ascending
     * @return
     */
    public static Range createRange(final List<RyaIRI> subjects, final RyaIRI fromProperty, final RyaIRI toProperty) {
        Preconditions.checkArgument(!subjects.isEmpty());

        if (subjects.size() > 1) {
            return createRange(subjects.get(0), subjects.get(subjects.size() - 1), fromProperty, toProperty);
        } else {
            return createRange(subjects.get(0), null, fromProperty, toProperty);
        }
    }

    public static Collection<Range> createRanges(
            final Collection<RyaIRI> iris, final RyaIRI startProperty, final RyaIRI endProperty, int numberOfGroups) {

        final List<RyaIRI> subjects = iris.parallelStream()
                .sorted(Comparator.comparing(RyaType::getData))
                .collect(Collectors.toList());

        final List<Range> results = new ArrayList<>();
        final int groupSize;
        if (subjects.size() <= numberOfGroups) {
            groupSize = 1;
            numberOfGroups = subjects.size();
        } else {
            groupSize = (subjects.size() / numberOfGroups) + 1;
        }

        for (int groupNum = 0; groupNum < numberOfGroups; groupNum++) {
            int fromIndex = groupNum * groupSize;
            if (fromIndex > subjects.size() - 1) {
                break;
            }

            int toIndex = (groupNum + 1) * groupSize;
            List<RyaIRI> group = subjects.subList(fromIndex, toIndex > subjects.size() ? subjects.size() : toIndex);

            Range groupRange = RangeUtils.createRange(group, startProperty, endProperty);

            results.add(groupRange);
        }

        return results;
    }

}
