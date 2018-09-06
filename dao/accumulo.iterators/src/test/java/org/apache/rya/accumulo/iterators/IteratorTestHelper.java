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

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.rya.api.RdfCloudTripleStoreConstants;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.resolver.triple.TripleRow;
import org.apache.rya.api.resolver.triple.TripleRowResolver;
import org.apache.rya.api.resolver.triple.TripleRowResolverException;

import java.util.Map;

public class IteratorTestHelper {

    public static void put(Map<Key, Value> data, final TripleRowResolver resolver, final RyaStatement stmt)
            throws TripleRowResolverException {
        TripleRow tr = resolver.serialize(stmt).get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.SPO);

        Value value = (tr.getValue() == null) ? new Value() : new Value(tr.getValue());

        data.put(new Key(tr.getRow(), tr.getColumnFamily(), tr.getColumnQualifier()), value);
    }

}
