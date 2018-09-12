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
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.iteratortest.IteratorTestCaseFinder;
import org.apache.accumulo.iteratortest.IteratorTestInput;
import org.apache.accumulo.iteratortest.IteratorTestOutput;
import org.apache.accumulo.iteratortest.junit4.BaseJUnit4IteratorTest;
import org.apache.accumulo.iteratortest.testcases.IteratorTestCase;
import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.persist.query.RyaQueryEngine;
import org.apache.rya.api.resolver.triple.TripleRowResolverException;
import org.apache.rya.api.resolver.triple.impl.WholeRowTripleResolver;
import org.junit.runners.Parameterized;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.rya.accumulo.iterators.IteratorTestHelper.put;

public class RDFPropertyFilterTest extends BaseJUnit4IteratorTest {

    public RDFPropertyFilterTest(IteratorTestInput input, IteratorTestOutput expectedOutput, IteratorTestCase testCase) {
        super(input, expectedOutput, testCase);
    }

    @Parameterized.Parameters
    public static Object[][] parameters() throws Exception {
        final IteratorTestInput input = createIteratorInput();
        final IteratorTestOutput output = createIteratorOutput();
        final List<IteratorTestCase> tests = IteratorTestCaseFinder.findAllTestCases();
        return BaseJUnit4IteratorTest.createParameters(input, output, tests);
    }

    private static TreeMap<Key, Value> createInputData() throws TripleRowResolverException {
        TreeMap<Key, Value> data = new TreeMap<>();
        WholeRowTripleResolver resolver = new WholeRowTripleResolver();
        put(data, resolver, new RyaStatement(
                new RyaIRI("urn:a1"), new RyaIRI("urn:relatedTo"), new RyaIRI("urn:b1")));
        put(data, resolver, new RyaStatement(
                new RyaIRI("urn:b2"), new RyaIRI("urn:gem#pred"), new RyaIRI("urn:c")));
        return data;
    }

    private static TreeMap<Key, Value> createOutputData() throws TripleRowResolverException {
        TreeMap<Key, Value> data = new TreeMap<>();
        WholeRowTripleResolver resolver = new WholeRowTripleResolver();
        put(data, resolver, new RyaStatement(
                new RyaIRI("urn:a1"), new RyaIRI("urn:relatedTo"), new RyaIRI("urn:b1")));
        return data;
    }

    private static IteratorTestInput createIteratorInput() throws TripleRowResolverException {
        Map<String, String> options = new HashMap<>();
        options.put(RDFPropertyFilter.OPTION_PROPERTIES, "urn:relatedTo");

        return new IteratorTestInput(RDFPropertyFilter.class, options, new Range(), createInputData());
    }

    private static IteratorTestOutput createIteratorOutput() throws TripleRowResolverException {
        return new IteratorTestOutput(createOutputData());
    }

}
