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

import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.rdftriplestore.RdfCloudTripleStore;
import org.apache.rya.rdftriplestore.RdfCloudTripleStoreConnection;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.helpers.AbstractSail;

public class GraphMiningSail<C extends RdfCloudTripleStoreConfiguration> extends AbstractSail {

    private final RdfCloudTripleStore<C> parentSail;

    public GraphMiningSail(Sail parentSail) {
        super();

        if (parentSail instanceof RdfCloudTripleStore) {
            this.parentSail = (RdfCloudTripleStore<C>) parentSail;
        } else {
            throw new IllegalArgumentException("Only RdfCloudTripleStore as the parent sail is supported!");
        }
    }

    @Override
    protected void initializeInternal() throws SailException {
        parentSail.initialize();
    }

    @Override
    protected void shutDownInternal() throws SailException {
        parentSail.shutDown();
    }

    @Override
    public boolean isWritable() throws SailException {
        return parentSail.isWritable();
    }

    @Override
    public ValueFactory getValueFactory() {
        return parentSail.getValueFactory();
    }

    @Override
    protected SailConnection getConnectionInternal() throws SailException {
        return new GraphMiningConnection<>(
                (RdfCloudTripleStoreConnection) parentSail.getConnection(), parentSail.getRyaDAO());
    }
}
