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
package org.apache.asterix.graphix.metadata.entity.schema;

import java.util.Objects;

import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.graphix.common.metadata.GraphIdentifier;
import org.apache.asterix.graphix.metadata.bootstrap.GraphixIndexDetailProvider;
import org.apache.asterix.metadata.api.ExtensionMetadataDatasetId;
import org.apache.asterix.metadata.api.IExtensionMetadataEntity;

/**
 * Metadata describing a graph view, composed of a {@link Schema} instance and a {@link GraphIdentifier}.
 */
public class Graph implements IExtensionMetadataEntity {
    private static final long serialVersionUID = 1L;

    private final GraphIdentifier graphIdentifier;
    private final Schema graphSchema;

    public Graph(GraphIdentifier graphIdentifier, Schema graphSchema) {
        this.graphSchema = Objects.requireNonNull(graphSchema);
        this.graphIdentifier = graphIdentifier;
    }

    public DataverseName getDataverseName() {
        return graphIdentifier.getDataverseName();
    }

    public String getGraphName() {
        return graphIdentifier.getGraphName();
    }

    public GraphIdentifier getIdentifier() {
        return graphIdentifier;
    }

    public Schema getGraphSchema() {
        return graphSchema;
    }

    @Override
    public ExtensionMetadataDatasetId getDatasetId() {
        return GraphixIndexDetailProvider.getGraphIndexDetail().getExtensionDatasetID();
    }
}
