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
package org.apache.asterix.graphix.metadata.entity.dependency;

import java.io.Serializable;

import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.graphix.metadata.bootstrap.GraphixIndexDetailProvider;
import org.apache.asterix.metadata.api.ExtensionMetadataDatasetId;
import org.apache.asterix.metadata.api.IExtensionMetadataEntity;

/**
 * Metadata for describing the pair [entity, list of dependencies for said entity]. This includes the following:
 * 1. A primary key value associated with the pair (logically, this is ignored).
 * 2. The dataverse name associated with the entity.
 * 3. The name associated with the entity.
 * 4. The kind associated with the entity (FUNCTION, GRAPH, or VIEW).
 * 5. An iterator of the dependencies associated with the entity.
 */
public interface IEntityRequirements extends Iterable<DependencyIdentifier>, IExtensionMetadataEntity, Serializable {
    String getPrimaryKeyValue();

    DataverseName getDataverseName();

    String getEntityName();

    String getDisplayName();

    DependentKind getDependentKind();

    enum DependentKind {
        FUNCTION,
        GRAPH,
        VIEW,
    }

    default ExtensionMetadataDatasetId getDatasetId() {
        return GraphixIndexDetailProvider.getGraphDependencyIndexDetail().getExtensionDatasetID();
    }
}
