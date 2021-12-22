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
package org.apache.asterix.graphix.metadata.bootstrap;

import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_DATAVERSE_NAME;

import java.util.Arrays;
import java.util.Collections;

import org.apache.asterix.common.metadata.MetadataIndexImmutableProperties;
import org.apache.asterix.graphix.extension.GraphixMetadataExtension;
import org.apache.asterix.graphix.metadata.entities.Graph;
import org.apache.asterix.graphix.metadata.entitytupletranslators.GraphTupleTranslator;
import org.apache.asterix.metadata.api.ExtensionMetadataDataset;
import org.apache.asterix.metadata.api.ExtensionMetadataDatasetId;
import org.apache.asterix.metadata.api.IMetadataEntityTupleTranslatorFactory;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;

public class GraphixMetadataIndexes {
    public static final String METADATA_DATASET_NAME_GRAPH = "Graph";

    public static final ExtensionMetadataDatasetId GRAPH_METADATA_DATASET_EXTENSION_ID = new ExtensionMetadataDatasetId(
            GraphixMetadataExtension.GRAPHIX_METADATA_EXTENSION_ID, METADATA_DATASET_NAME_GRAPH);

    public static final MetadataIndexImmutableProperties PROPERTIES_GRAPH_METADATA_DATASET =
            new MetadataIndexImmutableProperties(METADATA_DATASET_NAME_GRAPH,
                    MetadataIndexImmutableProperties.FIRST_AVAILABLE_EXTENSION_METADATA_DATASET_ID,
                    MetadataIndexImmutableProperties.FIRST_AVAILABLE_EXTENSION_METADATA_DATASET_ID);

    public static final ExtensionMetadataDataset<Graph> GRAPH_DATASET = new ExtensionMetadataDataset<>(
            PROPERTIES_GRAPH_METADATA_DATASET, 3, new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING },
            Arrays.asList(Collections.singletonList(FIELD_NAME_DATAVERSE_NAME),
                    Collections.singletonList(GraphixMetadataRecordTypes.FIELD_NAME_GRAPH_NAME)),
            0, GraphixMetadataRecordTypes.GRAPH_RECORDTYPE, true, new int[] { 0, 1 },
            GRAPH_METADATA_DATASET_EXTENSION_ID,
            (IMetadataEntityTupleTranslatorFactory<Graph>) GraphTupleTranslator::new);
}
