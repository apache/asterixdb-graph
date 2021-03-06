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

import java.util.Arrays;
import java.util.List;

import org.apache.asterix.common.metadata.MetadataIndexImmutableProperties;
import org.apache.asterix.graphix.extension.GraphixMetadataExtension;
import org.apache.asterix.graphix.metadata.entity.dependency.IEntityRequirements;
import org.apache.asterix.graphix.metadata.entity.schema.Graph;
import org.apache.asterix.graphix.metadata.entitytupletranslators.DependencyTupleTranslator;
import org.apache.asterix.graphix.metadata.entitytupletranslators.GraphTupleTranslator;
import org.apache.asterix.metadata.api.ExtensionMetadataDataset;
import org.apache.asterix.metadata.api.ExtensionMetadataDatasetId;
import org.apache.asterix.metadata.api.IMetadataEntityTupleTranslatorFactory;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;

/**
 * Provide detail about two metadata extension indexes: Graph and GraphDependency.
 */
public final class GraphixIndexDetailProvider {
    public static IGraphixIndexDetail<Graph> getGraphIndexDetail() {
        return graphIndexDetail;
    }

    public static IGraphixIndexDetail<IEntityRequirements> getGraphDependencyIndexDetail() {
        return dependencyIndexDetail;
    }

    private static final IGraphixIndexDetail<Graph> graphIndexDetail = new IGraphixIndexDetail<>() {
        private final ExtensionMetadataDatasetId datasetID = new ExtensionMetadataDatasetId(
                GraphixMetadataExtension.GRAPHIX_METADATA_EXTENSION_ID, getDatasetName());

        private final ExtensionMetadataDataset<Graph> metadataDataset;
        { // Construct our metadata dataset here.
            MetadataIndexImmutableProperties indexProperties = new MetadataIndexImmutableProperties(getDatasetName(),
                    MetadataIndexImmutableProperties.FIRST_AVAILABLE_EXTENSION_METADATA_DATASET_ID,
                    MetadataIndexImmutableProperties.FIRST_AVAILABLE_EXTENSION_METADATA_DATASET_ID);
            IAType[] fieldTypes = new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING };
            List<List<String>> fieldNames = Arrays.asList(List.of(MetadataRecordTypes.FIELD_NAME_DATAVERSE_NAME),
                    List.of(GraphixRecordDetailProvider.FIELD_NAME_GRAPH_NAME));
            IRecordTypeDetail graphRecordDetail = GraphixRecordDetailProvider.getGraphRecordDetail();
            metadataDataset = new ExtensionMetadataDataset<>(indexProperties, 3, fieldTypes, fieldNames, 0,
                    graphRecordDetail.getRecordType(), true, new int[] { 0, 1 }, datasetID,
                    (IMetadataEntityTupleTranslatorFactory<Graph>) GraphTupleTranslator::new);
        }

        @Override
        public String getDatasetName() {
            return "Graph";
        }

        @Override
        public ExtensionMetadataDatasetId getExtensionDatasetID() {
            return datasetID;
        }

        @Override
        public ExtensionMetadataDataset<Graph> getExtensionDataset() {
            return metadataDataset;
        }
    };

    private static final IGraphixIndexDetail<IEntityRequirements> dependencyIndexDetail = new IGraphixIndexDetail<>() {
        private final ExtensionMetadataDatasetId datasetID = new ExtensionMetadataDatasetId(
                GraphixMetadataExtension.GRAPHIX_METADATA_EXTENSION_ID, getDatasetName());

        private final ExtensionMetadataDataset<IEntityRequirements> metadataDataset;
        { // Construct our metadata dataset here. Our primary key is autogenerated.
            MetadataIndexImmutableProperties indexProperties = new MetadataIndexImmutableProperties(getDatasetName(),
                    MetadataIndexImmutableProperties.FIRST_AVAILABLE_EXTENSION_METADATA_DATASET_ID + 1,
                    MetadataIndexImmutableProperties.FIRST_AVAILABLE_EXTENSION_METADATA_DATASET_ID + 1);
            IAType[] fieldTypes = new IAType[] { BuiltinType.ASTRING };
            List<List<String>> fieldNames = List.of(List.of(GraphixRecordDetailProvider.FIELD_NAME_DEPENDENCY_ID));
            IRecordTypeDetail dependencyRecordDetail = GraphixRecordDetailProvider.getGraphDependencyRecordDetail();
            metadataDataset = new ExtensionMetadataDataset<>(indexProperties, 2, fieldTypes, fieldNames, 0,
                    dependencyRecordDetail.getRecordType(), true, new int[] { 0 }, datasetID,
                    (IMetadataEntityTupleTranslatorFactory<IEntityRequirements>) DependencyTupleTranslator::new);
        }

        @Override
        public String getDatasetName() {
            return "GraphDependency";
        }

        @Override
        public ExtensionMetadataDatasetId getExtensionDatasetID() {
            return datasetID;
        }

        @Override
        public ExtensionMetadataDataset<IEntityRequirements> getExtensionDataset() {
            return metadataDataset;
        }
    };
}
