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
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_DEPENDENCIES;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_KIND;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_PRIMARY_KEY;

import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;

/**
 * Provide detail about our two metadata extension datasets (Graph and GraphDependency), as well as their associated
 * record types (vertex definitions, edge definitions, vertex records, edge records, dependency records).
 */
public final class GraphixRecordDetailProvider {
    public static final String FIELD_NAME_BODY = "Body";
    public static final String FIELD_NAME_DEPENDENCY_ID = "DependencyID";
    public static final String FIELD_NAME_DESTINATION_KEY = "DestinationKey";
    public static final String FIELD_NAME_DESTINATION_LABEL = "DestinationLabel";
    public static final String FIELD_NAME_EDGES = "Edges";
    public static final String FIELD_NAME_ENTITY_NAME = "EntityName";
    public static final String FIELD_NAME_ENTITY_DETAIL = "EntityDetail";
    public static final String FIELD_NAME_GRAPH_NAME = "GraphName";
    public static final String FIELD_NAME_LABEL = "Label";
    public static final String FIELD_NAME_SOURCE_KEY = "SourceKey";
    public static final String FIELD_NAME_SOURCE_LABEL = "SourceLabel";
    public static final String FIELD_NAME_VERTICES = "Vertices";

    public static IRecordTypeDetail getVertexRecordDetail() {
        return vertexRecordDetail;
    }

    public static IRecordTypeDetail getEdgeRecordDetail() {
        return edgeRecordDetail;
    }

    public static IRecordTypeDetail getGraphRecordDetail() {
        return graphRecordDetail;
    }

    public static IRecordTypeDetail getDependencyRecordDetail() {
        return dependencyRecordDetail;
    }

    public static IRecordTypeDetail getGraphDependencyRecordDetail() {
        return graphDependencyRecordDetail;
    }

    private static abstract class AbstractRecordTypeDetail implements IRecordTypeDetail {
        // We set this in our child.
        protected ARecordType recordType;

        @Override
        public ARecordType getRecordType() {
            return recordType;
        }

        @Override
        public IAObject getObjectForField(ARecord record, String fieldName) {
            return record.getValueByPos(getIndexForField(fieldName));
        }

        @Override
        public IAType getTypeForField(String fieldName) {
            return getRecordType().getFieldTypes()[getIndexForField(fieldName)];
        }
    }

    // Record type for a Vertex object attached to a Graph.
    private static final IRecordTypeDetail vertexRecordDetail = new AbstractRecordTypeDetail() {
        { // Construct our record type here.
            String[] fieldNames = new String[] { FIELD_NAME_LABEL, FIELD_NAME_PRIMARY_KEY, FIELD_NAME_BODY };
            IAType[] fieldTypes = new IAType[] { BuiltinType.ASTRING,
                    new AOrderedListType(new AOrderedListType(BuiltinType.ASTRING, null), null), BuiltinType.ASTRING };
            recordType = MetadataRecordTypes.createRecordType(getRecordTypeName(), fieldNames, fieldTypes, true);
        }

        @Override
        public String getRecordTypeName() {
            return "VertexRecordType";
        }

        @Override
        public int getIndexForField(String fieldName) {
            switch (fieldName) {
                case FIELD_NAME_LABEL:
                    return 0;

                case FIELD_NAME_PRIMARY_KEY:
                    return 1;

                case FIELD_NAME_BODY:
                    return 2;

                default:
                    throw new IllegalArgumentException("Name " + fieldName + " not found for this record detail!");
            }
        }
    };

    // Record type for an Edge object attached to a Graph.
    private static final IRecordTypeDetail edgeRecordDetail = new AbstractRecordTypeDetail() {
        { // Construct our record type here.
            String[] fieldNames = new String[] { FIELD_NAME_LABEL, FIELD_NAME_DESTINATION_LABEL,
                    FIELD_NAME_SOURCE_LABEL, FIELD_NAME_SOURCE_KEY, FIELD_NAME_DESTINATION_KEY, FIELD_NAME_BODY };
            IAType[] fieldTypes = new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING,
                    new AOrderedListType(new AOrderedListType(BuiltinType.ASTRING, null), null),
                    new AOrderedListType(new AOrderedListType(BuiltinType.ASTRING, null), null), BuiltinType.ASTRING };
            recordType = MetadataRecordTypes.createRecordType(getRecordTypeName(), fieldNames, fieldTypes, true);
        }

        @Override
        public String getRecordTypeName() {
            return "EdgeRecordType";
        }

        @Override
        public int getIndexForField(String fieldName) {
            switch (fieldName) {
                case FIELD_NAME_LABEL:
                    return 0;

                case FIELD_NAME_DESTINATION_LABEL:
                    return 1;

                case FIELD_NAME_SOURCE_LABEL:
                    return 2;

                case FIELD_NAME_SOURCE_KEY:
                    return 3;

                case FIELD_NAME_DESTINATION_KEY:
                    return 4;

                case FIELD_NAME_BODY:
                    return 5;

                default:
                    throw new IllegalArgumentException("Name " + fieldName + " not found for this record detail!");
            }
        }
    };

    // Record type for a graph.
    private static final IRecordTypeDetail graphRecordDetail = new AbstractRecordTypeDetail() {
        { // Construct our record type here.
            String[] fieldNames = new String[] { FIELD_NAME_DATAVERSE_NAME, FIELD_NAME_GRAPH_NAME, FIELD_NAME_VERTICES,
                    FIELD_NAME_EDGES };
            IAType[] fieldTypes = new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING,
                    new AOrderedListType(vertexRecordDetail.getRecordType(), null),
                    new AOrderedListType(edgeRecordDetail.getRecordType(), null) };
            recordType = MetadataRecordTypes.createRecordType(getRecordTypeName(), fieldNames, fieldTypes, true);
        }

        @Override
        public String getRecordTypeName() {
            return "GraphRecordType";
        }

        @Override
        public int getIndexForField(String fieldName) {
            switch (fieldName) {
                case FIELD_NAME_DATAVERSE_NAME:
                    return 0;

                case FIELD_NAME_GRAPH_NAME:
                    return 1;

                case FIELD_NAME_VERTICES:
                    return 2;

                case FIELD_NAME_EDGES:
                    return 3;

                default:
                    throw new IllegalArgumentException("Name " + fieldName + " not found for this record detail!");
            }
        }
    };

    // Record type for a dependency attached to a GraphDependency.
    private static final IRecordTypeDetail dependencyRecordDetail = new AbstractRecordTypeDetail() {
        { // Construct our record type here.
            String[] fieldNames = new String[] { FIELD_NAME_DATAVERSE_NAME, FIELD_NAME_ENTITY_NAME, FIELD_NAME_KIND,
                    FIELD_NAME_ENTITY_DETAIL };
            IAType[] fieldTypes = new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING,
                    AUnionType.createMissableType(BuiltinType.ASTRING) };
            recordType = MetadataRecordTypes.createRecordType(getRecordTypeName(), fieldNames, fieldTypes, true);
        }

        @Override
        public String getRecordTypeName() {
            return "DependencyRecordType";
        }

        @Override
        public int getIndexForField(String fieldName) {
            switch (fieldName) {
                case FIELD_NAME_DATAVERSE_NAME:
                    return 0;

                case FIELD_NAME_ENTITY_NAME:
                    return 1;

                case FIELD_NAME_KIND:
                    return 2;

                case FIELD_NAME_ENTITY_DETAIL:
                    return 3;

                default:
                    throw new IllegalArgumentException("Name " + fieldName + " not found for this record detail!");
            }
        }
    };

    // Record type for a GraphDependency.
    private static final IRecordTypeDetail graphDependencyRecordDetail = new AbstractRecordTypeDetail() {
        { // Construct our record type here.
            String[] fieldNames = new String[] { FIELD_NAME_DEPENDENCY_ID, FIELD_NAME_DATAVERSE_NAME,
                    FIELD_NAME_ENTITY_NAME, FIELD_NAME_KIND, FIELD_NAME_DEPENDENCIES, FIELD_NAME_ENTITY_DETAIL };
            IAType[] fieldTypes = new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING,
                    BuiltinType.ASTRING, new AOrderedListType(dependencyRecordDetail.getRecordType(), null),
                    AUnionType.createMissableType(BuiltinType.ASTRING) };
            recordType = MetadataRecordTypes.createRecordType(getRecordTypeName(), fieldNames, fieldTypes, true);
        }

        @Override
        public String getRecordTypeName() {
            return "GraphDependencyRecordType";
        }

        @Override
        public int getIndexForField(String fieldName) {
            switch (fieldName) {
                case FIELD_NAME_DEPENDENCY_ID:
                    return 0;

                case FIELD_NAME_DATAVERSE_NAME:
                    return 1;

                case FIELD_NAME_ENTITY_NAME:
                    return 2;

                case FIELD_NAME_KIND:
                    return 3;

                case FIELD_NAME_DEPENDENCIES:
                    return 4;

                case FIELD_NAME_ENTITY_DETAIL:
                    return 5;

                default:
                    throw new IllegalArgumentException("Name " + fieldName + " not found for this record detail!");
            }
        }
    };
}
