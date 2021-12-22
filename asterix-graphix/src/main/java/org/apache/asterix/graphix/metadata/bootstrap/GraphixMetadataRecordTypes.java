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
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_PRIMARY_KEY;

import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;

public final class GraphixMetadataRecordTypes {
    public static final String RECORD_NAME_GRAPH = "GraphRecordType";
    public static final String RECORD_NAME_VERTICES = "VerticesRecordType";
    public static final String RECORD_NAME_EDGES = "EdgesRecordType";

    public static final String FIELD_NAME_DEFINITIONS = "Definitions";
    public static final String FIELD_NAME_DESTINATION_KEY = "DestinationKey";
    public static final String FIELD_NAME_DESTINATION_LABEL = "DestinationLabel";
    public static final String FIELD_NAME_EDGES = "Edges";
    public static final String FIELD_NAME_GRAPH_NAME = "GraphName";
    public static final String FIELD_NAME_LABEL = "Label";
    public static final String FIELD_NAME_SOURCE_KEY = "SourceKey";
    public static final String FIELD_NAME_SOURCE_LABEL = "SourceLabel";
    public static final String FIELD_NAME_VERTICES = "Vertices";

    public static final int GRAPH_VERTICES_ARECORD_LABEL_FIELD_INDEX = 0;
    public static final int GRAPH_VERTICES_ARECORD_PRIMARY_KEY_FIELD_INDEX = 1;
    public static final int GRAPH_VERTICES_ARECORD_DEFINITIONS_FIELD_INDEX = 2;
    public static final ARecordType VERTICES_RECORDTYPE = MetadataRecordTypes.createRecordType(RECORD_NAME_VERTICES,
            new String[] { FIELD_NAME_LABEL, FIELD_NAME_PRIMARY_KEY, FIELD_NAME_DEFINITIONS },
            new IAType[] { BuiltinType.ASTRING,
                    new AOrderedListType(new AOrderedListType(BuiltinType.ASTRING, null), null),
                    new AOrderedListType(BuiltinType.ASTRING, null) },
            true);

    public static final int GRAPH_EDGES_ARECORD_LABEL_FIELD_INDEX = 0;
    public static final int GRAPH_EDGES_ARECORD_DEST_LABEL_FIELD_INDEX = 1;
    public static final int GRAPH_EDGES_ARECORD_SOURCE_LABEL_FIELD_INDEX = 2;
    public static final int GRAPH_EDGES_ARECORD_PRIMARY_KEY_FIELD_INDEX = 3;
    public static final int GRAPH_EDGES_ARECORD_DEST_KEY_FIELD_INDEX = 4;
    public static final int GRAPH_EDGES_ARECORD_SOURCE_KEY_FIELD_INDEX = 5;
    public static final int GRAPH_EDGES_ARECORD_DEFINITIONS_FIELD_INDEX = 6;
    public static final ARecordType EDGES_RECORDTYPE = MetadataRecordTypes.createRecordType(RECORD_NAME_EDGES,
            new String[] { FIELD_NAME_LABEL, FIELD_NAME_DESTINATION_LABEL, FIELD_NAME_SOURCE_LABEL,
                    FIELD_NAME_PRIMARY_KEY, FIELD_NAME_DESTINATION_KEY, FIELD_NAME_SOURCE_KEY, FIELD_NAME_DEFINITIONS },
            new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING,
                    new AOrderedListType(new AOrderedListType(BuiltinType.ASTRING, null), null),
                    new AOrderedListType(new AOrderedListType(BuiltinType.ASTRING, null), null),
                    new AOrderedListType(new AOrderedListType(BuiltinType.ASTRING, null), null),
                    new AOrderedListType(BuiltinType.ASTRING, null) },
            true);

    public static final int GRAPH_ARECORD_DATAVERSENAME_FIELD_INDEX = 0;
    public static final int GRAPH_ARECORD_GRAPHNAME_FIELD_INDEX = 1;
    public static final int GRAPH_ARECORD_DEPENDENCIES_FIELD_INDEX = 2;
    public static final int GRAPH_ARECORD_VERTICES_FIELD_INDEX = 3;
    public static final int GRAPH_ARECORD_EDGES_FIELD_INDEX = 4;
    public static final ARecordType GRAPH_RECORDTYPE = MetadataRecordTypes.createRecordType(RECORD_NAME_GRAPH,
            new String[] { FIELD_NAME_DATAVERSE_NAME, FIELD_NAME_GRAPH_NAME, FIELD_NAME_DEPENDENCIES,
                    FIELD_NAME_VERTICES, FIELD_NAME_EDGES },
            new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING,
                    new AOrderedListType(new AOrderedListType(new AOrderedListType(BuiltinType.ASTRING, null), null),
                            null),
                    new AOrderedListType(VERTICES_RECORDTYPE, null), new AOrderedListType(EDGES_RECORDTYPE, null) },
            true);
}
