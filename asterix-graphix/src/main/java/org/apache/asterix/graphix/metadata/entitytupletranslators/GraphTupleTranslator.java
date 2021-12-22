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

package org.apache.asterix.graphix.metadata.entitytupletranslators;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.graphix.common.metadata.GraphIdentifier;
import org.apache.asterix.graphix.metadata.bootstrap.GraphixMetadataIndexes;
import org.apache.asterix.graphix.metadata.bootstrap.GraphixMetadataRecordTypes;
import org.apache.asterix.graphix.metadata.entities.Graph;
import org.apache.asterix.graphix.metadata.entities.GraphDependencies;
import org.apache.asterix.metadata.entitytupletranslators.AbstractTupleTranslator;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IACursor;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

public class GraphTupleTranslator extends AbstractTupleTranslator<Graph> {
    // Payload field containing serialized Graph.
    private static final int GRAPH_PAYLOAD_TUPLE_FIELD_INDEX = 2;

    // For constructing our dependency, edge, and vertex lists.
    protected OrderedListBuilder listBuilder;
    protected OrderedListBuilder innerListBuilder;
    protected OrderedListBuilder nameListBuilder;
    protected IARecordBuilder subRecordBuilder;
    protected AOrderedListType stringListList;
    protected AOrderedListType stringList;

    public GraphTupleTranslator(boolean getTuple) {
        super(getTuple, GraphixMetadataIndexes.GRAPH_DATASET, GRAPH_PAYLOAD_TUPLE_FIELD_INDEX);
        if (getTuple) {
            listBuilder = new OrderedListBuilder();
            innerListBuilder = new OrderedListBuilder();
            nameListBuilder = new OrderedListBuilder();
            subRecordBuilder = new RecordBuilder();
            stringListList = new AOrderedListType(new AOrderedListType(BuiltinType.ASTRING, null), null);
            stringList = new AOrderedListType(BuiltinType.ASTRING, null);
        }
    }

    @Override
    protected Graph createMetadataEntityFromARecord(ARecord graphRecord) throws AlgebricksException {
        // Read in the dataverse name.
        DataverseName dataverseName = DataverseName.createFromCanonicalForm(((AString) graphRecord
                .getValueByPos(GraphixMetadataRecordTypes.GRAPH_ARECORD_DATAVERSENAME_FIELD_INDEX)).getStringValue());

        // Read in the graph name.
        String graphName =
                ((AString) graphRecord.getValueByPos(GraphixMetadataRecordTypes.GRAPH_ARECORD_GRAPHNAME_FIELD_INDEX))
                        .getStringValue();

        // Read in the dependencies.
        IACursor dependenciesCursor = ((AOrderedList) graphRecord
                .getValueByPos(GraphixMetadataRecordTypes.GRAPH_ARECORD_DEPENDENCIES_FIELD_INDEX)).getCursor();
        List<List<Triple<DataverseName, String, String>>> graphDependencies = new ArrayList<>();
        while (dependenciesCursor.next()) {
            List<Triple<DataverseName, String, String>> dependencyList = new ArrayList<>();
            IACursor qualifiedDependencyCursor = ((AOrderedList) dependenciesCursor.get()).getCursor();
            while (qualifiedDependencyCursor.next()) {
                Triple<DataverseName, String, String> dependency =
                        getDependency((AOrderedList) qualifiedDependencyCursor.get());
                dependencyList.add(dependency);
            }
            graphDependencies.add(dependencyList);
        }

        // Read in the vertex and edge lists.
        GraphIdentifier graphIdentifier = new GraphIdentifier(dataverseName, graphName);
        Graph.Schema graphSchema = readGraphSchema(graphRecord, graphIdentifier);
        return new Graph(graphIdentifier, graphSchema, new GraphDependencies(graphDependencies));
    }

    private Graph.Schema readGraphSchema(ARecord graphRecord, GraphIdentifier graphIdentifier) throws AsterixException {
        Graph.Schema.Builder schemaBuilder = new Graph.Schema.Builder(graphIdentifier);

        // Read in the vertex list.
        IACursor verticesCursor = ((AOrderedList) graphRecord
                .getValueByPos(GraphixMetadataRecordTypes.GRAPH_ARECORD_VERTICES_FIELD_INDEX)).getCursor();
        while (verticesCursor.next()) {
            ARecord vertex = (ARecord) verticesCursor.get();

            // Read in the label name.
            String labelName = ((AString) vertex
                    .getValueByPos(GraphixMetadataRecordTypes.GRAPH_VERTICES_ARECORD_LABEL_FIELD_INDEX))
                            .getStringValue();

            // Read in the primary key fields.
            List<List<String>> primaryKeyFields = new ArrayList<>();
            IACursor primaryKeyCursor = ((AOrderedList) vertex
                    .getValueByPos(GraphixMetadataRecordTypes.GRAPH_VERTICES_ARECORD_PRIMARY_KEY_FIELD_INDEX))
                            .getCursor();
            while (primaryKeyCursor.next()) {
                IACursor nameCursor = ((AOrderedList) primaryKeyCursor.get()).getCursor();
                primaryKeyFields.add(readNameList(nameCursor));
            }

            // Read in the vertex definition(s). Validate each definition.
            IACursor definitionsCursor = ((AOrderedList) vertex
                    .getValueByPos(GraphixMetadataRecordTypes.GRAPH_VERTICES_ARECORD_DEFINITIONS_FIELD_INDEX))
                            .getCursor();
            while (definitionsCursor.next()) {
                String definition = ((AString) definitionsCursor.get()).getStringValue();
                schemaBuilder.addVertex(labelName, primaryKeyFields, definition);
                switch (schemaBuilder.getLastError()) {
                    case NO_ERROR:
                        break;

                    case CONFLICTING_PRIMARY_KEY:
                        throw new AsterixException(ErrorCode.METADATA_ERROR,
                                "Conflicting primary keys for vertices with label " + labelName);

                    default:
                        throw new AsterixException(ErrorCode.METADATA_ERROR,
                                "Schema vertex was not returned, but the error is not a conflicting primary key!");
                }
            }
        }

        // Read in the edge list.
        IACursor edgesCursor =
                ((AOrderedList) graphRecord.getValueByPos(GraphixMetadataRecordTypes.GRAPH_ARECORD_EDGES_FIELD_INDEX))
                        .getCursor();
        while (edgesCursor.next()) {
            ARecord edge = (ARecord) edgesCursor.get();

            // Read in the label name.
            String labelName =
                    ((AString) edge.getValueByPos(GraphixMetadataRecordTypes.GRAPH_EDGES_ARECORD_LABEL_FIELD_INDEX))
                            .getStringValue();

            // Read in the destination label name.
            String destinationLabelName = ((AString) edge
                    .getValueByPos(GraphixMetadataRecordTypes.GRAPH_EDGES_ARECORD_DEST_LABEL_FIELD_INDEX))
                            .getStringValue();

            // Read in the source label name.
            String sourceLabelName = ((AString) edge
                    .getValueByPos(GraphixMetadataRecordTypes.GRAPH_EDGES_ARECORD_SOURCE_LABEL_FIELD_INDEX))
                            .getStringValue();

            // Read in the primary key fields.
            List<List<String>> primaryKeyFields = new ArrayList<>();
            IACursor primaryKeyCursor = ((AOrderedList) edge
                    .getValueByPos(GraphixMetadataRecordTypes.GRAPH_EDGES_ARECORD_PRIMARY_KEY_FIELD_INDEX)).getCursor();
            while (primaryKeyCursor.next()) {
                IACursor nameCursor = ((AOrderedList) primaryKeyCursor.get()).getCursor();
                primaryKeyFields.add(readNameList(nameCursor));
            }

            // Read in the destination key fields.
            List<List<String>> destinationKeyFields = new ArrayList<>();
            IACursor destinationKeyCursor = ((AOrderedList) edge
                    .getValueByPos(GraphixMetadataRecordTypes.GRAPH_EDGES_ARECORD_DEST_KEY_FIELD_INDEX)).getCursor();
            while (destinationKeyCursor.next()) {
                IACursor nameCursor = ((AOrderedList) destinationKeyCursor.get()).getCursor();
                destinationKeyFields.add(readNameList(nameCursor));
            }

            // Read in the source key fields.
            List<List<String>> sourceKeyFields = new ArrayList<>();
            IACursor sourceKeyCursor = ((AOrderedList) edge
                    .getValueByPos(GraphixMetadataRecordTypes.GRAPH_EDGES_ARECORD_SOURCE_KEY_FIELD_INDEX)).getCursor();
            while (sourceKeyCursor.next()) {
                IACursor nameCursor = ((AOrderedList) sourceKeyCursor.get()).getCursor();
                sourceKeyFields.add(readNameList(nameCursor));
            }

            // Read in the edge definition(s). Validate each definition.
            IACursor definitionsCursor = ((AOrderedList) edge
                    .getValueByPos(GraphixMetadataRecordTypes.GRAPH_EDGES_ARECORD_DEFINITIONS_FIELD_INDEX)).getCursor();
            while (definitionsCursor.next()) {
                String definition = ((AString) definitionsCursor.get()).getStringValue();
                schemaBuilder.addEdge(labelName, destinationLabelName, sourceLabelName, primaryKeyFields,
                        destinationKeyFields, sourceKeyFields, definition);
                switch (schemaBuilder.getLastError()) {
                    case NO_ERROR:
                        break;

                    case SOURCE_VERTEX_NOT_FOUND:
                        throw new AsterixException(ErrorCode.METADATA_ERROR,
                                "Source vertex " + sourceLabelName + " not found in the edge " + labelName + ".");

                    case DESTINATION_VERTEX_NOT_FOUND:
                        throw new AsterixException(ErrorCode.METADATA_ERROR, "Destination vertex "
                                + destinationLabelName + " not found in the edge " + labelName + ".");

                    case CONFLICTING_PRIMARY_KEY:
                    case CONFLICTING_SOURCE_VERTEX:
                    case CONFLICTING_DESTINATION_VERTEX:
                        throw new AsterixException(ErrorCode.METADATA_ERROR,
                                "Conflicting edge with the same label found: " + labelName);

                    default:
                        throw new AsterixException(ErrorCode.METADATA_ERROR,
                                "Schema edge was not returned, and an unexpected error encountered");
                }
            }
        }

        return schemaBuilder.build();
    }

    private List<String> readNameList(IACursor nameCursor) {
        List<String> fieldName = new ArrayList<>();
        while (nameCursor.next()) {
            String subName = ((AString) nameCursor.get()).getStringValue();
            fieldName.add(subName);
        }
        return fieldName;
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(Graph graph) throws HyracksDataException {
        // Write our primary key (dataverse name, graph name).
        String dataverseCanonicalName = graph.getDataverseName().getCanonicalForm();
        tupleBuilder.reset();
        aString.setValue(dataverseCanonicalName);
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
        aString.setValue(graph.getGraphName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        // Write the payload in the third field of the tuple.
        recordBuilder.reset(GraphixMetadataRecordTypes.GRAPH_RECORDTYPE);

        // Write the dataverse name.
        fieldValue.reset();
        aString.setValue(dataverseCanonicalName);
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(GraphixMetadataRecordTypes.GRAPH_ARECORD_DATAVERSENAME_FIELD_INDEX, fieldValue);

        // Write the graph name.
        fieldValue.reset();
        aString.setValue(graph.getGraphName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(GraphixMetadataRecordTypes.GRAPH_ARECORD_GRAPHNAME_FIELD_INDEX, fieldValue);

        // Write our dependencies.
        ArrayBackedValueStorage itemValue = new ArrayBackedValueStorage();
        listBuilder.reset((AOrderedListType) GraphixMetadataRecordTypes.GRAPH_RECORDTYPE
                .getFieldTypes()[GraphixMetadataRecordTypes.GRAPH_ARECORD_DEPENDENCIES_FIELD_INDEX]);
        for (List<Triple<DataverseName, String, String>> dependencies : graph.getDependencies()
                .getListRepresentation()) {
            List<String> subNames = new ArrayList<>();
            innerListBuilder.reset(stringListList);
            for (Triple<DataverseName, String, String> dependency : dependencies) {
                subNames.clear();
                getDependencySubNames(dependency, subNames);
                writeNameList(subNames, itemValue);
                innerListBuilder.addItem(itemValue);
            }
            itemValue.reset();
            innerListBuilder.write(itemValue.getDataOutput(), true);
            listBuilder.addItem(itemValue);
        }
        fieldValue.reset();
        listBuilder.write(fieldValue.getDataOutput(), true);
        recordBuilder.addField(GraphixMetadataRecordTypes.GRAPH_ARECORD_DEPENDENCIES_FIELD_INDEX, fieldValue);

        // Write our vertex set.
        listBuilder.reset((AOrderedListType) GraphixMetadataRecordTypes.GRAPH_RECORDTYPE
                .getFieldTypes()[GraphixMetadataRecordTypes.GRAPH_ARECORD_VERTICES_FIELD_INDEX]);
        for (Graph.Vertex vertex : graph.getGraphSchema().getVertices()) {
            writeVertexRecord(vertex, itemValue);
            listBuilder.addItem(itemValue);
        }
        fieldValue.reset();
        listBuilder.write(fieldValue.getDataOutput(), true);
        recordBuilder.addField(GraphixMetadataRecordTypes.GRAPH_ARECORD_VERTICES_FIELD_INDEX, fieldValue);

        // Write our edge set.
        listBuilder.reset((AOrderedListType) GraphixMetadataRecordTypes.GRAPH_RECORDTYPE
                .getFieldTypes()[GraphixMetadataRecordTypes.GRAPH_ARECORD_EDGES_FIELD_INDEX]);
        for (Graph.Edge edge : graph.getGraphSchema().getEdges()) {
            writeEdgeRecord(edge, itemValue);
            listBuilder.addItem(itemValue);
        }
        fieldValue.reset();
        listBuilder.write(fieldValue.getDataOutput(), true);
        recordBuilder.addField(GraphixMetadataRecordTypes.GRAPH_ARECORD_EDGES_FIELD_INDEX, fieldValue);

        // Finally, write our record.
        recordBuilder.write(tupleBuilder.getDataOutput(), true);
        tupleBuilder.addFieldEndOffset();
        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }

    private void writeVertexRecord(Graph.Vertex vertex, ArrayBackedValueStorage itemValue) throws HyracksDataException {
        subRecordBuilder.reset(GraphixMetadataRecordTypes.VERTICES_RECORDTYPE);

        // Write the label name.
        fieldValue.reset();
        aString.setValue(vertex.getLabelName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        subRecordBuilder.addField(GraphixMetadataRecordTypes.GRAPH_VERTICES_ARECORD_LABEL_FIELD_INDEX, fieldValue);

        // Write the primary key fields.
        fieldValue.reset();
        innerListBuilder.reset(stringListList);
        for (List<String> keyField : vertex.getPrimaryKeyFieldNames()) {
            writeNameList(keyField, itemValue);
            innerListBuilder.addItem(itemValue);
        }
        innerListBuilder.write(fieldValue.getDataOutput(), true);
        subRecordBuilder.addField(GraphixMetadataRecordTypes.GRAPH_VERTICES_ARECORD_PRIMARY_KEY_FIELD_INDEX,
                fieldValue);

        // Write the vertex definition(s).
        fieldValue.reset();
        innerListBuilder.reset(stringList);
        for (String definition : vertex.getDefinitions()) {
            itemValue.reset();
            aString.setValue(definition);
            stringSerde.serialize(aString, itemValue.getDataOutput());
            innerListBuilder.addItem(itemValue);
        }
        innerListBuilder.write(fieldValue.getDataOutput(), true);
        subRecordBuilder.addField(GraphixMetadataRecordTypes.GRAPH_VERTICES_ARECORD_DEFINITIONS_FIELD_INDEX,
                fieldValue);

        itemValue.reset();
        subRecordBuilder.write(itemValue.getDataOutput(), true);
    }

    private void writeEdgeRecord(Graph.Edge edge, ArrayBackedValueStorage itemValue) throws HyracksDataException {
        subRecordBuilder.reset(GraphixMetadataRecordTypes.EDGES_RECORDTYPE);

        // Write the label name.
        fieldValue.reset();
        aString.setValue(edge.getLabelName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        subRecordBuilder.addField(GraphixMetadataRecordTypes.GRAPH_EDGES_ARECORD_LABEL_FIELD_INDEX, fieldValue);

        // Write the destination label name.
        fieldValue.reset();
        aString.setValue(edge.getDestinationLabelName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        subRecordBuilder.addField(GraphixMetadataRecordTypes.GRAPH_EDGES_ARECORD_DEST_LABEL_FIELD_INDEX, fieldValue);

        // Write the source label name.
        fieldValue.reset();
        aString.setValue(edge.getSourceLabelName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        subRecordBuilder.addField(GraphixMetadataRecordTypes.GRAPH_EDGES_ARECORD_SOURCE_LABEL_FIELD_INDEX, fieldValue);

        // Write the primary key fields.
        fieldValue.reset();
        innerListBuilder.reset(stringListList);
        for (List<String> keyField : edge.getPrimaryKeyFieldNames()) {
            writeNameList(keyField, itemValue);
            innerListBuilder.addItem(itemValue);
        }
        innerListBuilder.write(fieldValue.getDataOutput(), true);
        subRecordBuilder.addField(GraphixMetadataRecordTypes.GRAPH_EDGES_ARECORD_PRIMARY_KEY_FIELD_INDEX, fieldValue);

        // Write the destination key fields.
        fieldValue.reset();
        innerListBuilder.reset(stringListList);
        for (List<String> keyField : edge.getDestinationKeyFieldNames()) {
            writeNameList(keyField, itemValue);
            innerListBuilder.addItem(itemValue);
        }
        innerListBuilder.write(fieldValue.getDataOutput(), true);
        subRecordBuilder.addField(GraphixMetadataRecordTypes.GRAPH_EDGES_ARECORD_DEST_KEY_FIELD_INDEX, fieldValue);

        // Write the source key fields.
        fieldValue.reset();
        innerListBuilder.reset(stringListList);
        for (List<String> keyField : edge.getSourceKeyFieldNames()) {
            writeNameList(keyField, itemValue);
            innerListBuilder.addItem(itemValue);
        }
        innerListBuilder.write(fieldValue.getDataOutput(), true);
        subRecordBuilder.addField(GraphixMetadataRecordTypes.GRAPH_EDGES_ARECORD_SOURCE_KEY_FIELD_INDEX, fieldValue);

        // Write the edge definition(s).
        fieldValue.reset();
        innerListBuilder.reset(stringList);
        for (String definition : edge.getDefinitions()) {
            itemValue.reset();
            aString.setValue(definition);
            stringSerde.serialize(aString, itemValue.getDataOutput());
            innerListBuilder.addItem(itemValue);
        }
        innerListBuilder.write(fieldValue.getDataOutput(), true);
        subRecordBuilder.addField(GraphixMetadataRecordTypes.GRAPH_EDGES_ARECORD_DEFINITIONS_FIELD_INDEX, fieldValue);

        itemValue.reset();
        subRecordBuilder.write(itemValue.getDataOutput(), true);
    }

    private void writeNameList(List<String> name, ArrayBackedValueStorage itemValue) throws HyracksDataException {
        nameListBuilder.reset(stringList);
        for (String subName : name) {
            itemValue.reset();
            aString.setValue(subName);
            stringSerde.serialize(aString, itemValue.getDataOutput());
            nameListBuilder.addItem(itemValue);
        }
        itemValue.reset();
        nameListBuilder.write(itemValue.getDataOutput(), true);
    }
}
