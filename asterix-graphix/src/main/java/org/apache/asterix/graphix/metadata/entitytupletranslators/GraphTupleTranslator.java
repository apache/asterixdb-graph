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

import static org.apache.asterix.graphix.metadata.bootstrap.GraphixRecordDetailProvider.FIELD_NAME_BODY;
import static org.apache.asterix.graphix.metadata.bootstrap.GraphixRecordDetailProvider.FIELD_NAME_DESTINATION_KEY;
import static org.apache.asterix.graphix.metadata.bootstrap.GraphixRecordDetailProvider.FIELD_NAME_DESTINATION_LABEL;
import static org.apache.asterix.graphix.metadata.bootstrap.GraphixRecordDetailProvider.FIELD_NAME_EDGES;
import static org.apache.asterix.graphix.metadata.bootstrap.GraphixRecordDetailProvider.FIELD_NAME_GRAPH_NAME;
import static org.apache.asterix.graphix.metadata.bootstrap.GraphixRecordDetailProvider.FIELD_NAME_LABEL;
import static org.apache.asterix.graphix.metadata.bootstrap.GraphixRecordDetailProvider.FIELD_NAME_SOURCE_KEY;
import static org.apache.asterix.graphix.metadata.bootstrap.GraphixRecordDetailProvider.FIELD_NAME_SOURCE_LABEL;
import static org.apache.asterix.graphix.metadata.bootstrap.GraphixRecordDetailProvider.FIELD_NAME_VERTICES;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_DATAVERSE_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_PRIMARY_KEY;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.graphix.common.metadata.GraphIdentifier;
import org.apache.asterix.graphix.lang.struct.ElementLabel;
import org.apache.asterix.graphix.metadata.bootstrap.GraphixIndexDetailProvider;
import org.apache.asterix.graphix.metadata.bootstrap.GraphixRecordDetailProvider;
import org.apache.asterix.graphix.metadata.bootstrap.IRecordTypeDetail;
import org.apache.asterix.graphix.metadata.entity.schema.Edge;
import org.apache.asterix.graphix.metadata.entity.schema.Graph;
import org.apache.asterix.graphix.metadata.entity.schema.Schema;
import org.apache.asterix.graphix.metadata.entity.schema.Vertex;
import org.apache.asterix.metadata.entitytupletranslators.AbstractTupleTranslator;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IACursor;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

public class GraphTupleTranslator extends AbstractTupleTranslator<Graph> {
    // Payload field containing serialized Graph.
    private static final int GRAPH_PAYLOAD_TUPLE_FIELD_INDEX = 2;

    // We are interested in the detail of the following records.
    private static final IRecordTypeDetail EDGE_RECORD_DETAIL = GraphixRecordDetailProvider.getEdgeRecordDetail();
    private static final IRecordTypeDetail VERTEX_RECORD_DETAIL = GraphixRecordDetailProvider.getVertexRecordDetail();
    private static final IRecordTypeDetail GRAPH_RECORD_DETAIL = GraphixRecordDetailProvider.getGraphRecordDetail();

    // For constructing our edge and vertex lists.
    protected OrderedListBuilder listBuilder;
    protected OrderedListBuilder defListBuilder;
    protected OrderedListBuilder innerListBuilder;
    protected OrderedListBuilder nameListBuilder;
    protected IARecordBuilder elemRecordBuilder;
    protected IARecordBuilder defRecordBuilder;
    protected AOrderedListType stringListList;
    protected AOrderedListType stringList;

    public GraphTupleTranslator(boolean getTuple) {
        super(getTuple, GraphixIndexDetailProvider.getGraphIndexDetail().getExtensionDataset(),
                GRAPH_PAYLOAD_TUPLE_FIELD_INDEX);

        if (getTuple) {
            listBuilder = new OrderedListBuilder();
            defListBuilder = new OrderedListBuilder();
            innerListBuilder = new OrderedListBuilder();
            nameListBuilder = new OrderedListBuilder();
            elemRecordBuilder = new RecordBuilder();
            defRecordBuilder = new RecordBuilder();

            // Avoid having to create the string list types multiple times.
            stringListList = new AOrderedListType(new AOrderedListType(BuiltinType.ASTRING, null), null);
            stringList = new AOrderedListType(BuiltinType.ASTRING, null);
        }
    }

    @Override
    protected Graph createMetadataEntityFromARecord(ARecord graph) throws AlgebricksException {
        // Read in the dataverse name.
        IAObject dataverseNameObj = GRAPH_RECORD_DETAIL.getObjectForField(graph, FIELD_NAME_DATAVERSE_NAME);
        DataverseName dataverseName =
                DataverseName.createFromCanonicalForm(((AString) dataverseNameObj).getStringValue());

        // Read in the graph name.
        IAObject graphNameObj = GRAPH_RECORD_DETAIL.getObjectForField(graph, FIELD_NAME_GRAPH_NAME);
        String graphName = ((AString) graphNameObj).getStringValue();

        // Read in the vertex and edge lists.
        GraphIdentifier graphIdentifier = new GraphIdentifier(dataverseName, graphName);
        Schema graphSchema = readGraphSchema(graph, graphIdentifier);
        return new Graph(graphIdentifier, graphSchema);
    }

    private Schema readGraphSchema(ARecord graph, GraphIdentifier graphIdentifier) throws AsterixException {
        Schema.Builder schemaBuilder = new Schema.Builder(graphIdentifier);

        // Read in the vertex list.
        IAObject verticesObj = GRAPH_RECORD_DETAIL.getObjectForField(graph, FIELD_NAME_VERTICES);
        IACursor verticesCursor = ((AOrderedList) verticesObj).getCursor();
        while (verticesCursor.next()) {
            ARecord vertex = (ARecord) verticesCursor.get();

            // Read in the label name.
            IAObject labelNameObj = VERTEX_RECORD_DETAIL.getObjectForField(vertex, FIELD_NAME_LABEL);
            ElementLabel elementLabel = new ElementLabel(((AString) labelNameObj).getStringValue(), false);

            // Read in the primary key fields.
            List<List<String>> primaryKeyFields = new ArrayList<>();
            IAObject primaryKeyObj = VERTEX_RECORD_DETAIL.getObjectForField(vertex, FIELD_NAME_PRIMARY_KEY);
            IACursor primaryKeyCursor = ((AOrderedList) primaryKeyObj).getCursor();
            while (primaryKeyCursor.next()) {
                IACursor nameCursor = ((AOrderedList) primaryKeyCursor.get()).getCursor();
                primaryKeyFields.add(readNameList(nameCursor));
            }

            // Read in the definition body.
            IAObject bodyObj = VERTEX_RECORD_DETAIL.getObjectForField(vertex, FIELD_NAME_BODY);
            String definitionBody = ((AString) bodyObj).getStringValue();

            // Read in the vertex definition, and perform validation of the metadata record.
            schemaBuilder.addVertex(elementLabel, primaryKeyFields, definitionBody);
            switch (schemaBuilder.getLastError()) {
                case NO_ERROR:
                    break;

                case VERTEX_LABEL_CONFLICT:
                    throw new AsterixException(ErrorCode.METADATA_ERROR,
                            "Conflicting vertex label found: " + elementLabel);

                default:
                    throw new AsterixException(ErrorCode.METADATA_ERROR,
                            "Constructor vertex was not returned, but the error is not a conflicting vertex label!");
            }
        }

        // Read in the edge list.
        IAObject edgesObj = GRAPH_RECORD_DETAIL.getObjectForField(graph, FIELD_NAME_EDGES);
        IACursor edgesCursor = ((AOrderedList) edgesObj).getCursor();
        while (edgesCursor.next()) {
            ARecord edge = (ARecord) edgesCursor.get();

            // Read in the label name.
            IAObject labelNameObj = EDGE_RECORD_DETAIL.getObjectForField(edge, FIELD_NAME_LABEL);
            ElementLabel edgeLabel = new ElementLabel(((AString) labelNameObj).getStringValue(), false);

            // Read in the destination label name.
            IAObject destinationLabelNameObj = EDGE_RECORD_DETAIL.getObjectForField(edge, FIELD_NAME_DESTINATION_LABEL);
            AString destinationLabelString = (AString) destinationLabelNameObj;
            ElementLabel destinationLabel = new ElementLabel(destinationLabelString.getStringValue(), false);

            // Read in the source label name.
            IAObject sourceLabelNameObj = EDGE_RECORD_DETAIL.getObjectForField(edge, FIELD_NAME_SOURCE_LABEL);
            ElementLabel sourceLabel = new ElementLabel(((AString) sourceLabelNameObj).getStringValue(), false);

            // Read in the source key fields.
            List<List<String>> sourceKeyFields = new ArrayList<>();
            IAObject sourceKeyObj = EDGE_RECORD_DETAIL.getObjectForField(edge, FIELD_NAME_SOURCE_KEY);
            IACursor sourceKeyCursor = ((AOrderedList) sourceKeyObj).getCursor();
            while (sourceKeyCursor.next()) {
                IACursor nameCursor = ((AOrderedList) sourceKeyCursor.get()).getCursor();
                sourceKeyFields.add(readNameList(nameCursor));
            }

            // Read in the destination key fields (this is common to all edge definition records).
            List<List<String>> destinationKeyFields = new ArrayList<>();
            IAObject destinationKeyObj = EDGE_RECORD_DETAIL.getObjectForField(edge, FIELD_NAME_DESTINATION_KEY);
            IACursor destinationKeyCursor = ((AOrderedList) destinationKeyObj).getCursor();
            while (destinationKeyCursor.next()) {
                IACursor nameCursor = ((AOrderedList) destinationKeyCursor.get()).getCursor();
                destinationKeyFields.add(readNameList(nameCursor));
            }

            // Read in the definition body.
            IAObject bodyObj = EDGE_RECORD_DETAIL.getObjectForField(edge, FIELD_NAME_BODY);
            String definitionBody = ((AString) bodyObj).getStringValue();

            // Finally, read in the edge definition and perform validation of the metadata record.
            schemaBuilder.addEdge(edgeLabel, destinationLabel, sourceLabel, destinationKeyFields, sourceKeyFields,
                    definitionBody);
            switch (schemaBuilder.getLastError()) {
                case NO_ERROR:
                    break;

                case SOURCE_VERTEX_NOT_FOUND:
                    throw new AsterixException(ErrorCode.METADATA_ERROR,
                            "Source vertex " + sourceLabel + " not found in the edge " + edgeLabel + ".");

                case DESTINATION_VERTEX_NOT_FOUND:
                    throw new AsterixException(ErrorCode.METADATA_ERROR,
                            "Destination vertex " + destinationLabel + " not found in the edge " + edgeLabel + ".");

                case EDGE_LABEL_CONFLICT:
                    throw new AsterixException(ErrorCode.METADATA_ERROR, "Conflicting edge label found: " + edgeLabel);

                default:
                    throw new AsterixException(ErrorCode.METADATA_ERROR,
                            "Edge constructor was not returned, and an unexpected error encountered");
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
        recordBuilder.reset(GRAPH_RECORD_DETAIL.getRecordType());

        // Write the dataverse name.
        fieldValue.reset();
        aString.setValue(dataverseCanonicalName);
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(GRAPH_RECORD_DETAIL.getIndexForField(FIELD_NAME_DATAVERSE_NAME), fieldValue);

        // Write the graph name.
        fieldValue.reset();
        aString.setValue(graph.getGraphName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(GRAPH_RECORD_DETAIL.getIndexForField(FIELD_NAME_GRAPH_NAME), fieldValue);

        // Write our vertex set.
        ArrayBackedValueStorage itemValue = new ArrayBackedValueStorage();
        listBuilder.reset((AOrderedListType) GRAPH_RECORD_DETAIL.getTypeForField(FIELD_NAME_VERTICES));
        for (Vertex vertex : graph.getGraphSchema().getVertices()) {
            writeVertexRecord(vertex, itemValue);
            listBuilder.addItem(itemValue);
        }
        fieldValue.reset();
        listBuilder.write(fieldValue.getDataOutput(), true);
        recordBuilder.addField(GRAPH_RECORD_DETAIL.getIndexForField(FIELD_NAME_VERTICES), fieldValue);

        // Write our edge set.
        listBuilder.reset((AOrderedListType) GRAPH_RECORD_DETAIL.getTypeForField(FIELD_NAME_EDGES));
        for (Edge edge : graph.getGraphSchema().getEdges()) {
            writeEdgeRecord(edge, itemValue);
            listBuilder.addItem(itemValue);
        }
        fieldValue.reset();
        listBuilder.write(fieldValue.getDataOutput(), true);
        recordBuilder.addField(GRAPH_RECORD_DETAIL.getIndexForField(FIELD_NAME_EDGES), fieldValue);

        // Finally, write our record.
        recordBuilder.write(tupleBuilder.getDataOutput(), true);
        tupleBuilder.addFieldEndOffset();
        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }

    private void writeVertexRecord(Vertex vertex, ArrayBackedValueStorage itemValue) throws HyracksDataException {
        elemRecordBuilder.reset(VERTEX_RECORD_DETAIL.getRecordType());

        // Write the label name.
        fieldValue.reset();
        aString.setValue(vertex.getLabel().toString());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        elemRecordBuilder.addField(VERTEX_RECORD_DETAIL.getIndexForField(FIELD_NAME_LABEL), fieldValue);

        // Write the primary key fields.
        fieldValue.reset();
        innerListBuilder.reset(new AOrderedListType(new AOrderedListType(BuiltinType.ASTRING, null), null));
        for (List<String> keyField : vertex.getPrimaryKeyFieldNames()) {
            writeNameList(keyField, itemValue);
            innerListBuilder.addItem(itemValue);
        }
        innerListBuilder.write(fieldValue.getDataOutput(), true);
        elemRecordBuilder.addField(VERTEX_RECORD_DETAIL.getIndexForField(FIELD_NAME_PRIMARY_KEY), fieldValue);

        // Write the definition body.
        fieldValue.reset();
        aString.setValue(vertex.getDefinitionBody());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        elemRecordBuilder.addField(VERTEX_RECORD_DETAIL.getIndexForField(FIELD_NAME_BODY), fieldValue);

        itemValue.reset();
        elemRecordBuilder.write(itemValue.getDataOutput(), true);
    }

    private void writeEdgeRecord(Edge edge, ArrayBackedValueStorage itemValue) throws HyracksDataException {
        elemRecordBuilder.reset(EDGE_RECORD_DETAIL.getRecordType());

        // Write the label name.
        fieldValue.reset();
        aString.setValue(edge.getLabel().toString());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        elemRecordBuilder.addField(EDGE_RECORD_DETAIL.getIndexForField(FIELD_NAME_LABEL), fieldValue);

        // Write the destination label name.
        fieldValue.reset();
        aString.setValue(edge.getDestinationLabel().toString());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        elemRecordBuilder.addField(EDGE_RECORD_DETAIL.getIndexForField(FIELD_NAME_DESTINATION_LABEL), fieldValue);

        // Write the source label name.
        fieldValue.reset();
        aString.setValue(edge.getSourceLabel().toString());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        elemRecordBuilder.addField(EDGE_RECORD_DETAIL.getIndexForField(FIELD_NAME_SOURCE_LABEL), fieldValue);

        // Write the source key fields.
        fieldValue.reset();
        innerListBuilder.reset(stringListList);
        for (List<String> keyField : edge.getSourceKeyFieldNames()) {
            writeNameList(keyField, itemValue);
            innerListBuilder.addItem(itemValue);
        }
        innerListBuilder.write(fieldValue.getDataOutput(), true);
        elemRecordBuilder.addField(EDGE_RECORD_DETAIL.getIndexForField(FIELD_NAME_SOURCE_KEY), fieldValue);

        // Write the destination key fields.
        fieldValue.reset();
        innerListBuilder.reset(stringListList);
        for (List<String> keyField : edge.getDestinationKeyFieldNames()) {
            writeNameList(keyField, itemValue);
            innerListBuilder.addItem(itemValue);
        }
        innerListBuilder.write(fieldValue.getDataOutput(), true);
        elemRecordBuilder.addField(EDGE_RECORD_DETAIL.getIndexForField(FIELD_NAME_DESTINATION_KEY), fieldValue);

        // Write the definition body.
        fieldValue.reset();
        aString.setValue(edge.getDefinitionBody());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        elemRecordBuilder.addField(EDGE_RECORD_DETAIL.getIndexForField(FIELD_NAME_BODY), fieldValue);

        itemValue.reset();
        elemRecordBuilder.write(itemValue.getDataOutput(), true);
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
