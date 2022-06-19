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

import static org.apache.asterix.graphix.metadata.bootstrap.GraphixRecordDetailProvider.FIELD_NAME_DEPENDENCY_ID;
import static org.apache.asterix.graphix.metadata.bootstrap.GraphixRecordDetailProvider.FIELD_NAME_ENTITY_DETAIL;
import static org.apache.asterix.graphix.metadata.bootstrap.GraphixRecordDetailProvider.FIELD_NAME_ENTITY_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_DATAVERSE_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_DEPENDENCIES;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_KIND;

import java.util.HashSet;
import java.util.Set;

import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.graphix.metadata.bootstrap.GraphixIndexDetailProvider;
import org.apache.asterix.graphix.metadata.bootstrap.GraphixRecordDetailProvider;
import org.apache.asterix.graphix.metadata.bootstrap.IRecordTypeDetail;
import org.apache.asterix.graphix.metadata.entity.dependency.DependencyIdentifier;
import org.apache.asterix.graphix.metadata.entity.dependency.FunctionRequirements;
import org.apache.asterix.graphix.metadata.entity.dependency.GraphRequirements;
import org.apache.asterix.graphix.metadata.entity.dependency.IEntityRequirements;
import org.apache.asterix.graphix.metadata.entity.dependency.ViewRequirements;
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
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

public class DependencyTupleTranslator extends AbstractTupleTranslator<IEntityRequirements> {
    // Payload field containing serialized GraphDependency record.
    private static final int GRAPH_DEPENDENCY_PAYLOAD_TUPLE_FIELD_INDEX = 1;

    // We are interested in the detail of the following records.
    private static final IRecordTypeDetail DEP_RECORD_DETAIL = GraphixRecordDetailProvider.getDependencyRecordDetail();
    private static final IRecordTypeDetail GRA_RECORD_DETAIL =
            GraphixRecordDetailProvider.getGraphDependencyRecordDetail();

    // For constructing our dependency lists.
    protected OrderedListBuilder listBuilder;
    protected IARecordBuilder depRecordBuilder;
    protected AOrderedListType stringList;

    public DependencyTupleTranslator(boolean getTuple) {
        super(getTuple, GraphixIndexDetailProvider.getGraphDependencyIndexDetail().getExtensionDataset(),
                GRAPH_DEPENDENCY_PAYLOAD_TUPLE_FIELD_INDEX);

        if (getTuple) {
            listBuilder = new OrderedListBuilder();
            depRecordBuilder = new RecordBuilder();

            // Avoid having to create the string list types multiple times.
            stringList = new AOrderedListType(BuiltinType.ASTRING, null);
        }
    }

    @Override
    protected IEntityRequirements createMetadataEntityFromARecord(ARecord requirements) throws AlgebricksException {
        // Read in our primary key value.
        IAObject primaryKeyValueObj = GRA_RECORD_DETAIL.getObjectForField(requirements, FIELD_NAME_DEPENDENCY_ID);
        String primaryKeyValue = ((AString) primaryKeyValueObj).getStringValue();

        // Read in the dataverse name.
        IAObject dataverseNameObj = GRA_RECORD_DETAIL.getObjectForField(requirements, FIELD_NAME_DATAVERSE_NAME);
        DataverseName dataverseName =
                DataverseName.createFromCanonicalForm(((AString) dataverseNameObj).getStringValue());

        // Read in the entity name.
        IAObject entityNameObj = GRA_RECORD_DETAIL.getObjectForField(requirements, FIELD_NAME_ENTITY_NAME);
        String entityName = ((AString) entityNameObj).getStringValue();

        // Read in the GraphDependency kind.
        IEntityRequirements.DependentKind entityKind;
        IAObject entityKindObj = GRA_RECORD_DETAIL.getObjectForField(requirements, FIELD_NAME_KIND);
        String entityKindString = ((AString) entityKindObj).getStringValue();
        if (entityKindString.equals(IEntityRequirements.DependentKind.FUNCTION.toString())) {
            entityKind = IEntityRequirements.DependentKind.FUNCTION;

        } else if (entityKindString.equals(IEntityRequirements.DependentKind.GRAPH.toString())) {
            entityKind = IEntityRequirements.DependentKind.GRAPH;

        } else { // entityKindString.equals(IEntityRequirements.DependentKind.VIEW.toString())
            entityKind = IEntityRequirements.DependentKind.VIEW;
        }

        // If we have a function, then we must read in the entity detail.
        String entityDetail = null;
        if (entityKind == IEntityRequirements.DependentKind.FUNCTION) {
            IAObject entityDetailObj = GRA_RECORD_DETAIL.getObjectForField(requirements, FIELD_NAME_ENTITY_DETAIL);
            entityDetail = ((AString) entityDetailObj).getStringValue();
        }

        // Read in the dependencies.
        Set<DependencyIdentifier> dependencyIdentifiers = new HashSet<>();
        IAObject dependenciesObj = GRA_RECORD_DETAIL.getObjectForField(requirements, FIELD_NAME_DEPENDENCIES);
        IACursor dependenciesCursor = ((AOrderedList) dependenciesObj).getCursor();
        while (dependenciesCursor.next()) {
            ARecord dependency = (ARecord) dependenciesCursor.get();

            // Read in the dataverse name.
            IAObject depDataverseNameObj = DEP_RECORD_DETAIL.getObjectForField(dependency, FIELD_NAME_DATAVERSE_NAME);
            DataverseName depDataverseName =
                    DataverseName.createFromCanonicalForm(((AString) depDataverseNameObj).getStringValue());

            // Read in the entity name.
            IAObject depEntityNameObj = DEP_RECORD_DETAIL.getObjectForField(dependency, FIELD_NAME_ENTITY_NAME);
            String depEntityName = ((AString) depEntityNameObj).getStringValue();

            // Read in the entity kind, and add the new dependency identifier.
            DependencyIdentifier.Kind depEntityKind;
            IAObject depEntityKindObj = DEP_RECORD_DETAIL.getObjectForField(dependency, FIELD_NAME_KIND);
            String depEntityKindString = ((AString) depEntityKindObj).getStringValue();
            if (depEntityKindString.equals(DependencyIdentifier.Kind.GRAPH.toString())) {
                depEntityKind = DependencyIdentifier.Kind.GRAPH;

            } else if (depEntityKindString.equals(DependencyIdentifier.Kind.DATASET.toString())) {
                depEntityKind = DependencyIdentifier.Kind.DATASET;

            } else if (depEntityKindString.equals(DependencyIdentifier.Kind.SYNONYM.toString())) {
                depEntityKind = DependencyIdentifier.Kind.SYNONYM;

            } else { // depEntityKindString.equals(DependencyIdentifier.Kind.FUNCTION.toString())
                depEntityKind = DependencyIdentifier.Kind.FUNCTION;
            }

            // If we have a function, then we must read in the entity detail.
            String depEntityDetail = null;
            if (depEntityKind == DependencyIdentifier.Kind.FUNCTION) {
                IAObject depEntityDetailObj = DEP_RECORD_DETAIL.getObjectForField(dependency, FIELD_NAME_ENTITY_DETAIL);
                depEntityDetail = ((AString) depEntityDetailObj).getStringValue();
            }

            DependencyIdentifier depIdentifier = (depEntityKind != DependencyIdentifier.Kind.FUNCTION)
                    ? new DependencyIdentifier(depDataverseName, depEntityName, depEntityKind)
                    : new DependencyIdentifier(depDataverseName, depEntityName, depEntityDetail, depEntityKind);
            dependencyIdentifiers.add(depIdentifier);
        }

        // Return the entity requirements, based on the type.
        switch (entityKind) {
            case FUNCTION:
                int functionArity = Integer.parseInt(entityDetail);
                FunctionSignature functionSignature = new FunctionSignature(dataverseName, entityName, functionArity);
                return new FunctionRequirements(functionSignature, dependencyIdentifiers, primaryKeyValue);

            case GRAPH:
                return new GraphRequirements(dataverseName, entityName, dependencyIdentifiers, primaryKeyValue);

            case VIEW:
                return new ViewRequirements(dataverseName, entityName, dependencyIdentifiers, primaryKeyValue);
        }
        return null;
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(IEntityRequirements requirements) throws HyracksDataException {
        // Write our primary key.
        tupleBuilder.reset();
        aString.setValue(requirements.getPrimaryKeyValue());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        // Write the payload in the third field of the tuple.
        recordBuilder.reset(GRA_RECORD_DETAIL.getRecordType());

        // Write our primary key.
        fieldValue.reset();
        aString.setValue(requirements.getPrimaryKeyValue());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(GRA_RECORD_DETAIL.getIndexForField(FIELD_NAME_DEPENDENCY_ID), fieldValue);

        // Write the dataverse name.
        fieldValue.reset();
        aString.setValue(requirements.getDataverseName().getCanonicalForm());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(GRA_RECORD_DETAIL.getIndexForField(FIELD_NAME_DATAVERSE_NAME), fieldValue);

        // Write the entity name.
        fieldValue.reset();
        aString.setValue(requirements.getEntityName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(GRA_RECORD_DETAIL.getIndexForField(FIELD_NAME_ENTITY_NAME), fieldValue);

        // Write the entity kind.
        fieldValue.reset();
        aString.setValue(requirements.getDependentKind().toString());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(GRA_RECORD_DETAIL.getIndexForField(FIELD_NAME_KIND), fieldValue);

        // Write our dependencies list.
        listBuilder.reset((AOrderedListType) GRA_RECORD_DETAIL.getTypeForField(FIELD_NAME_DEPENDENCIES));
        for (DependencyIdentifier dependency : requirements) {
            depRecordBuilder.reset(DEP_RECORD_DETAIL.getRecordType());

            // Write the dependency dataverse.
            fieldValue.reset();
            aString.setValue(dependency.getDataverseName().getCanonicalForm());
            stringSerde.serialize(aString, fieldValue.getDataOutput());
            depRecordBuilder.addField(DEP_RECORD_DETAIL.getIndexForField(FIELD_NAME_DATAVERSE_NAME), fieldValue);

            // Write the dependency entity name.
            fieldValue.reset();
            aString.setValue(dependency.getEntityName());
            stringSerde.serialize(aString, fieldValue.getDataOutput());
            depRecordBuilder.addField(DEP_RECORD_DETAIL.getIndexForField(FIELD_NAME_ENTITY_NAME), fieldValue);

            // Write the dependency kind.
            fieldValue.reset();
            aString.setValue(dependency.getDependencyKind().toString());
            stringSerde.serialize(aString, fieldValue.getDataOutput());
            depRecordBuilder.addField(DEP_RECORD_DETAIL.getIndexForField(FIELD_NAME_KIND), fieldValue);

            // Write the dependency entity detail, if it exists.
            if (dependency.getDependencyKind() == DependencyIdentifier.Kind.FUNCTION) {
                fieldValue.reset();
                aString.setValue(dependency.getEntityDetail());
                stringSerde.serialize(aString, fieldValue.getDataOutput());
                depRecordBuilder.addField(DEP_RECORD_DETAIL.getIndexForField(FIELD_NAME_ENTITY_DETAIL), fieldValue);
            }

            // Write the dependencies record.
            fieldValue.reset();
            depRecordBuilder.write(fieldValue.getDataOutput(), true);
            listBuilder.addItem(fieldValue);
        }
        fieldValue.reset();
        listBuilder.write(fieldValue.getDataOutput(), true);
        recordBuilder.addField(GRA_RECORD_DETAIL.getIndexForField(FIELD_NAME_DEPENDENCIES), fieldValue);

        // Write the entity detail, if it exists.
        if (requirements.getDependentKind() == IEntityRequirements.DependentKind.FUNCTION) {
            FunctionRequirements functionRequirements = (FunctionRequirements) requirements;
            fieldValue.reset();
            aString.setValue(functionRequirements.getArityAsString());
            stringSerde.serialize(aString, fieldValue.getDataOutput());
            recordBuilder.addField(GRA_RECORD_DETAIL.getIndexForField(FIELD_NAME_ENTITY_DETAIL), fieldValue);
        }

        // Finally, write our record.
        recordBuilder.write(tupleBuilder.getDataOutput(), true);
        tupleBuilder.addFieldEndOffset();
        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }
}
