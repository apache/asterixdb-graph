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
package org.apache.asterix.graphix.lang.rewrites.record;

import static org.apache.asterix.graphix.lang.rewrites.util.LowerRewritingUtil.buildAccessorList;
import static org.apache.asterix.graphix.lang.rewrites.util.LowerRewritingUtil.buildSelectBlocksWithDecls;
import static org.apache.asterix.graphix.lang.rewrites.util.LowerRewritingUtil.buildSelectWithSelectBlockStream;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.asterix.graphix.common.metadata.GraphElementIdentifier;
import org.apache.asterix.graphix.common.metadata.GraphIdentifier;
import org.apache.asterix.graphix.lang.expression.EdgePatternExpr;
import org.apache.asterix.graphix.lang.rewrites.common.ElementLookupTable;
import org.apache.asterix.graphix.lang.rewrites.lower.LowerSupplierContext;
import org.apache.asterix.graphix.lang.rewrites.util.LowerRewritingUtil;
import org.apache.asterix.graphix.lang.statement.GraphElementDecl;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.expression.FieldBinding;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.sqlpp.clause.Projection;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;

/**
 * Intermediate lowered representation of a query edge (just the body), constructed with the following process:
 * 1. For each label of the edge, perform a UNION-ALL of their respective normalized bodies.
 * 2. Build a SELECT-BLOCK for each edge label, using the result of step 1. For each SELECT-CLAUSE, attach the edge
 * label, the edge direction, access to the source key, and access to the destination key, in addition to the
 * expression from step 1.
 * 3. Now having a list of SELECT-BLOCKs, UNION-ALL each SELECT-BLOCK and build a single SELECT-EXPR. This is the
 * lowered representation of our edge.
 * If this is a sub-path, we will also keep track of the contained {@link VertexRecord} here.
 */
public class EdgeRecord implements IElementRecord {
    // The details surrounding our edge will be included in the following field (alongside our body).
    public static final String EDGE_DETAIL_NAME = "_GraphixEdgeDetail";

    // We attach the following fields to the SELECT containing a declaration body.
    public static final String DIRECTION_FIELD_NAME = "EdgeDirection";
    public static final String SOURCE_KEY_FIELD_NAME = "SourceKey";
    public static final String DEST_KEY_FIELD_NAME = "DestinationKey";

    private final List<GraphElementIdentifier> elementIdentifiers;
    private final List<VertexRecord> internalVertices;
    private final EdgePatternExpr edgePatternExpr;
    private final Expression edgeExpression;
    private final VariableExpr edgeVar;

    public EdgeRecord(EdgePatternExpr edgePatternExpr, GraphIdentifier graphIdentifier,
            LowerSupplierContext lowerSupplierContext) {
        EdgeDescriptor edgeDescriptor = edgePatternExpr.getEdgeDescriptor();
        this.elementIdentifiers = edgeDescriptor.generateIdentifiers(graphIdentifier);
        this.edgeVar = new VariableExpr(edgeDescriptor.getVariableExpr().getVar());
        this.edgePatternExpr = edgePatternExpr;

        // Build the SELECT-EXPR associated with the given element identifiers.
        ElementLookupTable<GraphElementIdentifier> elementLookupTable = lowerSupplierContext.getElementLookupTable();
        List<GraphElementDecl> graphElementDeclList =
                elementIdentifiers.stream().map(elementLookupTable::getElementDecl).collect(Collectors.toList());
        List<SelectBlock> selectBlockList = buildSelectBlocksWithDecls(graphElementDeclList, edgeVar, d -> {
            List<Projection> projectionList = new ArrayList<>();

            // Build the label binding for the element detail projection.
            List<FieldBinding> elementDetailBindings = new ArrayList<>();
            StringLiteral labelLiteral = new StringLiteral(d.getIdentifier().getElementLabel().toString());
            LiteralExpr labelFieldValue = new LiteralExpr(labelLiteral);
            LiteralExpr labelFieldName = new LiteralExpr(new StringLiteral(ELEMENT_LABEL_FIELD_NAME));
            elementDetailBindings.add(new FieldBinding(labelFieldName, labelFieldValue));
            RecordConstructor elementDetailRecord = new RecordConstructor(elementDetailBindings);
            projectionList.add(new Projection(Projection.Kind.NAMED_EXPR, elementDetailRecord, ELEMENT_DETAIL_NAME));

            // Build the direction binding for the edge detail projection.
            List<FieldBinding> edgeDetailBindings = new ArrayList<>();
            LiteralExpr directionFieldValue =
                    new LiteralExpr(new StringLiteral(edgeDescriptor.getEdgeDirection().toString().toUpperCase()));
            LiteralExpr directionFieldName = new LiteralExpr(new StringLiteral(DIRECTION_FIELD_NAME));
            edgeDetailBindings.add(new FieldBinding(directionFieldName, directionFieldValue));

            // Access our source key value, and append this expression.
            List<FieldBinding> sourceKeyBindings =
                    buildAccessorList(edgeVar, elementLookupTable.getEdgeSourceKeys(d.getIdentifier())).stream()
                            .map(LowerRewritingUtil::buildFieldBindingFromFieldAccessor).collect(Collectors.toList());
            RecordConstructor sourceKeyAccess = new RecordConstructor(sourceKeyBindings);
            LiteralExpr sourceKeyName = new LiteralExpr(new StringLiteral(SOURCE_KEY_FIELD_NAME));
            edgeDetailBindings.add(new FieldBinding(sourceKeyName, sourceKeyAccess));

            // Access our destination key value, and append this expression.
            List<FieldBinding> destKeyBindings =
                    buildAccessorList(edgeVar, elementLookupTable.getEdgeDestKeys(d.getIdentifier())).stream()
                            .map(LowerRewritingUtil::buildFieldBindingFromFieldAccessor).collect(Collectors.toList());
            RecordConstructor destKeyAccess = new RecordConstructor(destKeyBindings);
            LiteralExpr destKeyName = new LiteralExpr(new StringLiteral(DEST_KEY_FIELD_NAME));
            edgeDetailBindings.add(new FieldBinding(destKeyName, destKeyAccess));
            RecordConstructor edgeDetailRecord = new RecordConstructor(edgeDetailBindings);
            projectionList.add(new Projection(Projection.Kind.NAMED_EXPR, edgeDetailRecord, EDGE_DETAIL_NAME));

            // Return our new assembled projections.
            return projectionList;
        });
        this.edgeExpression = buildSelectWithSelectBlockStream(selectBlockList.stream());

        // Build the VERTEX-RECORD(s) associated with the sub-path.
        this.internalVertices = new ArrayList<>();
        edgePatternExpr.getInternalVertices().stream()
                .map(v -> new VertexRecord(v, graphIdentifier, lowerSupplierContext)).forEach(internalVertices::add);
    }

    @Override
    public Expression getExpression() {
        return edgeExpression;
    }

    public List<GraphElementIdentifier> getElementIdentifiers() {
        return elementIdentifiers;
    }

    public VariableExpr getVarExpr() {
        return edgeVar;
    }

    public EdgePatternExpr getEdgePatternExpr() {
        return edgePatternExpr;
    }

    public List<VertexRecord> getInternalVertices() {
        return internalVertices;
    }
}
