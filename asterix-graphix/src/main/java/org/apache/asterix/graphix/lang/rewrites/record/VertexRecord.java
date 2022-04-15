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
import org.apache.asterix.graphix.lang.expression.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrites.common.ElementLookupTable;
import org.apache.asterix.graphix.lang.rewrites.lower.LowerSupplierContext;
import org.apache.asterix.graphix.lang.rewrites.util.LowerRewritingUtil;
import org.apache.asterix.graphix.lang.statement.GraphElementDecl;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.expression.FieldBinding;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.sqlpp.clause.Projection;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;

/**
 * Lowered representation of a query vertex, constructed with the following process:
 * 1. For each label of the vertex, perform a UNION-ALL of their respective normalized bodies.
 * 2. Build a SELECT-BLOCK for each vertex label, using the result of step 1. For each SELECT-CLAUSE, attach the vertex
 * label and access to the primary key, in addition to the expression from step 1.
 * 3. Now having a list of SELECT-BLOCKs, UNION-ALL each SELECT-BLOCK and build a single SELECT-EXPR. This is the
 * lowered representation of our vertex.
 */
public class VertexRecord implements IElementRecord {
    // The details surrounding our vertex will be included in the following field (alongside our body).
    public static final String VERTEX_DETAIL_NAME = "_GraphixVertexDetail";

    // We attach the following fields to the SELECT containing a declaration body.
    public static final String PRIMARY_KEY_FIELD_NAME = "PrimaryKey";

    private final List<GraphElementIdentifier> elementIdentifiers;
    private final VertexPatternExpr vertexPatternExpr;
    private final Expression vertexExpression;
    private final VariableExpr vertexVar;

    public VertexRecord(VertexPatternExpr vertexPatternExpr, GraphIdentifier graphIdentifier,
            LowerSupplierContext lowerSupplierContext) {
        this.elementIdentifiers = vertexPatternExpr.generateIdentifiers(graphIdentifier);
        this.vertexVar = new VariableExpr(vertexPatternExpr.getVariableExpr().getVar());
        this.vertexPatternExpr = vertexPatternExpr;

        // Build the SELECT-EXPR associated with the given element identifiers.
        ElementLookupTable<GraphElementIdentifier> elementLookupTable = lowerSupplierContext.getElementLookupTable();
        List<GraphElementDecl> graphElementDeclList =
                elementIdentifiers.stream().map(elementLookupTable::getElementDecl).collect(Collectors.toList());
        List<SelectBlock> selectBlockList = buildSelectBlocksWithDecls(graphElementDeclList, vertexVar, d -> {
            List<Projection> projectionList = new ArrayList<>();

            // Build the label binding for the element detail projection.
            List<FieldBinding> elementDetailBindings = new ArrayList<>();
            StringLiteral labelLiteral = new StringLiteral(d.getIdentifier().getElementLabel().toString());
            LiteralExpr labelFieldValue = new LiteralExpr(labelLiteral);
            LiteralExpr labelFieldName = new LiteralExpr(new StringLiteral(ELEMENT_LABEL_FIELD_NAME));
            elementDetailBindings.add(new FieldBinding(labelFieldName, labelFieldValue));
            RecordConstructor elementDetailRecord = new RecordConstructor(elementDetailBindings);
            projectionList.add(new Projection(Projection.Kind.NAMED_EXPR, elementDetailRecord, ELEMENT_DETAIL_NAME));

            // Build the vertex detail projection.
            List<FieldBinding> vertexDetailBindings = new ArrayList<>();
            List<FieldBinding> primaryKeyBindings =
                    buildAccessorList(vertexVar, elementLookupTable.getVertexKey(d.getIdentifier())).stream()
                            .map(LowerRewritingUtil::buildFieldBindingFromFieldAccessor).collect(Collectors.toList());
            RecordConstructor primaryKeyAccess = new RecordConstructor(primaryKeyBindings);
            LiteralExpr primaryKeyName = new LiteralExpr(new StringLiteral(PRIMARY_KEY_FIELD_NAME));
            vertexDetailBindings.add(new FieldBinding(primaryKeyName, primaryKeyAccess));
            RecordConstructor vertexDetailRecord = new RecordConstructor(vertexDetailBindings);
            projectionList.add(new Projection(Projection.Kind.NAMED_EXPR, vertexDetailRecord, VERTEX_DETAIL_NAME));

            // Return our newly assembled projections.
            return projectionList;
        });
        this.vertexExpression = buildSelectWithSelectBlockStream(selectBlockList.stream());
    }

    @Override
    public Expression getExpression() {
        return vertexExpression;
    }

    public List<GraphElementIdentifier> getElementIdentifiers() {
        return elementIdentifiers;
    }

    public VariableExpr getVarExpr() {
        return vertexVar;
    }

    public VertexPatternExpr getVertexPatternExpr() {
        return vertexPatternExpr;
    }
}
