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
package org.apache.asterix.graphix.lang.rewrites.util;

import static org.apache.asterix.graphix.lang.rewrites.util.ClauseRewritingUtil.buildConnectedClauses;
import static org.apache.asterix.graphix.lang.rewrites.util.LowerRewritingUtil.buildAccessorList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.asterix.graphix.common.metadata.GraphElementIdentifier;
import org.apache.asterix.graphix.lang.rewrites.common.ElementLookupTable;
import org.apache.asterix.graphix.lang.rewrites.lower.LowerSupplierNode;
import org.apache.asterix.graphix.lang.rewrites.record.IElementRecord;
import org.apache.asterix.graphix.lang.struct.ElementLabel;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.OperatorExpr;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.common.literal.TrueLiteral;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.OperatorType;
import org.apache.asterix.lang.sqlpp.clause.Projection;

public final class EdgeRewritingUtil {
    public static Expression buildVertexEdgeJoin(List<GraphElementIdentifier> vertexIdentifiers,
            List<GraphElementIdentifier> edgeIdentifiers,
            Function<GraphElementIdentifier, List<Expression>> edgeLabelPredicateBuilder,
            BiFunction<GraphElementIdentifier, GraphElementIdentifier, Boolean> elementRestrictionPredicate,
            Function<GraphElementIdentifier, List<FieldAccessor>> vertexKeyBuilder,
            Function<GraphElementIdentifier, List<List<FieldAccessor>>> edgeKeysBuilder) {
        // Connect multiple vertex labels to multiple edge bodies of multiple labels.
        Expression intermediateClause;
        List<Expression> finalClauses = new ArrayList<>();
        for (GraphElementIdentifier edgeIdentifier : edgeIdentifiers) {

            // Connect multiple vertex labels to multiple edge bodies of a single label.
            List<Expression> multipleVertexToMultipleEdgeClauses = new ArrayList<>();
            List<List<FieldAccessor>> edgeKeyList = edgeKeysBuilder.apply(edgeIdentifier);
            for (List<FieldAccessor> edgeKey : edgeKeyList) {

                // Connect multiple vertex labels to a single edge body of a single label.
                List<Expression> multipleVertexToSingleEdgeClauses = new ArrayList<>();
                for (GraphElementIdentifier vertexIdentifier : vertexIdentifiers) {
                    if (!elementRestrictionPredicate.apply(vertexIdentifier, edgeIdentifier)) {
                        continue;
                    }

                    // Connect a single vertex label to a single edge body of a single label.
                    List<Expression> singleVertexToSingleEdgeClauses = new ArrayList<>();
                    List<FieldAccessor> vertexKey = vertexKeyBuilder.apply(vertexIdentifier);
                    for (int i = 0; i < edgeKey.size(); i++) {
                        OperatorExpr equalityExpr = new OperatorExpr(List.of(edgeKey.get(i), vertexKey.get(i)),
                                Collections.singletonList(OperatorType.EQ), false);
                        singleVertexToSingleEdgeClauses.add(equalityExpr);
                    }
                    singleVertexToSingleEdgeClauses.addAll(edgeLabelPredicateBuilder.apply(edgeIdentifier));

                    intermediateClause = buildConnectedClauses(singleVertexToSingleEdgeClauses, OperatorType.AND);
                    multipleVertexToSingleEdgeClauses.add(intermediateClause);
                }
                if (multipleVertexToSingleEdgeClauses.isEmpty()) {
                    // If we have no vertices-to-edge clauses, then our intermediate clause is TRUE.
                    multipleVertexToMultipleEdgeClauses.add(new LiteralExpr(TrueLiteral.INSTANCE));

                } else {
                    intermediateClause = buildConnectedClauses(multipleVertexToSingleEdgeClauses, OperatorType.OR);
                    multipleVertexToMultipleEdgeClauses.add(intermediateClause);
                }
            }

            intermediateClause = buildConnectedClauses(multipleVertexToMultipleEdgeClauses, OperatorType.OR);
            finalClauses.add(intermediateClause);
        }

        return buildConnectedClauses(finalClauses, OperatorType.OR);
    }

    public static Expression buildInputAssemblyJoin(LowerSupplierNode inputNode, Expression startingAccessExpr,
            Expression qualifiedVar, Expression existingJoin, Function<String, Boolean> joinFieldPredicate) {
        boolean isAnyProjectionApplicable =
                inputNode.getProjectionList().stream().map(Projection::getName).anyMatch(joinFieldPredicate::apply);

        if (!isAnyProjectionApplicable && existingJoin == null) {
            // We cannot join with any of the input projections. Perform a cross product.
            return new LiteralExpr(TrueLiteral.INSTANCE);

        } else if (!isAnyProjectionApplicable) {
            // We cannot join with any of the input projections. Return the existing join.
            return existingJoin;

        } else {
            // We can perform a join with one (or more) of our input projections.
            List<Expression> joinsWithInput = new ArrayList<>();
            if (existingJoin != null) {
                joinsWithInput.add(existingJoin);
            }

            for (Projection inputProjection : inputNode.getProjectionList()) {
                String projectedVarName = inputProjection.getName();
                if (joinFieldPredicate.apply(projectedVarName)) {
                    List<List<String>> projectedFieldName = List.of(List.of(projectedVarName));
                    FieldAccessor inputAccessExpr = buildAccessorList(startingAccessExpr, projectedFieldName).get(0);
                    List<OperatorType> joinOperator = Collections.singletonList(OperatorType.EQ);
                    joinsWithInput.add(new OperatorExpr(List.of(qualifiedVar, inputAccessExpr), joinOperator, false));
                }
            }
            return buildConnectedClauses(joinsWithInput, OperatorType.AND);
        }
    }

    public static Function<GraphElementIdentifier, List<List<FieldAccessor>>> buildEdgeKeyBuilder(
            Expression startingExpr, ElementLookupTable<GraphElementIdentifier> elementLookupTable,
            Function<GraphElementIdentifier, List<List<String>>> keyFieldBuilder) {
        return identifier -> {
            List<List<FieldAccessor>> fieldAccessorList = new ArrayList<>();
            for (int i = 0; i < elementLookupTable.getEdgeSourceKeys(identifier).size(); i++) {
                List<List<String>> keyField = keyFieldBuilder.apply(identifier);
                fieldAccessorList.add(buildAccessorList(startingExpr, keyField));
            }
            return fieldAccessorList;
        };
    }

    public static Function<GraphElementIdentifier, List<Expression>> buildEdgeLabelPredicateBuilder(
            Expression vertexExpr, Expression edgeExpr, Function<GraphElementIdentifier, ElementLabel> labelGetter) {
        return identifier -> {
            final Identifier elementDetailSuffix = new Identifier(IElementRecord.ELEMENT_DETAIL_NAME);
            final Identifier elementLabelSuffix = new Identifier(IElementRecord.ELEMENT_LABEL_FIELD_NAME);

            // Access the labels in our vertex and our edge.
            LiteralExpr vertexValue = new LiteralExpr(new StringLiteral(labelGetter.apply(identifier).toString()));
            LiteralExpr edgeValue = new LiteralExpr(new StringLiteral(identifier.getElementLabel().toString()));
            FieldAccessor vertexDetailAccess = new FieldAccessor(vertexExpr, elementDetailSuffix);
            FieldAccessor edgeDetailAccess = new FieldAccessor(edgeExpr, elementDetailSuffix);
            FieldAccessor vertexLabelAccess = new FieldAccessor(vertexDetailAccess, elementLabelSuffix);
            FieldAccessor edgeLabelAccess = new FieldAccessor(edgeDetailAccess, elementLabelSuffix);

            // Ensure that our vertex label and our edge label match.
            List<Expression> labelPredicates = new ArrayList<>();
            List<Expression> vertexExprList = List.of(vertexValue, vertexLabelAccess);
            List<Expression> edgeExprList = List.of(edgeValue, edgeLabelAccess);
            labelPredicates.add(new OperatorExpr(vertexExprList, List.of(OperatorType.EQ), false));
            labelPredicates.add(new OperatorExpr(edgeExprList, List.of(OperatorType.EQ), false));
            return labelPredicates;
        };
    }
}
