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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.graphix.lang.statement.GraphElementDecl;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.FieldBinding;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.sqlpp.clause.FromClause;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.lang.sqlpp.clause.Projection;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.SelectClause;
import org.apache.asterix.lang.sqlpp.clause.SelectElement;
import org.apache.asterix.lang.sqlpp.clause.SelectRegular;
import org.apache.asterix.lang.sqlpp.clause.SelectSetOperation;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.optype.SetOpType;
import org.apache.asterix.lang.sqlpp.struct.SetOperationInput;
import org.apache.asterix.lang.sqlpp.struct.SetOperationRight;
import org.apache.asterix.om.functions.BuiltinFunctions;

public final class LowerRewritingUtil {
    public static List<FieldAccessor> buildAccessorList(Expression startingExpr, List<List<String>> fieldNames) {
        List<FieldAccessor> fieldAccessors = new ArrayList<>();
        for (List<String> nestedField : fieldNames) {
            FieldAccessor workingAccessor = new FieldAccessor(startingExpr, new Identifier(nestedField.get(0)));
            for (String field : nestedField.subList(1, nestedField.size())) {
                workingAccessor = new FieldAccessor(workingAccessor, new Identifier(field));
            }
            fieldAccessors.add(workingAccessor);
        }
        return fieldAccessors;
    }

    public static FieldBinding buildFieldBindingFromFieldAccessor(FieldAccessor fieldAccessor) {
        LiteralExpr leftExpr = new LiteralExpr(new StringLiteral(fieldAccessor.getIdent().getValue()));
        return new FieldBinding(leftExpr, fieldAccessor);
    }

    public static SetOperationInput buildSetOpInputWithFrom(FromTerm term, List<Projection> projections,
            List<AbstractClause> letWhereClauseList) {
        FromClause fromClause = new FromClause(Collections.singletonList(term));

        // Build up each FROM-CLAUSE as its own SET-OP-INPUT.
        SelectRegular selectRegular = new SelectRegular(projections);
        SelectClause selectClause = new SelectClause(null, selectRegular, false);
        SelectBlock selectBlock = new SelectBlock(selectClause, fromClause, letWhereClauseList, null, null);
        return new SetOperationInput(selectBlock, null);
    }

    public static SelectExpression buildSelectWithFromAndElement(FromTerm fromTerm,
            List<AbstractClause> letWhereClauses, Expression element) {
        FromClause fromClause = new FromClause(List.of(fromTerm));
        SelectElement selectElement = new SelectElement(element);
        SelectClause selectClause = new SelectClause(selectElement, null, false);
        SelectBlock selectBlock = new SelectBlock(selectClause, fromClause, letWhereClauses, null, null);
        SetOperationInput setOperationInput = new SetOperationInput(selectBlock, null);
        SelectSetOperation selectSetOperation = new SelectSetOperation(setOperationInput, null);
        return new SelectExpression(null, selectSetOperation, null, null, true);
    }

    public static LetClause buildLetClauseWithFromList(List<FromTerm> fromTerms, List<AbstractClause> letWhereClauses,
            List<Projection> projections, VariableExpr bindingLetVar) {
        // Build up a LET-CLAUSE from our input FROM-TERM list. Use a SELECT-ELEMENT instead of a SELECT-REGULAR.
        FromClause fromClause = new FromClause(fromTerms);
        SelectRegular selectRegular = new SelectRegular(projections);
        SelectClause selectClause = new SelectClause(null, selectRegular, false);
        SelectBlock selectBlock = new SelectBlock(selectClause, fromClause, letWhereClauses, null, null);
        SetOperationInput setOperationInput = new SetOperationInput(selectBlock, null);
        SelectSetOperation selectSetOperation = new SelectSetOperation(setOperationInput, null);
        SelectExpression selectExpression = new SelectExpression(null, selectSetOperation, null, null, true);
        return new LetClause(bindingLetVar, selectExpression);
    }

    public static List<SelectBlock> buildSelectBlocksWithDecls(List<GraphElementDecl> graphElementDecls,
            VariableExpr startingVar, Function<GraphElementDecl, List<Projection>> detailSupplier) {
        // Each declaration body will act as its own SELECT-BLOCK.
        List<SelectBlock> selectBlockList = new ArrayList<>();
        for (GraphElementDecl graphElementDecl : graphElementDecls) {
            for (Expression normalizedBody : graphElementDecl.getNormalizedBodies()) {
                FromTerm fromTerm = new FromTerm(normalizedBody, startingVar, null, null);
                FromClause fromClause = new FromClause(Collections.singletonList(fromTerm));

                // All details should be given to us as another set of projections.
                Expression selectElementExpr = detailSupplier.apply(graphElementDecl).stream().sequential()
                        .reduce(startingVar, (Expression inputExpr, Projection nextProjection) -> {
                            FunctionSignature functionSignature = new FunctionSignature(BuiltinFunctions.RECORD_ADD);
                            LiteralExpr fieldNameExpr = new LiteralExpr(new StringLiteral(nextProjection.getName()));
                            Expression fieldValueExpr = nextProjection.getExpression();
                            List<Expression> callArgumentList = List.of(inputExpr, fieldNameExpr, fieldValueExpr);
                            return new CallExpr(functionSignature, callArgumentList);
                        }, (a, b) -> b);
                SelectElement selectElement = new SelectElement(selectElementExpr);

                // Build up each FROM-CLAUSE as its own SELECT-BLOCK.
                SelectClause selectClause = new SelectClause(selectElement, null, false);
                SelectBlock selectBlock = new SelectBlock(selectClause, fromClause, null, null, null);
                selectBlockList.add(selectBlock);
            }
        }

        return selectBlockList;
    }

    public static SelectExpression buildSelectWithSelectBlockStream(Stream<SelectBlock> selectBlockStream) {
        // Build our parts, with each SELECT-BLOCK acting as its own SET-OPERATION-INPUT.
        List<SetOperationInput> selectExprParts =
                selectBlockStream.map(b -> new SetOperationInput(b, null)).collect(Collectors.toList());

        // Next, build our SELECT-SET-OPERATION. The first part will act as our leading term.
        SetOperationInput leadingPart = selectExprParts.get(0);
        List<SetOperationRight> rightPart = new ArrayList<>();
        if (selectExprParts.size() > 1) {
            for (SetOperationInput trailingPart : selectExprParts.subList(1, selectExprParts.size())) {
                rightPart.add(new SetOperationRight(SetOpType.UNION, false, trailingPart));
            }
        }
        SelectSetOperation selectExprComposite = new SelectSetOperation(leadingPart, rightPart);

        // Finally, build our SelectExpression.
        return new SelectExpression(null, selectExprComposite, null, null, true);
    }
}
