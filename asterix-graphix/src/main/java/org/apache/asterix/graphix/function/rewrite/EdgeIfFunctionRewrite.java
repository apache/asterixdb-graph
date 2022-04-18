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
package org.apache.asterix.graphix.function.rewrite;

import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.rewrites.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.rewrites.record.EdgeRecord;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.OperatorExpr;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.common.literal.TrueLiteral;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.OperatorType;
import org.apache.asterix.lang.sqlpp.expression.CaseExpression;

/**
 * Given the expression EDGE*_IF(myEdgeVar, expr1, expr2), rewrite this function to return either expression (depending
 * on our owner).
 * 1. Access our edge direction.
 * 2. Build two conditions: one where our edge direction is equal to LEFT_TO_RIGHT, and another where our edge direction
 * is equal to RIGHT_TO_LEFT.
 * 3. Build our case statement.
 */
public class EdgeIfFunctionRewrite implements IFunctionRewrite {
    private final boolean isSourceVertex;

    public EdgeIfFunctionRewrite(boolean isSourceRewrite) {
        this.isSourceVertex = isSourceRewrite;
    }

    @Override
    public Expression apply(GraphixRewritingContext graphixRewritingContext, List<Expression> callArguments)
            throws CompilationException {
        // Fetch our edge direction.
        final Identifier detailSuffix = new Identifier(EdgeRecord.EDGE_DETAIL_NAME);
        final Identifier directionSuffix = new Identifier(EdgeRecord.DIRECTION_FIELD_NAME);
        FieldAccessor edgeDetailAccess = new FieldAccessor(callArguments.get(0), detailSuffix);
        FieldAccessor edgeDirAccess = new FieldAccessor(edgeDetailAccess, directionSuffix);

        // Create a LEFT_TO_RIGHT condition.
        LiteralExpr l2RLiteral = new LiteralExpr(new StringLiteral(EdgeDescriptor.EdgeType.LEFT_TO_RIGHT.name()));
        List<Expression> l2ROperands = List.of(edgeDirAccess, l2RLiteral);
        OperatorExpr l2RCondition = new OperatorExpr(l2ROperands, List.of(OperatorType.EQ), false);

        // Create a RIGHT_TO_LEFT condition.
        LiteralExpr r2LLiteral = new LiteralExpr(new StringLiteral(EdgeDescriptor.EdgeType.RIGHT_TO_LEFT.name()));
        List<Expression> r2LOperands = List.of(edgeDirAccess, r2LLiteral);
        OperatorExpr r2LCondition = new OperatorExpr(r2LOperands, List.of(OperatorType.EQ), false);

        // Build our CASE expression.
        List<Expression> thenExpressionList = isSourceVertex ? List.of(callArguments.get(1), callArguments.get(2))
                : List.of(callArguments.get(2), callArguments.get(1));
        return new CaseExpression(new LiteralExpr(TrueLiteral.INSTANCE), List.of(l2RCondition, r2LCondition),
                thenExpressionList, null);
    }
}
