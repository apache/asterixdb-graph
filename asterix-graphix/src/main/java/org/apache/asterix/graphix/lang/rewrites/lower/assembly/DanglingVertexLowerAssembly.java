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
package org.apache.asterix.graphix.lang.rewrites.lower.assembly;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.asterix.graphix.lang.expression.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrites.lower.LowerSupplierContext;
import org.apache.asterix.graphix.lang.rewrites.record.VertexRecord;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.TrueLiteral;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.lang.sqlpp.clause.JoinClause;
import org.apache.asterix.lang.sqlpp.clause.Projection;
import org.apache.asterix.lang.sqlpp.optype.JoinType;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;

/**
 * Lower node for handling vertices that are not attached to an edge.
 * - The output FROM-TERM is a cross-product of all dangling vertices.
 * - The output projections are the variable expressions associated with each vertex.
 */
public class DanglingVertexLowerAssembly extends AbstractLowerAssembly {
    public DanglingVertexLowerAssembly(LowerSupplierContext lowerSupplierContext) {
        super(lowerSupplierContext);
    }

    @Override
    protected FromTerm buildOutputFromTerm() {
        List<Expression> vertexExpressions = lowerSupplierContext.getDanglingVertices().stream()
                .map(v -> new VertexRecord(v, graphIdentifier, lowerSupplierContext)).map(VertexRecord::getExpression)
                .collect(Collectors.toList());

        // Our FROM-TERM will start with the first defined vertex.
        Expression leadingVertexExpression = vertexExpressions.get(0);
        VariableExpr leadingVariable = lowerSupplierContext.getDanglingVertices().get(0).getVariableExpr();
        FromTerm fromTerm = new FromTerm(leadingVertexExpression,
                new VariableExpr(new VarIdentifier(leadingVariable.getVar())), null, null);

        // Perform a CROSS-JOIN with all subsequent dangling vertices.
        List<AbstractBinaryCorrelateClause> correlateClauses = fromTerm.getCorrelateClauses();
        for (int i = 1; i < vertexExpressions.size(); i++) {
            VertexPatternExpr followingVertexPattern = lowerSupplierContext.getDanglingVertices().get(i);
            VariableExpr followingVariable = followingVertexPattern.getVariableExpr();
            correlateClauses.add(new JoinClause(JoinType.INNER, vertexExpressions.get(i),
                    new VariableExpr(new VarIdentifier(followingVariable.getVar())), null,
                    new LiteralExpr(TrueLiteral.INSTANCE), null));
        }

        return fromTerm;
    }

    @Override
    protected List<Projection> buildOutputProjections() {
        return lowerSupplierContext.getDanglingVertices().stream().map(v -> {
            String projectionName = SqlppVariableUtil.toUserDefinedName(v.getVariableExpr().getVar().getValue());
            VariableExpr variableExpr = new VariableExpr(v.getVariableExpr().getVar());
            return new Projection(Projection.Kind.NAMED_EXPR, variableExpr, projectionName);
        }).collect(Collectors.toList());
    }
}
