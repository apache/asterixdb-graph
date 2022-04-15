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

import static org.apache.asterix.graphix.lang.rewrites.util.LowerRewritingUtil.buildSelectWithFromAndElement;

import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.graphix.lang.rewrites.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.rewrites.record.PathRecord;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.clause.WhereClause;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.ListConstructor;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.lang.sqlpp.clause.UnnestClause;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.optype.UnnestType;
import org.apache.asterix.om.functions.BuiltinFunctions;

/**
 * Given the expression PATH_VERTICES(myPathVar), rewrite this function to extract all vertices from the given path.
 * 1. First, we create a FROM-TERM from the given call argument. We assume that this is a path variable.
 * 2. Next, we build an ordered-list expression of accesses to the left vertex and the right vertex of our path record.
 * 3. UNNEST this ordered-list expression to get the vertex values, and mark the associated SELECT-EXPR as DISTINCT to
 * remove duplicates vertices. Building this ordered-list and then UNNESTing said list is required to get a flat list of
 * vertices.
 *
 * TODO (GLENN): We can definitely remove the "DISTINCT" and "UNNEST", but this is logically equivalent.
 */
public class PathVerticesFunctionRewrite implements IFunctionRewrite {
    @Override
    public Expression apply(GraphixRewritingContext graphixRewritingContext, List<Expression> callArguments)
            throws CompilationException {
        // Access the left and right vertices of our edge record.
        VariableExpr edgeVar = new VariableExpr(graphixRewritingContext.getNewVariable());
        FieldAccessor leftVertex = new FieldAccessor(edgeVar, new Identifier(PathRecord.LEFT_VERTEX_FIELD_NAME));
        FieldAccessor rightVertex = new FieldAccessor(edgeVar, new Identifier(PathRecord.RIGHT_VERTEX_FIELD_NAME));

        // Build a list of vertices, and UNNEST this list to get a flat list of vertices.
        ListConstructor listConstructor =
                new ListConstructor(ListConstructor.Type.ORDERED_LIST_CONSTRUCTOR, List.of(leftVertex, rightVertex));
        VariableExpr vertexVar = new VariableExpr(graphixRewritingContext.getNewVariable());
        UnnestClause edgeUnnest = new UnnestClause(UnnestType.INNER, listConstructor, vertexVar, null, null);

        // Ensure that we filter out unknown vertices (can occur with dangling vertices).
        FunctionSignature isUnknownFunctionSignature = new FunctionSignature(BuiltinFunctions.IS_UNKNOWN);
        FunctionSignature notFunctionSignature = new FunctionSignature(BuiltinFunctions.NOT);
        List<AbstractClause> whereClauses = List.of(new WhereClause(new CallExpr(notFunctionSignature,
                List.of(new CallExpr(isUnknownFunctionSignature, List.of(vertexVar))))));
        FromTerm fromTerm = new FromTerm(callArguments.get(0), edgeVar, null, List.of(edgeUnnest));

        // Build the SELECT-EXPR. We will also mark this SELECT-CLAUSE as DISTINCT.
        SelectExpression selectExpr = buildSelectWithFromAndElement(fromTerm, whereClauses, vertexVar);
        selectExpr.getSelectSetOperation().getLeftInput().getSelectBlock().getSelectClause().setDistinct(true);
        return selectExpr;
    }
}
