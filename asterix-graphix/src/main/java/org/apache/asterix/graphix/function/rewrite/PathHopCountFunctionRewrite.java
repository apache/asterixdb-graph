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
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.graphix.lang.rewrites.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.rewrites.record.PathRecord;
import org.apache.asterix.graphix.lang.rewrites.util.LowerRewritingUtil;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.clause.WhereClause;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.IntegerLiteral;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.om.functions.BuiltinFunctions;

/**
 * Given the expression PATH_HOP_COUNT(myPathVar), rewrite this function to return a count of edges in the given path.
 * 1. First, create a field access to get the edge of our path record.
 * 2. Next, filter out all unknown edges of our path. An path-edge can be unknown if a dangling vertex exists.
 * 3. Build up a SELECT-EXPR whose FROM-TERM is our path variable.
 * 4. Finally, return the LENGTH of the SELECT-EXPR list.
 */
public class PathHopCountFunctionRewrite implements IFunctionRewrite {
    @Override
    public Expression apply(GraphixRewritingContext graphixRewritingContext, List<Expression> callArguments)
            throws CompilationException {
        // We only want edges that are not unknown.
        VariableExpr edgeVar = new VariableExpr(graphixRewritingContext.getNewVariable());
        FromTerm fromTerm = new FromTerm(callArguments.get(0), edgeVar, null, null);
        FieldAccessor edgeAccess = new FieldAccessor(edgeVar, new Identifier(PathRecord.EDGE_FIELD_NAME));
        FunctionSignature isUnknownFunctionSignature = new FunctionSignature(BuiltinFunctions.IS_UNKNOWN);
        FunctionSignature notFunctionSignature = new FunctionSignature(BuiltinFunctions.NOT);
        CallExpr callExpr = new CallExpr(notFunctionSignature,
                List.of(new CallExpr(isUnknownFunctionSignature, List.of(edgeAccess))));
        SelectExpression selectExpression = LowerRewritingUtil.buildSelectWithFromAndElement(fromTerm,
                List.of(new WhereClause(callExpr)), new LiteralExpr(new IntegerLiteral(1)));

        // Our SELECT returns a list. Count the length of this list.
        FunctionSignature lengthFunctionSignature = new FunctionSignature(BuiltinFunctions.LEN);
        return new CallExpr(lengthFunctionSignature, List.of(selectExpression));
    }
}
