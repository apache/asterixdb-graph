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
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.clause.WhereClause;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.om.functions.BuiltinFunctions;

/**
 * Given the expression PATH_EDGES(myPathVar), rewrite this function to extract all edges from the given path.
 * 1. Create a field access to get the edge of our path record.
 * 2. Filter out unknown edges (may occur w/ dangling vertices in paths).
 * 2. Build up a SELECT-EXPR whose FROM-TERM is our path variable.
 */
public class PathEdgesFunctionRewrite implements IFunctionRewrite {
    @Override
    public Expression apply(GraphixRewritingContext graphixRewritingContext, List<Expression> callArguments)
            throws CompilationException {
        // Build the SELECT-EXPR. We want to get the edge from our path record.
        VariableExpr edgeVar = new VariableExpr(graphixRewritingContext.getNewVariable());
        FromTerm fromTerm = new FromTerm(callArguments.get(0), edgeVar, null, null);
        FunctionSignature isUnknownFunctionSignature = new FunctionSignature(BuiltinFunctions.IS_UNKNOWN);
        FunctionSignature notFunctionSignature = new FunctionSignature(BuiltinFunctions.NOT);
        FieldAccessor edgeAccess = new FieldAccessor(edgeVar, new Identifier(PathRecord.EDGE_FIELD_NAME));
        List<AbstractClause> whereClauses = List.of(new WhereClause(new CallExpr(notFunctionSignature,
                List.of(new CallExpr(isUnknownFunctionSignature, List.of(edgeAccess))))));
        return LowerRewritingUtil.buildSelectWithFromAndElement(fromTerm, whereClauses, edgeAccess);
    }
}
