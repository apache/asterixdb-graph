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

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.graphix.function.GraphixFunctionIdentifiers;
import org.apache.asterix.graphix.lang.rewrite.GraphixRewritingContext;
import org.apache.asterix.graphix.type.MaterializePathTypeComputer;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.struct.Identifier;

public class PathEdgesRewrite implements IFunctionRewrite {
    @Override
    public Expression apply(GraphixRewritingContext graphixRewritingContext, CallExpr callExpr)
            throws CompilationException {
        if (callExpr.getExprList().size() != 1) {
            throw new CompilationException(ErrorCode.ILLEGAL_FUNCTION_USE, callExpr.getSourceLocation(),
                    GraphixFunctionIdentifiers.PATH_EDGES.toString());
        }
        Identifier pathEdgeIdentifier = new Identifier(MaterializePathTypeComputer.EDGES_FIELD_NAME);
        FieldAccessor pathEdgeAccess = new FieldAccessor(callExpr.getExprList().get(0), pathEdgeIdentifier);
        pathEdgeAccess.setSourceLocation(callExpr.getSourceLocation());
        return pathEdgeAccess;
    }
}
