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
import org.apache.asterix.graphix.function.prepare.AbstractElementPrepare;
import org.apache.asterix.graphix.lang.rewrite.GraphixRewritingContext;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

/**
 * For most function rewrites, we just need to access the "_GraphixSchemaIdentifier" field we attached during schema
 * enrichment (i.e. on "prepare").
 */
public class SchemaAccessRewrite implements IFunctionRewrite {
    protected final FunctionIdentifier workingFunctionIdentifier;
    protected final Identifier workingFunctionPrepareIdentifier;

    public SchemaAccessRewrite(FunctionIdentifier functionIdentifier, Identifier functionPrepareIdentifier) {
        this.workingFunctionIdentifier = functionIdentifier;
        this.workingFunctionPrepareIdentifier = functionPrepareIdentifier;
    }

    @Override
    public Expression apply(GraphixRewritingContext graphixRewritingContext, CallExpr callExpr)
            throws CompilationException {
        if (callExpr.getExprList().size() != 1) {
            throw new CompilationException(ErrorCode.ILLEGAL_FUNCTION_USE, callExpr.getSourceLocation(),
                    workingFunctionIdentifier.toString());
        }

        // Access our schema, then our element-label field.
        Identifier schemaIdentifier = AbstractElementPrepare.GRAPHIX_SCHEMA_IDENTIFIER;
        FieldAccessor schemaFieldAccessor = new FieldAccessor(callExpr.getExprList().get(0), schemaIdentifier);
        schemaFieldAccessor.setSourceLocation(callExpr.getSourceLocation());
        FieldAccessor finalFieldAccessor = new FieldAccessor(schemaFieldAccessor, workingFunctionPrepareIdentifier);
        finalFieldAccessor.setSourceLocation(callExpr.getSourceLocation());
        return finalFieldAccessor;
    }
}
