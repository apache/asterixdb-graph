/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.graphix.lang.rewrites.visitor;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.graphix.function.GraphixFunctionResolver;
import org.apache.asterix.graphix.lang.rewrites.GraphixRewritingContext;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.sqlpp.expression.WindowExpression;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.SqlppFunctionCallResolverVisitor;

/**
 * Resolve all function calls, while accounting for Graphix specific functions. This is meant to replace the class
 * {@link org.apache.asterix.lang.sqlpp.rewrites.visitor.SqlppFunctionCallResolverVisitor}, and should contain all
 * the functionality exposed there.
 */
public class FunctionResolutionVisitor extends AbstractGraphixQueryVisitor {
    private final SqlppFunctionCallResolverVisitor sqlppFunctionCallResolverVisitor;
    private final GraphixFunctionResolver graphixFunctionResolver;
    private final boolean allowNonStoredUdfCalls;

    public FunctionResolutionVisitor(GraphixRewritingContext graphixRewritingContext, boolean allowNonStoredUDFCalls) {
        this.sqlppFunctionCallResolverVisitor =
                new SqlppFunctionCallResolverVisitor(graphixRewritingContext, allowNonStoredUDFCalls);
        this.graphixFunctionResolver = new GraphixFunctionResolver(graphixRewritingContext.getMetadataProvider(),
                graphixRewritingContext.getDeclaredFunctions());
        this.allowNonStoredUdfCalls = allowNonStoredUDFCalls;
    }

    @Override
    public Expression visit(CallExpr callExpr, ILangExpression arg) throws CompilationException {
        FunctionSignature functionSignature = graphixFunctionResolver.resolve(callExpr, allowNonStoredUdfCalls);
        if (functionSignature != null) {
            callExpr.setFunctionSignature(functionSignature);
        }
        return super.visit(callExpr, arg);
    }

    @Override
    public Expression visit(WindowExpression windowExpression, ILangExpression arg) throws CompilationException {
        // Delegate WINDOW function resolution to our SQL++ resolver.
        return sqlppFunctionCallResolverVisitor.visit(windowExpression, arg);
    }
}
