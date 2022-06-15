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
package org.apache.asterix.graphix.lang.rewrites.visitor;

import java.util.Map;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.graphix.function.GraphixFunctionIdentifiers;
import org.apache.asterix.graphix.function.GraphixFunctionMap;
import org.apache.asterix.graphix.function.GraphixFunctionResolver;
import org.apache.asterix.graphix.function.rewrite.IFunctionRewrite;
import org.apache.asterix.graphix.lang.rewrites.GraphixRewritingContext;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppSimpleExpressionVisitor;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

/**
 * Replace all Graphix-specific function calls with a SQL++ expression.
 *
 * @see GraphixFunctionIdentifiers
 * @see GraphixFunctionResolver
 * @see GraphixFunctionMap
 */
public class GraphixFunctionCallVisitor extends AbstractSqlppSimpleExpressionVisitor {
    private final GraphixRewritingContext graphixRewritingContext;
    private final GraphixFunctionResolver graphixFunctionResolver;

    public GraphixFunctionCallVisitor(GraphixRewritingContext graphixRewritingContext) {
        this.graphixRewritingContext = graphixRewritingContext;

        // We want to make sure that user-defined functions take precedence over Graphix-defined functions.
        Map<FunctionSignature, FunctionDecl> declaredFunctions = graphixRewritingContext.getDeclaredFunctions();
        MetadataProvider metadataProvider = graphixRewritingContext.getMetadataProvider();
        this.graphixFunctionResolver = new GraphixFunctionResolver(metadataProvider, declaredFunctions);
    }

    @Override
    public Expression visit(CallExpr callExpr, ILangExpression arg) throws CompilationException {
        // Determine which Graphix function we need to resolve.
        FunctionSignature functionSignature = graphixFunctionResolver.resolve(callExpr, true);
        if (functionSignature == null || !functionSignature.getDataverseName().getCanonicalForm()
                .equals(GraphixFunctionIdentifiers.GRAPHIX_DV.getCanonicalForm())) {
            return super.visit(callExpr, arg);
        }

        // Condition on our function ID, and perform the rewrite.
        FunctionIdentifier functionIdentifier = functionSignature.createFunctionIdentifier();
        IFunctionRewrite functionRewrite = GraphixFunctionMap.getFunctionRewrite(functionIdentifier);
        Expression rewrittenCallArgExpr = functionRewrite.apply(graphixRewritingContext, callExpr);
        if (rewrittenCallArgExpr == null) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, callExpr.getSourceLocation(),
                    "Function " + functionIdentifier.getName() + " not implemented!");
        }
        return rewrittenCallArgExpr;
    }
}
