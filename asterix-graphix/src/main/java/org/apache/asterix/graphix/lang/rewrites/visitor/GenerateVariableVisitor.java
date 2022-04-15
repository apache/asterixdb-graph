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

import java.util.function.Supplier;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.expression.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrites.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.GenerateColumnNameVisitor;

/**
 * Populate all unknown a) graph elements (vertices and edges), b) column names in SELECT-CLAUSEs, and c) GROUP-BY keys.
 */
public class GenerateVariableVisitor extends AbstractGraphixQueryVisitor {
    private final GenerateColumnNameVisitor generateColumnNameVisitor;
    private final Supplier<VarIdentifier> newVariableSupplier;

    public GenerateVariableVisitor(GraphixRewritingContext graphixRewritingContext) {
        generateColumnNameVisitor = new GenerateColumnNameVisitor(graphixRewritingContext.getLangRewritingContext());
        newVariableSupplier = graphixRewritingContext::getNewVariable;
    }

    @Override
    public Expression visit(SelectExpression selectExpression, ILangExpression arg) throws CompilationException {
        selectExpression.accept(generateColumnNameVisitor, arg);
        return super.visit(selectExpression, arg);
    }

    @Override
    public Expression visit(VertexPatternExpr vertexExpression, ILangExpression arg) throws CompilationException {
        if (vertexExpression.getVariableExpr() == null) {
            vertexExpression.setVariableExpr(new VariableExpr(newVariableSupplier.get()));
        }
        return super.visit(vertexExpression, arg);
    }

    @Override
    public Expression visit(EdgePatternExpr edgeExpression, ILangExpression arg) throws CompilationException {
        EdgeDescriptor edgeDescriptor = edgeExpression.getEdgeDescriptor();
        if (edgeDescriptor.getVariableExpr() == null) {
            edgeDescriptor.setVariableExpr(new VariableExpr(newVariableSupplier.get()));
        }
        return super.visit(edgeExpression, arg);
    }
}
