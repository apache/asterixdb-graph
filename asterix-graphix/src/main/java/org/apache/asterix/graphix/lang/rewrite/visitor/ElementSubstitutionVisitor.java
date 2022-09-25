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
package org.apache.asterix.graphix.lang.rewrite.visitor;

import java.util.List;
import java.util.ListIterator;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.clause.MatchClause;
import org.apache.asterix.graphix.lang.expression.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.PathPatternExpr;
import org.apache.asterix.graphix.lang.expression.VertexPatternExpr;
import org.apache.asterix.graphix.lang.visitor.base.AbstractGraphixQueryVisitor;
import org.apache.asterix.lang.common.base.AbstractExpression;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;

/**
 * Visitor to replace a {@link VertexPatternExpr}, a {@link EdgePatternExpr}, or a {@link PathPatternExpr}.
 */
public class ElementSubstitutionVisitor<T extends AbstractExpression> extends AbstractGraphixQueryVisitor {
    private final T replacementElementExpression;
    private final T originalElementExpression;

    public ElementSubstitutionVisitor(T replacementElementExpression, T originalElementExpression) {
        this.replacementElementExpression = replacementElementExpression;
        this.originalElementExpression = originalElementExpression;
        if (!((replacementElementExpression instanceof VertexPatternExpr)
                || (replacementElementExpression instanceof EdgePatternExpr)
                || (replacementElementExpression instanceof PathPatternExpr))) {
            throw new IllegalArgumentException(
                    "Only VertexPatternExpr, EdgePatternExpr, or PathPatternExpr " + "instances should be given.");
        }
    }

    @Override
    public Expression visit(MatchClause matchClause, ILangExpression arg) throws CompilationException {
        if (replacementElementExpression instanceof PathPatternExpr) {
            ListIterator<PathPatternExpr> pathIterator = matchClause.getPathExpressions().listIterator();
            while (pathIterator.hasNext()) {
                PathPatternExpr pathPatternExpr = pathIterator.next();
                if (pathPatternExpr.equals(originalElementExpression)) {
                    pathIterator.set((PathPatternExpr) replacementElementExpression);
                    break;
                }
            }
            return null;
        }
        return super.visit(matchClause, arg);
    }

    @Override
    public Expression visit(PathPatternExpr pathPatternExpr, ILangExpression arg) throws CompilationException {
        if (replacementElementExpression instanceof VertexPatternExpr) {
            ListIterator<VertexPatternExpr> vertexExprIterator = pathPatternExpr.getVertexExpressions().listIterator();
            while (vertexExprIterator.hasNext()) {
                VertexPatternExpr workingVertexExpression = vertexExprIterator.next();
                if (workingVertexExpression.equals(originalElementExpression)) {
                    vertexExprIterator.set((VertexPatternExpr) replacementElementExpression);
                    break;
                }
            }
            List<EdgePatternExpr> edgeExpressions = pathPatternExpr.getEdgeExpressions();
            for (EdgePatternExpr workingEdgeExpression : edgeExpressions) {
                if (workingEdgeExpression.getLeftVertex().equals(originalElementExpression)) {
                    workingEdgeExpression.setLeftVertex((VertexPatternExpr) replacementElementExpression);
                }
                if (workingEdgeExpression.getRightVertex().equals(originalElementExpression)) {
                    workingEdgeExpression.setRightVertex((VertexPatternExpr) replacementElementExpression);
                }
            }

        } else { // replacementElementExpression instanceof EdgePatternExpr
            ListIterator<EdgePatternExpr> edgeExprIterator = pathPatternExpr.getEdgeExpressions().listIterator();
            while (edgeExprIterator.hasNext()) {
                EdgePatternExpr workingEdgeExpression = edgeExprIterator.next();
                if (workingEdgeExpression.equals(originalElementExpression)) {
                    edgeExprIterator.set((EdgePatternExpr) replacementElementExpression);
                    break;
                }
            }
        }
        return pathPatternExpr;
    }
}
