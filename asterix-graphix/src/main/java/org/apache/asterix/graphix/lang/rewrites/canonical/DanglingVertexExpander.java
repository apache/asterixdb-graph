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
package org.apache.asterix.graphix.lang.rewrites.canonical;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.clause.GraphSelectBlock;
import org.apache.asterix.graphix.lang.clause.MatchClause;
import org.apache.asterix.graphix.lang.expression.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.PathPatternExpr;
import org.apache.asterix.graphix.lang.expression.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrites.visitor.GraphixDeepCopyVisitor;
import org.apache.asterix.graphix.lang.struct.ElementLabel;
import org.apache.asterix.lang.common.expression.VariableExpr;

/**
 * Expand a disconnected vertex into a set of canonical disconnected vertices. For dangling vertices, this just
 * includes ensuring that each vertex has only one label.
 */
public class DanglingVertexExpander implements ICanonicalExpander<VertexPatternExpr> {
    private final GraphixDeepCopyVisitor deepCopyVisitor = new GraphixDeepCopyVisitor();

    @Override
    public void apply(VertexPatternExpr vertexPatternExpr, List<GraphSelectBlock> inputSelectBlocks)
            throws CompilationException {
        if (vertexPatternExpr.getLabels().size() == 1) {
            // No expansion is necessary. Our vertex is in canonical form.
            return;
        }
        VariableExpr originalVariableExpr = vertexPatternExpr.getVariableExpr();

        // We want to end up with |GSBs| * |labels| number of generated GSBs.
        List<GraphSelectBlock> generatedGraphSelectBlocks = new ArrayList<>();
        for (GraphSelectBlock oldGraphSelectBlock : inputSelectBlocks) {
            for (ElementLabel vertexLabel : vertexPatternExpr.getLabels()) {
                GraphSelectBlock clonedSelectBlock = deepCopyVisitor.visit(oldGraphSelectBlock, null);
                for (MatchClause matchClause : clonedSelectBlock.getFromGraphClause().getMatchClauses()) {
                    for (PathPatternExpr pathExpression : matchClause.getPathExpressions()) {
                        VariableExpr newVertexVariableExpr = deepCopyVisitor.visit(originalVariableExpr, null);
                        VertexPatternExpr newVertexPatternExpr =
                                new VertexPatternExpr(newVertexVariableExpr, Set.of(vertexLabel));
                        replaceVertexExpression(pathExpression.getVertexExpressions(),
                                pathExpression.getEdgeExpressions(), vertexPatternExpr, newVertexPatternExpr);
                    }
                }
                generatedGraphSelectBlocks.add(clonedSelectBlock);
            }
        }
        inputSelectBlocks.clear();
        inputSelectBlocks.addAll(generatedGraphSelectBlocks);
    }

    private static void replaceVertexExpression(List<VertexPatternExpr> vertexExpressions,
            List<EdgePatternExpr> edgeExpressions, VertexPatternExpr oldVertexExpression,
            VertexPatternExpr newVertexExpression) {
        ListIterator<VertexPatternExpr> vertexExpressionIterator = vertexExpressions.listIterator();
        while (vertexExpressionIterator.hasNext()) {
            VertexPatternExpr workingVertexExpression = vertexExpressionIterator.next();
            if (workingVertexExpression.equals(oldVertexExpression)) {
                vertexExpressionIterator.set(newVertexExpression);
                break;
            }
        }
        for (EdgePatternExpr workingEdgeExpression : edgeExpressions) {
            if (workingEdgeExpression.getLeftVertex().equals(oldVertexExpression)) {
                workingEdgeExpression.setLeftVertex(newVertexExpression);

            } else if (workingEdgeExpression.getRightVertex().equals(oldVertexExpression)) {
                workingEdgeExpression.setRightVertex(newVertexExpression);
            }
        }
    }
}
