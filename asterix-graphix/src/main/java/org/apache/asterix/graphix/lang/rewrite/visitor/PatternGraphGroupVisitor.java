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

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.common.metadata.GraphIdentifier;
import org.apache.asterix.graphix.lang.clause.FromGraphClause;
import org.apache.asterix.graphix.lang.clause.MatchClause;
import org.apache.asterix.graphix.lang.expression.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrite.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.struct.PatternGroup;
import org.apache.asterix.graphix.lang.visitor.base.AbstractGraphixQueryVisitor;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.asterix.metadata.declared.MetadataProvider;

/**
 * Aggregate all query patterns, grouped by the graph being queried.
 *
 * @see PatternGroup
 */
public class PatternGraphGroupVisitor extends AbstractGraphixQueryVisitor {
    private final Map<GraphIdentifier, PatternGroup> patternGroupMap;
    private final Deque<GraphIdentifier> graphIdentifierStack;
    protected final MetadataProvider metadataProvider;

    public PatternGraphGroupVisitor(Map<GraphIdentifier, PatternGroup> patternGroupMap,
            GraphixRewritingContext graphixRewritingContext) {
        this.metadataProvider = graphixRewritingContext.getMetadataProvider();
        this.graphIdentifierStack = new ArrayDeque<>();
        this.patternGroupMap = patternGroupMap;
    }

    @Override
    public Expression visit(FromGraphClause fromGraphClause, ILangExpression arg) throws CompilationException {
        // Collect the vertices and edges in this FROM-GRAPH-CLAUSE.
        graphIdentifierStack.push(fromGraphClause.getGraphIdentifier(metadataProvider));
        for (MatchClause matchClause : fromGraphClause.getMatchClauses()) {
            matchClause.accept(this, arg);
        }
        for (AbstractBinaryCorrelateClause correlateClause : fromGraphClause.getCorrelateClauses()) {
            correlateClause.accept(this, arg);
        }
        graphIdentifierStack.pop();
        return null;
    }

    @Override
    public Expression visit(VertexPatternExpr vertexPatternExpr, ILangExpression arg) throws CompilationException {
        patternGroupMap.putIfAbsent(graphIdentifierStack.peek(), new PatternGroup());
        patternGroupMap.get(graphIdentifierStack.peek()).getVertexPatternSet().add(vertexPatternExpr);
        return super.visit(vertexPatternExpr, arg);
    }

    // We do not visit our internal vertices here.
    @Override
    public Expression visit(EdgePatternExpr edgePatternExpr, ILangExpression arg) throws CompilationException {
        patternGroupMap.putIfAbsent(graphIdentifierStack.peek(), new PatternGroup());
        patternGroupMap.get(graphIdentifierStack.peek()).getEdgePatternSet().add(edgePatternExpr);
        if (edgePatternExpr.getEdgeDescriptor().getFilterExpr() != null) {
            edgePatternExpr.getEdgeDescriptor().getFilterExpr().accept(this, arg);
        }
        return edgePatternExpr;
    }
}
