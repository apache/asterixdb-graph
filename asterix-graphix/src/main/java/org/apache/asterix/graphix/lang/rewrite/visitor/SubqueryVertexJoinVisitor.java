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
package org.apache.asterix.graphix.lang.rewrite.visitor;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.common.metadata.GraphIdentifier;
import org.apache.asterix.graphix.lang.annotation.SubqueryVertexJoinAnnotation;
import org.apache.asterix.graphix.lang.clause.FromGraphClause;
import org.apache.asterix.graphix.lang.clause.MatchClause;
import org.apache.asterix.graphix.lang.expression.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.PathPatternExpr;
import org.apache.asterix.graphix.lang.expression.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrite.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.visitor.base.AbstractGraphixQueryVisitor;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.WhereClause;
import org.apache.asterix.lang.common.context.Scope;
import org.apache.asterix.lang.common.expression.OperatorExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.parser.ScopeChecker;
import org.apache.asterix.lang.common.struct.OperatorType;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.metadata.declared.MetadataProvider;

/**
 * Search for Graphix sub-queries that have 'free' vertices, and see if we can explicitly JOIN them with a vertex in
 * a parent query that refers to the same graph.
 */
public class SubqueryVertexJoinVisitor extends AbstractGraphixQueryVisitor {
    private final GraphixDeepCopyVisitor graphixDeepCopyVisitor;
    private final GraphixRewritingContext graphixRewritingContext;
    private final Map<GraphIdentifier, ScopeChecker> vertexScopeMap;
    private final Deque<GraphIdentifier> graphIdentifierStack;

    public SubqueryVertexJoinVisitor(GraphixRewritingContext graphixRewritingContext) {
        this.graphixDeepCopyVisitor = new GraphixDeepCopyVisitor();
        this.graphixRewritingContext = graphixRewritingContext;
        this.graphIdentifierStack = new ArrayDeque<>();
        this.vertexScopeMap = new HashMap<>();
    }

    @Override
    public Expression visit(SelectBlock selectBlock, ILangExpression arg) throws CompilationException {
        if (selectBlock.hasFromClause() && selectBlock.getFromClause() instanceof FromGraphClause) {
            FromGraphClause fromGraphClause = (FromGraphClause) selectBlock.getFromClause();
            MetadataProvider metadataProvider = graphixRewritingContext.getMetadataProvider();
            GraphIdentifier graphIdentifier = fromGraphClause.getGraphIdentifier(metadataProvider);
            vertexScopeMap.putIfAbsent(graphIdentifier, new ScopeChecker());
            graphIdentifierStack.push(graphIdentifier);
            vertexScopeMap.get(graphIdentifier).createNewScope();

            // We want to provide our SELECT-BLOCK to our MATCH-CLAUSE.
            super.visit(selectBlock, selectBlock);
            graphIdentifierStack.pop();
            vertexScopeMap.get(graphIdentifier).removeCurrentScope();

        } else {
            super.visit(selectBlock, arg);
        }
        return null;
    }

    @Override
    public Expression visit(MatchClause matchClause, ILangExpression arg) throws CompilationException {
        GraphIdentifier workingGraphIdentifier = graphIdentifierStack.peek();
        ScopeChecker workingScopeChecker = vertexScopeMap.get(workingGraphIdentifier);
        Scope precedingScope = workingScopeChecker.getPrecedingScope();
        Scope currentScope = workingScopeChecker.getCurrentScope();

        Map<VariableExpr, VariableExpr> remappedVariables = new HashMap<>();
        for (PathPatternExpr pathExpression : matchClause.getPathExpressions()) {
            for (VertexPatternExpr vertexExpression : pathExpression.getVertexExpressions()) {
                VariableExpr vertexVariable = vertexExpression.getVariableExpr();
                VarIdentifier vertexName = vertexVariable.getVar();
                if (precedingScope == null || precedingScope.getParentScope() == null) {
                    // We are in a top-level FROM-GRAPH-CLAUSE. Add this variable to our current scope.
                    currentScope.addSymbolToScope(vertexName);

                } else if (currentScope.findLocalSymbol(vertexName.getValue()) == null
                        && currentScope.findSymbol(vertexName.getValue()) != null) {
                    // We have a nested Graphix query, and we have found a vertex that references an outer vertex.
                    SelectBlock parentSelectBlock = (SelectBlock) arg;

                    // Replace the current vertex variable with a copy.
                    VariableExpr newVariable = graphixRewritingContext.getGraphixVariableCopy(vertexVariable);
                    vertexExpression.setVariableExpr(newVariable);
                    vertexExpression.getVariableExpr().setSourceLocation(vertexVariable.getSourceLocation());
                    vertexExpression.addHint(new SubqueryVertexJoinAnnotation(vertexVariable));
                    remappedVariables.put(vertexVariable, newVariable);

                    // JOIN our copy with the parent vertex variable.
                    List<Expression> joinArgs = List.of(vertexVariable, newVariable);
                    OperatorExpr joinExpr = new OperatorExpr(joinArgs, List.of(OperatorType.EQ), false);
                    parentSelectBlock.getLetWhereList().add(new WhereClause(joinExpr));

                } else if (currentScope.findLocalSymbol(vertexName.getValue()) == null) {
                    // We have a nested Graphix query that does not correlate with an outer query.
                    currentScope.addSymbolToScope(vertexName);

                } else if (remappedVariables.containsKey(vertexVariable)) {
                    // We have replaced this variable already. Update the reference.
                    VariableExpr remappedVertexVariable = remappedVariables.get(vertexVariable);
                    vertexExpression.setVariableExpr(graphixDeepCopyVisitor.visit(remappedVertexVariable, null));
                    vertexExpression.getVariableExpr().setSourceLocation(vertexVariable.getSourceLocation());
                    vertexExpression.addHint(new SubqueryVertexJoinAnnotation(vertexVariable));
                }
            }
            for (EdgePatternExpr edgeExpression : pathExpression.getEdgeExpressions()) {
                VertexPatternExpr leftVertex = edgeExpression.getLeftVertex();
                VertexPatternExpr rightVertex = edgeExpression.getRightVertex();
                if (remappedVariables.containsKey(leftVertex.getVariableExpr())) {
                    VariableExpr leftVariableRemap = remappedVariables.get(leftVertex.getVariableExpr());
                    leftVertex.setVariableExpr(graphixDeepCopyVisitor.visit(leftVariableRemap, null));
                    leftVertex.addHint(new SubqueryVertexJoinAnnotation(leftVariableRemap));
                }
                if (remappedVariables.containsKey(rightVertex.getVariableExpr())) {
                    VariableExpr rightVariableRemap = remappedVariables.get(rightVertex.getVariableExpr());
                    rightVertex.setVariableExpr(graphixDeepCopyVisitor.visit(rightVariableRemap, null));
                    rightVertex.addHint(new SubqueryVertexJoinAnnotation(rightVariableRemap));
                }
            }
        }
        return null;
    }
}
