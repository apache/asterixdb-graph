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

import static org.apache.asterix.graphix.function.GraphixFunctionIdentifiers.isEdgeFunction;
import static org.apache.asterix.graphix.function.GraphixFunctionIdentifiers.isVertexFunction;
import static org.apache.asterix.graphix.function.GraphixFunctionMap.getFunctionPrepare;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.common.metadata.GraphIdentifier;
import org.apache.asterix.graphix.function.prepare.IFunctionPrepare;
import org.apache.asterix.graphix.lang.clause.CorrLetClause;
import org.apache.asterix.graphix.lang.clause.GraphSelectBlock;
import org.apache.asterix.graphix.lang.clause.MatchClause;
import org.apache.asterix.graphix.lang.expression.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrites.common.ElementLookupTable;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

/**
 * Perform a pass to enrich any {@link CorrLetClause} nodes with necessary schema information. Note that this
 * process may overestimate the amount of enrichment actually required (to minimize this difference requires some form
 * of equivalence classes @ the rewriter level).
 */
public class SchemaEnrichmentVisitor extends AbstractGraphixQueryVisitor {
    private final ElementLookupTable elementLookupTable;
    private final Set<FunctionIdentifier> functionIdentifiers;
    private final Map<VarIdentifier, Expression> expressionMap;
    private final GraphSelectBlock workingSelectBlock;
    private final GraphIdentifier graphIdentifier;

    public SchemaEnrichmentVisitor(ElementLookupTable elementLookupTable, GraphIdentifier graphIdentifier,
            GraphSelectBlock workingSelectBlock, Set<FunctionIdentifier> functionIdentifiers) {
        this.graphIdentifier = graphIdentifier;
        this.elementLookupTable = elementLookupTable;
        this.workingSelectBlock = workingSelectBlock;
        this.functionIdentifiers = functionIdentifiers;
        this.expressionMap = new HashMap<>();
    }

    @Override
    public Expression visit(GraphSelectBlock graphSelectBlock, ILangExpression arg) throws CompilationException {
        // Visit our immediate MATCH AST nodes (do not visit lower levels).
        for (MatchClause matchClause : graphSelectBlock.getFromGraphClause().getMatchClauses()) {
            matchClause.accept(this, arg);
        }

        // We are going to enrich these LET-CORRELATE clauses w/ the necessary schema.
        if (workingSelectBlock.equals(graphSelectBlock)) {
            List<CorrLetClause> corrLetClauses = graphSelectBlock.getFromClause().getFromTerms().get(0)
                    .getCorrelateClauses().stream().filter(c -> c instanceof CorrLetClause).map(c -> (CorrLetClause) c)
                    .collect(Collectors.toList());
            Collections.reverse(corrLetClauses);
            for (FunctionIdentifier functionIdentifier : functionIdentifiers) {
                IFunctionPrepare functionPrepare = getFunctionPrepare(functionIdentifier);
                Set<VarIdentifier> visitedBindings = new HashSet<>();
                for (CorrLetClause corrLetClause : corrLetClauses) {
                    VarIdentifier rightVar = corrLetClause.getRightVariable().getVar();
                    Expression graphExpr = expressionMap.getOrDefault(rightVar, null);
                    boolean isVertex = isVertexFunction(functionIdentifier) && graphExpr instanceof VertexPatternExpr;
                    boolean isEdge = isEdgeFunction(functionIdentifier) && graphExpr instanceof EdgePatternExpr;
                    if (!visitedBindings.contains(rightVar) && (isEdge || isVertex)) {
                        Expression outputExpr = functionPrepare.prepare(corrLetClause.getRightExpression(), graphExpr,
                                graphIdentifier, elementLookupTable);
                        corrLetClause.setRightExpression(outputExpr);
                        visitedBindings.add(rightVar);
                    }
                }
            }
        }
        return null;
    }

    @Override
    public Expression visit(VertexPatternExpr vertexPatternExpr, ILangExpression arg) throws CompilationException {
        VariableExpr variableExpr = vertexPatternExpr.getVariableExpr();
        if (variableExpr != null) {
            expressionMap.put(variableExpr.getVar(), vertexPatternExpr);
        }
        return super.visit(vertexPatternExpr, arg);
    }

    @Override
    public Expression visit(EdgePatternExpr edgePatternExpr, ILangExpression arg) throws CompilationException {
        EdgeDescriptor edgeDescriptor = edgePatternExpr.getEdgeDescriptor();
        VariableExpr variableExpr = edgeDescriptor.getVariableExpr();
        if (variableExpr != null) {
            expressionMap.put(variableExpr.getVar(), edgePatternExpr);
        }
        return super.visit(edgePatternExpr, arg);
    }
}
