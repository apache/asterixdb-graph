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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.clause.FromGraphClause;
import org.apache.asterix.graphix.lang.clause.MatchClause;
import org.apache.asterix.graphix.lang.expression.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.PathPatternExpr;
import org.apache.asterix.graphix.lang.expression.VertexPatternExpr;
import org.apache.asterix.graphix.lang.optype.MatchType;
import org.apache.asterix.graphix.lang.rewrites.common.EdgeDependencyGraph;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.VarIdentifier;

/**
 * Perform an analysis pass of our AST to generate three items: a) a list of edge orderings, b) a list of dangling
 * vertices, and c) a list of optional variables (i.e. vertices) per FROM-GRAPH-CLAUSE.
 * - To generate an {@link EdgeDependencyGraph}, we create an adjacency map of query edges whose adjacency between
 * other query edges is defined as sharing some vertex. The adjacency list associated with each query edge is ordered
 * by visitation, and will be used in {@link EdgeDependencyGraph#iterator()}- in short, this means that the order of
 * the patterns in query may affect the resulting lowered AST.
 * - For each LEFT-MATCH-CLAUSE, we build a new dependency subgraph. Every subsequent subgraph should possess a
 * leading edge that contains a vertex found in the previous subgraph (if it exists).
 * - To generate our list of optional vertices, we first collect all vertices found before the first LEFT-MATCH-CLAUSE.
 * For the set of vertices found after this LEFT-MATCH-CLAUSE, we take the difference of this set and the previous set.
 */
public class ElementAnalysisVisitor extends AbstractGraphixQueryVisitor {
    private static class AnalysisEnvironment {
        // We must populate these maps (edge ID -> edge IDs).
        private final List<Map<Identifier, List<Identifier>>> adjacencyMaps = new ArrayList<>();
        private final Map<Identifier, EdgePatternExpr> edgePatternMap = new LinkedHashMap<>();

        // Build a separate map to define dependencies (vertex ID -> edge IDs).
        private final Map<Identifier, List<Identifier>> vertexEdgeMap = new LinkedHashMap<>();
        private final List<VertexPatternExpr> danglingVertices = new ArrayList<>();

        // To allow for LEFT-MATCH patterns, we must keep track of which vertices are not mandatory.
        private final Set<VarIdentifier> visitedVertices = new HashSet<>();
        private final Set<VarIdentifier> mandatoryVertices = new HashSet<>();
        private final Set<VarIdentifier> optionalVertices = new HashSet<>();
        private Map<Identifier, List<Identifier>> workingAdjacencyMap = new LinkedHashMap<>();
    }

    // We will return instances of the following back to our caller.
    public static class FromGraphClauseContext {
        private final EdgeDependencyGraph edgeDependencyGraph;
        private final List<VertexPatternExpr> danglingVertices;
        private final List<VarIdentifier> optionalVariables;

        private FromGraphClauseContext(EdgeDependencyGraph edgeDependencyGraph,
                List<VertexPatternExpr> danglingVertices, List<VarIdentifier> optionalVariables) {
            this.edgeDependencyGraph = edgeDependencyGraph;
            this.danglingVertices = danglingVertices;
            this.optionalVariables = optionalVariables;
        }

        public EdgeDependencyGraph getEdgeDependencyGraph() {
            return edgeDependencyGraph;
        }

        public List<VertexPatternExpr> getDanglingVertices() {
            return danglingVertices;
        }

        public List<VarIdentifier> getOptionalVariables() {
            return optionalVariables;
        }
    }

    // We will build new environments on each visit of a FROM-GRAPH-CLAUSE.
    private final Map<FromGraphClause, AnalysisEnvironment> analysisEnvironmentMap = new HashMap<>();
    private AnalysisEnvironment workingEnvironment;

    private void addEdgeDependency(VarIdentifier vertexID, VarIdentifier edgeID, List<Identifier> dependencyList) {
        if (workingEnvironment.vertexEdgeMap.containsKey(vertexID)) {
            dependencyList.addAll(workingEnvironment.vertexEdgeMap.get(vertexID));
            workingEnvironment.vertexEdgeMap.get(vertexID).add(edgeID);

        } else {
            List<Identifier> vertexDependencies = new ArrayList<>();
            vertexDependencies.add(edgeID);
            workingEnvironment.vertexEdgeMap.put(vertexID, vertexDependencies);
        }
    }

    @Override
    public Expression visit(FromGraphClause fromGraphClause, ILangExpression arg) throws CompilationException {
        // Add to our a map a new analysis environment.
        workingEnvironment = new AnalysisEnvironment();
        analysisEnvironmentMap.put(fromGraphClause, workingEnvironment);
        return super.visit(fromGraphClause, arg);
    }

    @Override
    public Expression visit(MatchClause matchClause, ILangExpression arg) throws CompilationException {
        // Reset to build a new graph.
        if (matchClause.getMatchType() == MatchType.LEFTOUTER) {
            workingEnvironment.adjacencyMaps.add(workingEnvironment.workingAdjacencyMap);
            workingEnvironment.workingAdjacencyMap = new LinkedHashMap<>();
            workingEnvironment.vertexEdgeMap.clear();
        }
        super.visit(matchClause, arg);

        // Record which vertices are mandatory and which are optional.
        if (matchClause.getMatchType() != MatchType.LEFTOUTER && workingEnvironment.optionalVertices.isEmpty()) {
            workingEnvironment.mandatoryVertices.addAll(workingEnvironment.visitedVertices);

        } else { // We have encountered a LEFT-OUTER-MATCH-CLAUSE.
            workingEnvironment.visitedVertices.stream().filter(v -> !workingEnvironment.mandatoryVertices.contains(v))
                    .forEach(workingEnvironment.optionalVertices::add);
        }
        return null;
    }

    @Override
    public Expression visit(PathPatternExpr pathExpression, ILangExpression arg) throws CompilationException {
        // Visit our edges first, then our vertices. This allows us to determine our dangling vertices.
        for (EdgePatternExpr edgeExpression : pathExpression.getEdgeExpressions()) {
            edgeExpression.accept(this, arg);
        }
        for (VertexPatternExpr vertexExpression : pathExpression.getVertexExpressions()) {
            vertexExpression.accept(this, arg);
        }
        return pathExpression;
    }

    @Override
    public Expression visit(VertexPatternExpr vertexExpression, ILangExpression arg) throws CompilationException {
        VarIdentifier vertexID = vertexExpression.getVariableExpr().getVar();
        if (!workingEnvironment.vertexEdgeMap.containsKey(vertexID)) {
            workingEnvironment.danglingVertices.add(vertexExpression);
        }
        workingEnvironment.visitedVertices.add(vertexID);
        return vertexExpression;
    }

    @Override
    public Expression visit(EdgePatternExpr edgeExpression, ILangExpression arg) throws CompilationException {
        VarIdentifier leftVertexID = edgeExpression.getLeftVertex().getVariableExpr().getVar();
        VarIdentifier rightVertexID = edgeExpression.getRightVertex().getVariableExpr().getVar();
        VarIdentifier edgeID = edgeExpression.getEdgeDescriptor().getVariableExpr().getVar();
        List<Identifier> edgeDependencies = new ArrayList<>();

        addEdgeDependency(leftVertexID, edgeID, edgeDependencies);
        addEdgeDependency(rightVertexID, edgeID, edgeDependencies);
        workingEnvironment.workingAdjacencyMap.put(edgeID, edgeDependencies);
        workingEnvironment.edgePatternMap.put(edgeID, edgeExpression);
        for (Identifier dependencyEdgeID : edgeDependencies) {
            // Populate our map in reverse as well.
            workingEnvironment.workingAdjacencyMap.get(dependencyEdgeID).add(edgeID);
        }

        // We ignore any internal vertices here.
        return edgeExpression;
    }

    public Map<FromGraphClause, FromGraphClauseContext> getFromGraphClauseContextMap() {
        return analysisEnvironmentMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> {
            AnalysisEnvironment analysisEnvironment = e.getValue();
            if (!analysisEnvironment.workingAdjacencyMap.isEmpty()) {
                // We have not finished building our edge dependency subgraph.
                analysisEnvironment.adjacencyMaps.add(analysisEnvironment.workingAdjacencyMap);
                analysisEnvironment.workingAdjacencyMap = new LinkedHashMap<>();
                analysisEnvironment.vertexEdgeMap.clear();
            }

            // Construct our output.
            EdgeDependencyGraph edgeDependencyGraph =
                    new EdgeDependencyGraph(e.getValue().adjacencyMaps, e.getValue().edgePatternMap);
            List<VertexPatternExpr> danglingVertices = e.getValue().danglingVertices;
            List<VarIdentifier> optionalVariables = new ArrayList<>(e.getValue().optionalVertices);
            return new FromGraphClauseContext(edgeDependencyGraph, danglingVertices, optionalVariables);
        }));
    }
}
