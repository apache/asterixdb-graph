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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.graphix.common.metadata.GraphIdentifier;
import org.apache.asterix.graphix.lang.clause.FromGraphClause;
import org.apache.asterix.graphix.lang.clause.MatchClause;
import org.apache.asterix.graphix.lang.expression.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.PathPatternExpr;
import org.apache.asterix.graphix.lang.expression.VertexPatternExpr;
import org.apache.asterix.graphix.lang.optype.MatchType;
import org.apache.asterix.graphix.lang.rewrites.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.rewrites.common.EdgeDependencyGraph;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.metadata.declared.MetadataProvider;

/**
 * Perform an analysis pass of our AST to generate three items: a) a list of edge orderings, b) a list of dangling
 * vertices, and c) a list of path patterns.
 * - To generate an {@link EdgeDependencyGraph}, we create an adjacency map of query edges whose adjacency between
 * other query edges is defined as sharing some vertex. The adjacency list associated with each query edge is ordered
 * by visitation, and will be used in {@link EdgeDependencyGraph#iterator()}- in short, this means that the order of
 * the patterns in query may affect the resulting lowered AST.
 * - For each LEFT-MATCH-CLAUSE, we build a new dependency subgraph. Every subsequent subgraph should possess a
 * leading edge that contains a vertex found in the previous subgraph (if it exists).
 */
public class StructureAnalysisVisitor extends AbstractGraphixQueryVisitor {
    private static class AnalysisEnvironment {
        // We must populate these maps (edge ID -> edge IDs).
        private final List<Map<Identifier, List<Identifier>>> adjacencyMaps;
        private final Map<Identifier, EdgePatternExpr> edgePatternMap;
        private Map<Identifier, List<Identifier>> workingAdjacencyMap;

        // Build a separate map to define dependencies (vertex ID -> edge IDs).
        private final Map<Identifier, List<Identifier>> vertexEdgeMap;
        private final Deque<List<VertexPatternExpr>> danglingVertices;
        private final Deque<List<PathPatternExpr>> pathPatterns;

        private AnalysisEnvironment() {
            this.adjacencyMaps = new ArrayList<>();
            this.edgePatternMap = new LinkedHashMap<>();
            this.workingAdjacencyMap = new LinkedHashMap<>();

            this.pathPatterns = new ArrayDeque<>();
            this.danglingVertices = new ArrayDeque<>();
            this.pathPatterns.addLast(new ArrayList<>());
            this.danglingVertices.addLast(new ArrayList<>());
            this.vertexEdgeMap = new LinkedHashMap<>();
        }
    }

    // We will return instances of the following back to our caller.
    public static class StructureContext {
        private final Deque<List<PathPatternExpr>> pathPatternQueue;
        private final Deque<List<VertexPatternExpr>> danglingVertexQueue;
        private final EdgeDependencyGraph edgeDependencyGraph;
        private final GraphIdentifier graphIdentifier;

        private StructureContext(EdgeDependencyGraph edgeGraph, Deque<List<PathPatternExpr>> pathPatternQueue,
                Deque<List<VertexPatternExpr>> danglingVertexQueue, GraphIdentifier graphIdentifier) {
            this.edgeDependencyGraph = edgeGraph;
            this.pathPatternQueue = pathPatternQueue;
            this.danglingVertexQueue = danglingVertexQueue;
            this.graphIdentifier = graphIdentifier;
        }

        public EdgeDependencyGraph getEdgeDependencyGraph() {
            return edgeDependencyGraph;
        }

        public Deque<List<VertexPatternExpr>> getDanglingVertexQueue() {
            return danglingVertexQueue;
        }

        public Deque<List<PathPatternExpr>> getPathPatternQueue() {
            return pathPatternQueue;
        }

        public GraphIdentifier getGraphIdentifier() {
            return graphIdentifier;
        }
    }

    // We will build new environments on each visit of a FROM-GRAPH-CLAUSE.
    private final Map<FromGraphClause, AnalysisEnvironment> analysisEnvironmentMap;
    private final GraphixRewritingContext graphixRewritingContext;
    private AnalysisEnvironment workingEnvironment;

    public StructureAnalysisVisitor(GraphixRewritingContext graphixRewritingContext) {
        this.analysisEnvironmentMap = new HashMap<>();
        this.graphixRewritingContext = graphixRewritingContext;
    }

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

        // Collect our structure context.
        return super.visit(fromGraphClause, arg);
    }

    @Override
    public Expression visit(MatchClause matchClause, ILangExpression arg) throws CompilationException {
        // Reset to build a new graph.
        if (matchClause.getMatchType() == MatchType.LEFTOUTER) {
            workingEnvironment.danglingVertices.addLast(new ArrayList<>());
            workingEnvironment.pathPatterns.addLast(new ArrayList<>());
            workingEnvironment.vertexEdgeMap.clear();

            // We should retain all adjacency information from the previous walks.
            workingEnvironment.adjacencyMaps.add(workingEnvironment.workingAdjacencyMap);
            workingEnvironment.workingAdjacencyMap = new LinkedHashMap<>(workingEnvironment.workingAdjacencyMap);
        }
        return super.visit(matchClause, arg);
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
        workingEnvironment.pathPatterns.getLast().add(pathExpression);
        return pathExpression;
    }

    @Override
    public Expression visit(VertexPatternExpr vertexExpression, ILangExpression arg) throws CompilationException {
        VarIdentifier vertexID = vertexExpression.getVariableExpr().getVar();
        if (!workingEnvironment.vertexEdgeMap.containsKey(vertexID)) {
            workingEnvironment.danglingVertices.getLast().add(vertexExpression);
        }
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

    public Map<FromGraphClause, StructureContext> getFromGraphClauseContextMap() {
        return analysisEnvironmentMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> {
            AnalysisEnvironment analysisEnvironment = e.getValue();
            if (!analysisEnvironment.workingAdjacencyMap.isEmpty()) {
                // We have not finished building our edge dependency subgraph.
                analysisEnvironment.adjacencyMaps.add(analysisEnvironment.workingAdjacencyMap);
                analysisEnvironment.workingAdjacencyMap = new LinkedHashMap<>();
                analysisEnvironment.vertexEdgeMap.clear();
            }

            // Build our graph identifier.
            FromGraphClause fromGraphClause = e.getKey();
            MetadataProvider metadataProvider = graphixRewritingContext.getMetadataProvider();
            DataverseName dataverseName = (fromGraphClause.getDataverseName() == null)
                    ? metadataProvider.getDefaultDataverseName() : fromGraphClause.getDataverseName();
            String graphName = (fromGraphClause.getGraphName() != null) ? fromGraphClause.getGraphName().getValue()
                    : fromGraphClause.getGraphConstructor().getInstanceID();
            GraphIdentifier graphIdentifier = new GraphIdentifier(dataverseName, graphName);

            // Construct our output.
            List<Map<Identifier, List<Identifier>>> adjacencyMaps = e.getValue().adjacencyMaps;
            Map<Identifier, EdgePatternExpr> edgePatternMap = e.getValue().edgePatternMap;
            EdgeDependencyGraph edgeDependencyGraph = new EdgeDependencyGraph(adjacencyMaps, edgePatternMap);
            Deque<List<VertexPatternExpr>> danglingVertices = e.getValue().danglingVertices;
            Deque<List<PathPatternExpr>> pathPatterns = e.getValue().pathPatterns;
            return new StructureContext(edgeDependencyGraph, pathPatterns, danglingVertices, graphIdentifier);
        }));
    }
}
