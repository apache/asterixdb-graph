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

import static org.apache.asterix.graphix.extension.GraphixMetadataExtension.getGraph;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.graphix.common.metadata.GraphIdentifier;
import org.apache.asterix.graphix.lang.clause.FromGraphClause;
import org.apache.asterix.graphix.lang.clause.MatchClause;
import org.apache.asterix.graphix.lang.expression.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.GraphConstructor;
import org.apache.asterix.graphix.lang.expression.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrite.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.statement.DeclareGraphStatement;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.graphix.lang.struct.ElementLabel;
import org.apache.asterix.graphix.lang.visitor.base.AbstractGraphixQueryVisitor;
import org.apache.asterix.graphix.metadata.entity.schema.Graph;
import org.apache.asterix.graphix.metadata.entity.schema.Schema;
import org.apache.asterix.graphix.metadata.entity.schema.Vertex;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * A pre-rewrite pass to validate / raise warnings about our user query.
 * <p>
 * We validate the following:
 * <ul>
 *  <li>An edge variable is not defined more than once. (e.g. (u)-[e]-(v), (w)-[e]-(y))</li>
 *  <li>A vertex variable is not defined more than once with labels. (e.g. (u:User)-[e]-(v), (u:User)-[]-(:Review))</li>
 *  <li>An edge label exists in the context of the edge's {@link FromGraphClause}.</li>
 *  <li>A vertex label exists in the context of the vertex's {@link FromGraphClause}.</li>
 *  <li>The minimum hops and maximum hops of a sub-path is not equal to zero.</li>
 *  <li>The maximum hops of a sub-path is greater than or equal to the minimum hops of the same sub-path.</li>
 *  <li>An anonymous / declared graph passes the same validation that a named graph does.</li>
 *  <li>That variables in an element filter expression do not reference other previously defined graph-elements.</li>
 * </ul>
 * <p>
 * We raise warnings about the following:
 * <ul>
 *  <li>We encounter a disconnected pattern. TODO (GLENN): Implement this.</li>
 * </ul>
 */
public class PreRewriteCheckVisitor extends AbstractGraphixQueryVisitor {
    private final Map<GraphIdentifier, DeclareGraphStatement> declaredGraphs;
    private final MetadataProvider metadataProvider;

    // Build new environments on each FROM-GRAPH-CLAUSE visit.
    private static class PreRewriteCheckEnvironment {
        private final Set<ElementLabel> elementLabels = new HashSet<>();
        private final Set<ElementLabel> edgeLabels = new HashSet<>();
        private final Set<Identifier> vertexVariablesWithLabels = new HashSet<>();
        private final Set<Identifier> edgeVariables = new HashSet<>();
        private final Set<Identifier> allElementVariables = new HashSet<>();
    }

    private final Map<ILangExpression, PreRewriteCheckEnvironment> environmentMap = new HashMap<>();

    public PreRewriteCheckVisitor(GraphixRewritingContext graphixRewritingContext) {
        this.declaredGraphs = graphixRewritingContext.getDeclaredGraphs();
        this.metadataProvider = graphixRewritingContext.getMetadataProvider();
    }

    @Override
    public Expression visit(GraphConstructor graphConstructor, ILangExpression arg) throws CompilationException {
        GraphIdentifier graphIdentifier = ((FromGraphClause) arg).getGraphIdentifier(metadataProvider);
        Schema.Builder schemaBuilder = new Schema.Builder(graphIdentifier);

        // Perform the same validation we do for named graphs-- but don't build the schema object.
        for (GraphConstructor.VertexConstructor vertex : graphConstructor.getVertexElements()) {
            schemaBuilder.addVertex(vertex.getLabel(), vertex.getPrimaryKeyFields(), vertex.getDefinition());
            if (schemaBuilder.getLastError() == Schema.Builder.Error.VERTEX_LABEL_CONFLICT) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, vertex.getSourceLocation(),
                        "Conflicting vertex label found: " + vertex.getLabel());

            } else if (schemaBuilder.getLastError() != Schema.Builder.Error.NO_ERROR) {
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, vertex.getSourceLocation(),
                        "Constructor vertex was not returned, but the error is not a conflicting vertex label!");
            }
        }
        for (GraphConstructor.EdgeConstructor edge : graphConstructor.getEdgeElements()) {
            schemaBuilder.addEdge(edge.getEdgeLabel(), edge.getDestinationLabel(), edge.getSourceLabel(),
                    edge.getDestinationKeyFields(), edge.getSourceKeyFields(), edge.getDefinition());
            switch (schemaBuilder.getLastError()) {
                case NO_ERROR:
                    continue;

                case SOURCE_VERTEX_NOT_FOUND:
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, edge.getSourceLocation(),
                            "Source vertex " + edge.getSourceLabel() + " not found in the edge " + edge.getEdgeLabel()
                                    + ".");

                case DESTINATION_VERTEX_NOT_FOUND:
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, edge.getSourceLocation(),
                            "Destination vertex " + edge.getDestinationLabel() + " not found in the edge "
                                    + edge.getEdgeLabel() + ".");

                case EDGE_LABEL_CONFLICT:
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, edge.getSourceLocation(),
                            "Conflicting edge label found: " + edge.getEdgeLabel());

                default:
                    throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, edge.getSourceLocation(),
                            "Edge constructor was not returned, and an unexpected error encountered");
            }
        }
        return null;
    }

    @Override
    public Expression visit(FromGraphClause fromGraphClause, ILangExpression arg) throws CompilationException {
        environmentMap.put(fromGraphClause, new PreRewriteCheckEnvironment());

        // Establish the vertex and edge labels associated with this FROM-GRAPH-CLAUSE.
        GraphConstructor graphConstructor = fromGraphClause.getGraphConstructor();
        if (graphConstructor == null) {
            DataverseName dataverseName = (fromGraphClause.getDataverseName() == null)
                    ? metadataProvider.getDefaultDataverseName() : fromGraphClause.getDataverseName();
            Identifier graphName = fromGraphClause.getGraphName();

            // First, see if we can fetch the graph constructor from our declared graphs.
            GraphIdentifier graphIdentifier = fromGraphClause.getGraphIdentifier(metadataProvider);
            DeclareGraphStatement declaredGraph = declaredGraphs.get(graphIdentifier);
            if (declaredGraph != null) {
                graphConstructor = declaredGraph.getGraphConstructor();

            } else {
                // Otherwise, fetch the graph from our metadata.
                try {
                    Graph graphFromMetadata =
                            getGraph(metadataProvider.getMetadataTxnContext(), dataverseName, graphName.getValue());
                    if (graphFromMetadata == null) {
                        throw new CompilationException(ErrorCode.COMPILATION_ERROR, fromGraphClause.getSourceLocation(),
                                "Graph " + graphName.getValue() + " does not exist.");

                    } else {
                        graphFromMetadata.getGraphSchema().getVertices().stream().map(Vertex::getLabel)
                                .forEach(environmentMap.get(fromGraphClause).elementLabels::add);
                        graphFromMetadata.getGraphSchema().getEdges().forEach(e -> {
                            environmentMap.get(fromGraphClause).elementLabels.add(e.getSourceLabel());
                            environmentMap.get(fromGraphClause).elementLabels.add(e.getDestinationLabel());
                            environmentMap.get(fromGraphClause).edgeLabels.add(e.getLabel());
                        });
                    }

                } catch (AlgebricksException e) {
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, fromGraphClause.getSourceLocation(),
                            "Graph " + graphName.getValue() + " does not exist.");
                }
            }
        }
        if (graphConstructor != null) {
            graphConstructor.getVertexElements().stream().map(GraphConstructor.VertexConstructor::getLabel)
                    .forEach(environmentMap.get(fromGraphClause).elementLabels::add);
            graphConstructor.getEdgeElements().forEach(e -> {
                environmentMap.get(fromGraphClause).elementLabels.add(e.getSourceLabel());
                environmentMap.get(fromGraphClause).elementLabels.add(e.getDestinationLabel());
                environmentMap.get(fromGraphClause).edgeLabels.add(e.getEdgeLabel());
            });
            graphConstructor.accept(this, fromGraphClause);
        }

        // We need to pass our FROM-GRAPH-CLAUSE to our MATCH-CLAUSE.
        for (MatchClause matchClause : fromGraphClause.getMatchClauses()) {
            matchClause.accept(this, fromGraphClause);
        }
        for (AbstractBinaryCorrelateClause correlateClause : fromGraphClause.getCorrelateClauses()) {
            correlateClause.accept(this, arg);
        }
        return null;
    }

    @Override
    public Expression visit(VertexPatternExpr vertexExpression, ILangExpression arg) throws CompilationException {
        PreRewriteCheckEnvironment workingEnvironment = environmentMap.get(arg);
        Set<ElementLabel> environmentLabels = workingEnvironment.elementLabels;
        for (ElementLabel elementLabel : vertexExpression.getLabels()) {
            if (environmentLabels.stream().noneMatch(e -> e.getLabelName().equals(elementLabel.getLabelName()))) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, vertexExpression.getSourceLocation(),
                        "Vertex label " + elementLabel + " does not exist in the given graph schema.");
            }
        }
        if (vertexExpression.getFilterExpr() != null) {
            vertexExpression.getFilterExpr().accept(new AbstractGraphixQueryVisitor() {
                @Override
                public Expression visit(VariableExpr varExpr, ILangExpression arg) throws CompilationException {
                    if (workingEnvironment.allElementVariables.contains(varExpr.getVar())) {
                        SourceLocation sourceLocation = vertexExpression.getFilterExpr().getSourceLocation();
                        throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLocation,
                                "Cannot reference other graph elements in a filter expression! Consider putting your "
                                        + "condition in a WHERE clause.");
                    }
                    return super.visit(varExpr, arg);
                }
            }, null);
        }
        if (vertexExpression.getVariableExpr() != null && !vertexExpression.getLabels().isEmpty()) {
            Identifier vertexIdentifier = vertexExpression.getVariableExpr().getVar();
            if (workingEnvironment.vertexVariablesWithLabels.contains(vertexIdentifier)) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, vertexExpression.getSourceLocation(),
                        "Vertex " + vertexIdentifier + " defined with a label more than once. Labels can only be "
                                + "bound to vertices once.");
            }
            workingEnvironment.vertexVariablesWithLabels.add(vertexIdentifier);
            workingEnvironment.allElementVariables.add(vertexIdentifier);
        }
        return vertexExpression;
    }

    @Override
    public Expression visit(EdgePatternExpr edgeExpression, ILangExpression arg) throws CompilationException {
        PreRewriteCheckEnvironment workingEnvironment = environmentMap.get(arg);
        EdgeDescriptor edgeDescriptor = edgeExpression.getEdgeDescriptor();
        Set<ElementLabel> environmentLabels = workingEnvironment.edgeLabels;
        if (environmentLabels.isEmpty()) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, edgeExpression.getSourceLocation(),
                    "Query edge given, but no edge is defined in the schema.");
        }

        for (ElementLabel edgeLabel : edgeDescriptor.getEdgeLabels()) {
            if (environmentLabels.stream().noneMatch(e -> e.getLabelName().equals(edgeLabel.getLabelName()))) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, edgeExpression.getSourceLocation(),
                        "Edge label " + edgeLabel + " does not exist in the given graph schema.");
            }
        }
        if (edgeDescriptor.getFilterExpr() != null) {
            edgeDescriptor.getFilterExpr().accept(new AbstractGraphixQueryVisitor() {
                @Override
                public Expression visit(VariableExpr varExpr, ILangExpression arg) throws CompilationException {
                    if (workingEnvironment.allElementVariables.contains(varExpr.getVar())) {
                        SourceLocation sourceLocation = edgeDescriptor.getFilterExpr().getSourceLocation();
                        throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLocation,
                                "Cannot reference other graph elements in a filter expression! Consider putting your "
                                        + "condition in a WHERE clause.");
                    }
                    return super.visit(varExpr, arg);
                }
            }, null);
        }
        if (edgeDescriptor.getVariableExpr() != null) {
            Identifier edgeIdentifier = edgeDescriptor.getVariableExpr().getVar();
            if (workingEnvironment.edgeVariables.contains(edgeIdentifier)) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, edgeExpression.getSourceLocation(),
                        "Edge " + edgeIdentifier + " defined more than once. Edges can only connect two vertices.");
            }
            workingEnvironment.edgeVariables.add(edgeIdentifier);
            workingEnvironment.allElementVariables.add(edgeIdentifier);
        }
        if (edgeDescriptor.getPatternType() == EdgeDescriptor.PatternType.PATH) {
            Integer minimumHops = edgeDescriptor.getMinimumHops();
            Integer maximumHops = edgeDescriptor.getMaximumHops();
            if ((maximumHops != null && maximumHops == 0) || (minimumHops != null && minimumHops == 0)) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, edgeExpression.getSourceLocation(),
                        "Sub-path edges cannot have a hop length less than 1.");

            } else if (minimumHops != null && maximumHops != null && maximumHops < minimumHops) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, edgeExpression.getSourceLocation(),
                        "Sub-path edges cannot have a maximum hop length (" + maximumHops
                                + ") less than the minimum hop length (" + minimumHops + ").");
            }
        }
        return edgeExpression;
    }
}
