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

import static org.apache.asterix.graphix.lang.parser.GraphElementBodyParser.parse;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.graphix.common.metadata.GraphElementIdentifier;
import org.apache.asterix.graphix.common.metadata.GraphIdentifier;
import org.apache.asterix.graphix.extension.GraphixMetadataExtension;
import org.apache.asterix.graphix.lang.clause.FromGraphClause;
import org.apache.asterix.graphix.lang.clause.MatchClause;
import org.apache.asterix.graphix.lang.expression.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.GraphConstructor;
import org.apache.asterix.graphix.lang.expression.VertexPatternExpr;
import org.apache.asterix.graphix.lang.parser.GraphixParserFactory;
import org.apache.asterix.graphix.lang.rewrites.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.rewrites.common.ElementLookupTable;
import org.apache.asterix.graphix.lang.statement.DeclareGraphStatement;
import org.apache.asterix.graphix.lang.statement.GraphElementDeclaration;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.graphix.lang.struct.ElementLabel;
import org.apache.asterix.graphix.metadata.entity.schema.Edge;
import org.apache.asterix.graphix.metadata.entity.schema.Graph;
import org.apache.asterix.graphix.metadata.entity.schema.Vertex;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.IWarningCollector;

/**
 * Populate the given graph element table, which will hold all referenced {@link GraphElementDeclaration}s. We assume
 * that our graph elements are properly labeled at this point (i.e. {@link StructureResolutionVisitor} must run before
 * this).
 */
public class ElementLookupTableVisitor extends AbstractGraphixQueryVisitor {
    private final IWarningCollector warningCollector;
    private final MetadataProvider metadataProvider;
    private final GraphixParserFactory parserFactory;

    private final Set<ElementLabel> referencedVertexLabels = new HashSet<>();
    private final Set<ElementLabel> referencedEdgeLabels = new HashSet<>();
    private final ElementLookupTable elementLookupTable;
    private final Map<GraphIdentifier, DeclareGraphStatement> declaredGraphs;

    public ElementLookupTableVisitor(GraphixRewritingContext graphixRewritingContext,
            ElementLookupTable elementLookupTable, GraphixParserFactory parserFactory) {
        this.parserFactory = Objects.requireNonNull(parserFactory);
        this.elementLookupTable = Objects.requireNonNull(elementLookupTable);
        this.warningCollector = graphixRewritingContext.getWarningCollector();
        this.metadataProvider = graphixRewritingContext.getMetadataProvider();
        this.declaredGraphs = graphixRewritingContext.getDeclaredGraphs();
    }

    @Override
    public Expression visit(FromGraphClause fromGraphClause, ILangExpression arg) throws CompilationException {
        for (MatchClause m : fromGraphClause.getMatchClauses()) {
            m.accept(this, null);
        }

        GraphConstructor graphConstructor = fromGraphClause.getGraphConstructor();
        GraphIdentifier graphIdentifier = null;
        if (graphConstructor == null) {
            DataverseName dataverseName = (fromGraphClause.getDataverseName() == null)
                    ? metadataProvider.getDefaultDataverseName() : fromGraphClause.getDataverseName();
            Identifier graphName = fromGraphClause.getGraphName();

            // Our query refers to a named graph. First see if we can find this in our declared graph set.
            graphIdentifier = new GraphIdentifier(dataverseName, graphName.getValue());
            DeclareGraphStatement declaredGraph = declaredGraphs.get(graphIdentifier);
            if (declaredGraph != null) {
                graphConstructor = declaredGraph.getGraphConstructor();

            } else {
                // Otherwise, load this from our metadata.
                Graph graphFromMetadata;
                try {
                    graphFromMetadata = GraphixMetadataExtension.getGraph(metadataProvider.getMetadataTxnContext(),
                            dataverseName, graphName.getValue());
                    if (graphFromMetadata == null) {
                        throw new CompilationException(ErrorCode.COMPILATION_ERROR, fromGraphClause.getSourceLocation(),
                                "Graph " + graphName.getValue() + " does not exist.");
                    }

                } catch (AlgebricksException e) {
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, fromGraphClause.getSourceLocation(),
                            "Graph " + graphName.getValue() + " does not exist.");
                }

                for (Vertex vertex : graphFromMetadata.getGraphSchema().getVertices()) {
                    if (referencedVertexLabels.contains(vertex.getLabel())) {
                        GraphElementDeclaration vertexDecl = parse(vertex, parserFactory, warningCollector);
                        elementLookupTable.put(vertex.getIdentifier(), vertexDecl);
                        elementLookupTable.putVertexKey(vertex.getIdentifier(), vertex.getPrimaryKeyFieldNames());
                    }
                }
                for (Edge edge : graphFromMetadata.getGraphSchema().getEdges()) {
                    if (referencedEdgeLabels.contains(edge.getLabel())) {
                        GraphElementDeclaration edgeDecl = parse(edge, parserFactory, warningCollector);
                        elementLookupTable.put(edge.getIdentifier(), edgeDecl);
                        elementLookupTable.putEdgeKeys(edge.getIdentifier(), edge.getSourceKeyFieldNames(),
                                edge.getDestinationKeyFieldNames());
                    }
                }
            }
        }
        if (graphConstructor != null) {
            if (graphIdentifier == null) {
                // We have been provided an anonymous graph. Load the referenced elements from our walk.
                DataverseName defaultDataverse = metadataProvider.getDefaultDataverse().getDataverseName();
                graphIdentifier = new GraphIdentifier(defaultDataverse, graphConstructor.getInstanceID());
            }

            for (GraphConstructor.VertexConstructor vertex : graphConstructor.getVertexElements()) {
                if (referencedVertexLabels.contains(vertex.getLabel())) {
                    GraphElementIdentifier identifier = new GraphElementIdentifier(graphIdentifier,
                            GraphElementIdentifier.Kind.VERTEX, vertex.getLabel());
                    elementLookupTable.put(identifier, new GraphElementDeclaration(identifier, vertex.getExpression()));
                    elementLookupTable.putVertexKey(identifier, vertex.getPrimaryKeyFields());
                }
            }
            for (GraphConstructor.EdgeConstructor edge : graphConstructor.getEdgeElements()) {
                if (referencedEdgeLabels.contains(edge.getEdgeLabel())) {
                    GraphElementIdentifier identifier = new GraphElementIdentifier(graphIdentifier,
                            GraphElementIdentifier.Kind.EDGE, edge.getEdgeLabel());
                    elementLookupTable.put(identifier, new GraphElementDeclaration(identifier, edge.getExpression()));
                    elementLookupTable.putEdgeKeys(identifier, edge.getSourceKeyFields(),
                            edge.getDestinationKeyFields());
                }
            }
        }
        return null;
    }

    public Expression visit(EdgePatternExpr edgeExpression, ILangExpression arg) throws CompilationException {
        EdgeDescriptor edgeDescriptor = edgeExpression.getEdgeDescriptor();
        if (edgeDescriptor.getEdgeLabels().isEmpty()) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, edgeExpression.getSourceLocation(),
                    "EdgePatternExpr found without labels. Elements should have been resolved earlier.");
        }
        referencedEdgeLabels.addAll(edgeDescriptor.getEdgeLabels());
        for (VertexPatternExpr internalVertex : edgeExpression.getInternalVertices()) {
            internalVertex.accept(this, arg);
        }
        return edgeExpression;
    }

    public Expression visit(VertexPatternExpr vertexExpression, ILangExpression arg) throws CompilationException {
        if (vertexExpression.getLabels().isEmpty()) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, vertexExpression.getSourceLocation(),
                    "VertexPatternExpr found without labels. Elements should have been resolved earlier.");
        }
        referencedVertexLabels.addAll(vertexExpression.getLabels());
        return vertexExpression;
    }
}
