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

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.graphix.algebra.compiler.provider.GraphixCompilationProvider;
import org.apache.asterix.graphix.extension.GraphixMetadataExtension;
import org.apache.asterix.graphix.lang.clause.FromGraphClause;
import org.apache.asterix.graphix.lang.expression.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrites.resolve.IGraphElementResolver;
import org.apache.asterix.graphix.lang.rewrites.resolve.InferenceBasedResolver;
import org.apache.asterix.graphix.lang.rewrites.resolve.NoResolutionResolver;
import org.apache.asterix.graphix.lang.rewrites.resolve.SchemaKnowledgeTable;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.graphix.lang.struct.ElementLabel;
import org.apache.asterix.graphix.metadata.entity.schema.Graph;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Resolve graph element labels and edge directions in our AST. We assume that all graph elements have a variable.
 *
 * @see NoResolutionResolver
 * @see InferenceBasedResolver
 */
public class ElementResolutionVisitor extends AbstractGraphixQueryVisitor {
    private static final Logger LOGGER = LogManager.getLogger(ElementResolutionVisitor.class);

    // If we exceed 500 iterations, something is probably wrong... log this.
    private static final long DEFAULT_RESOLVER_ITERATION_MAX = 500;

    private final MetadataProvider metadataProvider;
    private final IWarningCollector warningCollector;

    public ElementResolutionVisitor(MetadataProvider metadataProvider, IWarningCollector warningCollector) {
        this.metadataProvider = metadataProvider;
        this.warningCollector = warningCollector;
    }

    @Override
    public Expression visit(FromGraphClause fromGraphClause, ILangExpression arg) throws CompilationException {
        // Establish our schema knowledge.
        SchemaKnowledgeTable schemaKnowledgeTable;
        if (fromGraphClause.getGraphConstructor() == null) {
            DataverseName dataverseName = (fromGraphClause.getDataverseName() == null)
                    ? metadataProvider.getDefaultDataverseName() : fromGraphClause.getDataverseName();
            Identifier graphName = fromGraphClause.getGraphName();

            // Fetch the graph from our metadata.
            try {
                Graph graphFromMetadata = GraphixMetadataExtension.getGraph(metadataProvider.getMetadataTxnContext(),
                        dataverseName, graphName.getValue());
                if (graphFromMetadata == null) {
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, fromGraphClause.getSourceLocation(),
                            "Graph " + graphName.getValue() + " does not exist.");
                }
                schemaKnowledgeTable = new SchemaKnowledgeTable(graphFromMetadata);

            } catch (AlgebricksException e) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, fromGraphClause.getSourceLocation(),
                        "Graph " + graphName.getValue() + " does not exist.");
            }

        } else {
            schemaKnowledgeTable = new SchemaKnowledgeTable(fromGraphClause.getGraphConstructor());
        }

        // Determine our resolution strategy. By default, we will perform bare-bones resolution.
        IGraphElementResolver graphElementResolver;
        String resolverMetadataKeyName = GraphixCompilationProvider.RESOLVER_METADATA_CONFIG;
        if (metadataProvider.getConfig().containsKey(resolverMetadataKeyName)) {
            String resolverProperty = metadataProvider.getProperty(resolverMetadataKeyName);
            if (resolverProperty.equalsIgnoreCase(NoResolutionResolver.METADATA_CONFIG_NAME)) {
                graphElementResolver = new NoResolutionResolver();

            } else if (resolverProperty.equalsIgnoreCase(InferenceBasedResolver.METADATA_CONFIG_NAME)) {
                graphElementResolver = new InferenceBasedResolver(schemaKnowledgeTable);

            } else {
                throw new CompilationException(ErrorCode.ILLEGAL_SET_PARAMETER, resolverProperty);
            }

        } else {
            graphElementResolver = new InferenceBasedResolver(schemaKnowledgeTable);
        }

        // Perform our resolution passes (repeat until we reach a fixed point or the iteration max).
        String resolverIterationMaxMetadataKeyName = GraphixCompilationProvider.RESOLVER_ITERATION_MAX_METADATA_CONFIG;
        long resolverIterationMax;
        if (metadataProvider.getConfig().containsKey(resolverIterationMaxMetadataKeyName)) {
            String resolverIterationMaxProperty = metadataProvider.getProperty(resolverIterationMaxMetadataKeyName);
            try {
                resolverIterationMax = Long.parseLong(resolverIterationMaxProperty);

            } catch (NumberFormatException e) {
                throw new CompilationException(ErrorCode.ILLEGAL_SET_PARAMETER, resolverIterationMaxProperty);
            }

        } else {
            resolverIterationMax = DEFAULT_RESOLVER_ITERATION_MAX;
        }
        for (int i = 0; i < resolverIterationMax && !graphElementResolver.isAtFixedPoint(); i++) {
            graphElementResolver.resolve(fromGraphClause);
            if (i == resolverIterationMax - 1) {
                LOGGER.warn("Number of iterations for element resolution has exceeded " + resolverIterationMax);
            }
        }

        // Perform the final pass of our FROM-GRAPH-CLAUSE. Raise warnings if necessary.
        new AbstractGraphixQueryVisitor() {
            @Override
            public Expression visit(VertexPatternExpr vertexPatternExpr, ILangExpression arg) {
                Identifier vertexIdentifier = vertexPatternExpr.getVariableExpr().getVar();
                if (vertexPatternExpr.getLabels().isEmpty()) {
                    vertexPatternExpr.getLabels().addAll(schemaKnowledgeTable.getVertexLabelSet());
                    if (schemaKnowledgeTable.getVertexLabelSet().size() > 1 && warningCollector.shouldWarn()) {
                        warningCollector.warn(Warning.of(vertexPatternExpr.getSourceLocation(),
                                ErrorCode.COMPILATION_ERROR, "Vertex label could not be resolved. Assuming that"
                                        + " vertex " + vertexIdentifier + " has all schema labels."));
                    }

                } else if (vertexPatternExpr.getLabels().stream().allMatch(ElementLabel::isInferred)
                        && vertexPatternExpr.getLabels().size() > 1) {
                    warningCollector.warn(Warning.of(vertexPatternExpr.getSourceLocation(), ErrorCode.COMPILATION_ERROR,
                            "More than one label has been resolved for vertex " + vertexIdentifier + "."));
                }
                return vertexPatternExpr;
            }

            @Override
            public Expression visit(EdgePatternExpr edgePatternExpr, ILangExpression arg) throws CompilationException {
                EdgeDescriptor edgeDescriptor = edgePatternExpr.getEdgeDescriptor();
                Identifier edgeIdentifier = edgeDescriptor.getVariableExpr().getVar();
                if (edgeDescriptor.getEdgeLabels().isEmpty()) {
                    edgeDescriptor.getEdgeLabels().addAll(schemaKnowledgeTable.getEdgeLabelSet());
                    if (edgeDescriptor.getEdgeLabels().size() > 1 && warningCollector.shouldWarn()) {
                        warningCollector.warn(Warning.of(edgePatternExpr.getSourceLocation(),
                                ErrorCode.COMPILATION_ERROR, "Edge label for " + edgeIdentifier + " could not be"
                                        + " resolved. Assuming that this edge has all schema labels."));
                    }
                } else if (edgeDescriptor.getEdgeLabels().stream().allMatch(ElementLabel::isInferred)
                        && edgeDescriptor.getEdgeLabels().size() > 1) {
                    warningCollector.warn(Warning.of(edgePatternExpr.getSourceLocation(), ErrorCode.COMPILATION_ERROR,
                            "More than one label has been resolved for edge " + edgeIdentifier + "."));
                }
                for (VertexPatternExpr internalVertex : edgePatternExpr.getInternalVertices()) {
                    internalVertex.accept(this, arg);
                }
                return edgePatternExpr;
            }
        }.visit(fromGraphClause, null);

        return null;
    }
}
