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
package org.apache.asterix.graphix.lang.util;

import java.rmi.RemoteException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.asterix.common.api.IMetadataLockManager;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.graphix.app.translator.GraphixQueryTranslator;
import org.apache.asterix.graphix.common.metadata.GraphElementIdentifier;
import org.apache.asterix.graphix.common.metadata.GraphIdentifier;
import org.apache.asterix.graphix.extension.GraphixMetadataExtension;
import org.apache.asterix.graphix.lang.expression.GraphConstructor;
import org.apache.asterix.graphix.lang.rewrites.GraphixQueryRewriter;
import org.apache.asterix.graphix.lang.statement.CreateGraphStatement;
import org.apache.asterix.graphix.lang.statement.GraphDropStatement;
import org.apache.asterix.graphix.lang.statement.GraphElementDecl;
import org.apache.asterix.graphix.metadata.entities.Graph;
import org.apache.asterix.graphix.metadata.entities.GraphDependencies;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;

public final class GraphStatementHandlingUtil {
    public static void acquireGraphWriteLocks(MetadataProvider metadataProvider, DataverseName activeDataverseName,
            String graphName) throws AlgebricksException {
        // Acquire a READ lock on our dataverse and a WRITE lock on our graph.
        IMetadataLockManager metadataLockManager = metadataProvider.getApplicationContext().getMetadataLockManager();
        metadataLockManager.acquireDataverseReadLock(metadataProvider.getLocks(), activeDataverseName);
        metadataLockManager.acquireExtensionEntityWriteLock(metadataProvider.getLocks(),
                GraphixMetadataExtension.GRAPHIX_METADATA_EXTENSION_ID.getName(), activeDataverseName, graphName);
    }

    public static void handleCreateGraph(CreateGraphStatement cgs, MetadataProvider metadataProvider,
            IStatementExecutor statementExecutor, DataverseName activeDataverseName) throws Exception {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);

        // Ensure that our active dataverse exists.
        Dataverse dv = MetadataManager.INSTANCE.getDataverse(mdTxnCtx, activeDataverseName);
        if (dv == null) {
            throw new CompilationException(ErrorCode.UNKNOWN_DATAVERSE, cgs.getSourceLocation(), activeDataverseName);
        }

        // If a graph already exists, skip.
        Graph existingGraph = GraphixMetadataExtension.getGraph(mdTxnCtx, activeDataverseName, cgs.getGraphName());
        if (existingGraph != null) {
            if (cgs.isIfNotExists()) {
                MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                return;

            } else if (!cgs.isReplaceIfExists()) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, cgs.getSourceLocation(),
                        "Graph " + existingGraph.getGraphName() + " already exists.");
            }
        }

        // Build the graph schema.
        GraphIdentifier graphIdentifier = new GraphIdentifier(activeDataverseName, cgs.getGraphName());
        Graph.Schema.Builder schemaBuilder = new Graph.Schema.Builder(graphIdentifier);
        Map<GraphElementIdentifier, GraphElementDecl> graphElementDecls = new LinkedHashMap<>();
        for (GraphConstructor.VertexElement vertex : cgs.getVertexElements()) {
            Graph.Vertex schemaVertex =
                    schemaBuilder.addVertex(vertex.getLabel(), vertex.getPrimaryKeyFields(), vertex.getDefinition());
            switch (schemaBuilder.getLastError()) {
                case NO_ERROR:
                    GraphElementIdentifier id = schemaVertex.getIdentifier();
                    if (graphElementDecls.containsKey(id)) {
                        graphElementDecls.get(id).getBodies().add(vertex.getExpression());

                    } else {
                        GraphElementDecl decl = new GraphElementDecl(id, vertex.getExpression());
                        decl.setSourceLocation(vertex.getSourceLocation());
                        graphElementDecls.put(schemaVertex.getIdentifier(), decl);
                    }
                    break;

                case CONFLICTING_PRIMARY_KEY:
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, vertex.getSourceLocation(),
                            "Conflicting primary keys for vertices with label " + vertex.getLabel());

                default:
                    throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, vertex.getSourceLocation(),
                            "Schema vertex was not returned, but the error is not a conflicting primary key!");
            }
        }
        for (GraphConstructor.EdgeElement edge : cgs.getEdgeElements()) {
            Graph.Edge schemaEdge;
            if (edge.getDefinition() == null) {
                schemaEdge = schemaBuilder.addEdge(edge.getEdgeLabel(), edge.getDestinationLabel(),
                        edge.getSourceLabel(), edge.getDestinationKeyFields());

            } else {
                schemaEdge = schemaBuilder.addEdge(edge.getEdgeLabel(), edge.getDestinationLabel(),
                        edge.getSourceLabel(), edge.getPrimaryKeyFields(), edge.getDestinationKeyFields(),
                        edge.getSourceKeyFields(), edge.getDefinition());
            }

            switch (schemaBuilder.getLastError()) {
                case NO_ERROR:
                    if (edge.getDefinition() != null) {
                        GraphElementIdentifier id = schemaEdge.getIdentifier();
                        if (graphElementDecls.containsKey(id)) {
                            graphElementDecls.get(id).getBodies().add(edge.getExpression());

                        } else {
                            GraphElementDecl decl = new GraphElementDecl(id, edge.getExpression());
                            decl.setSourceLocation(edge.getSourceLocation());
                            graphElementDecls.put(id, decl);
                        }
                    }
                    break;

                case SOURCE_VERTEX_NOT_FOUND:
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, edge.getSourceLocation(),
                            "Source vertex " + edge.getSourceLabel() + " not found in the edge " + edge.getEdgeLabel()
                                    + ".");

                case DESTINATION_VERTEX_NOT_FOUND:
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, edge.getSourceLocation(),
                            "Destination vertex " + edge.getDestinationLabel() + " not found in the edge "
                                    + edge.getEdgeLabel() + ".");

                case CONFLICTING_PRIMARY_KEY:
                case CONFLICTING_SOURCE_VERTEX:
                case CONFLICTING_DESTINATION_VERTEX:
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, edge.getSourceLocation(),
                            "Conflicting edge with the same label found: " + edge.getEdgeLabel());

                default:
                    throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, edge.getSourceLocation(),
                            "Schema edge was not returned, and an unexpected error encountered");
            }
        }

        // Verify that each element definition is usable.
        GraphixQueryRewriter graphixQueryRewriter = ((GraphixQueryTranslator) statementExecutor).getQueryRewriter();
        metadataProvider.setDefaultDataverse(dv);
        for (GraphElementDecl graphElementDecl : graphElementDecls.values()) {
            ((GraphixQueryTranslator) statementExecutor).setGraphElementNormalizedBody(metadataProvider,
                    graphElementDecl, graphixQueryRewriter);
        }

        // Build our dependencies (collected over all graph element bodies).
        GraphDependencies graphDependencies = new GraphDependencies();
        for (GraphElementDecl graphElementDecl : graphElementDecls.values()) {
            if (graphElementDecl.getNormalizedBodies().size() != graphElementDecl.getBodies().size()) {
                // We should have set the normalized body by calling {@code normalizeGraphElementAsQuery} beforehand.
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE,
                        graphElementDecl.getSourceLocation(), "Normalized body not found!");
            }
            for (Expression normalizedBody : graphElementDecl.getNormalizedBodies()) {
                graphDependencies.collectDependencies(normalizedBody, graphixQueryRewriter);
            }
        }

        // Add / upsert our graph to our metadata.
        Graph newGraph = new Graph(graphIdentifier, schemaBuilder.build(), graphDependencies);
        if (existingGraph == null) {
            MetadataManager.INSTANCE.addEntity(mdTxnCtx, newGraph);

        } else {
            MetadataManager.INSTANCE.upsertEntity(mdTxnCtx, newGraph);
        }
        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
    }

    public static void handleGraphDrop(GraphDropStatement gds, MetadataProvider metadataProvider,
            DataverseName activeDataverseName) throws RemoteException, AlgebricksException {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);

        // Verify that our active dataverse exists.
        Dataverse dataverse = MetadataManager.INSTANCE.getDataverse(mdTxnCtx, activeDataverseName);
        if (dataverse == null) {
            if (gds.getIfExists()) {
                MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                return;

            } else {
                throw new CompilationException(ErrorCode.UNKNOWN_DATAVERSE, gds.getSourceLocation(),
                        activeDataverseName);
            }
        }

        // Verify that our graph exists. If it does not, skip.
        Graph graph = GraphixMetadataExtension.getGraph(mdTxnCtx, activeDataverseName, gds.getGraphName());
        if (graph == null) {
            if (gds.getIfExists()) {
                MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                return;

            } else {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, gds.getSourceLocation(),
                        "Graph " + gds.getGraphName() + " does not exist.");
            }
        }

        MetadataManager.INSTANCE.deleteEntity(mdTxnCtx, graph);
        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
    }
}
