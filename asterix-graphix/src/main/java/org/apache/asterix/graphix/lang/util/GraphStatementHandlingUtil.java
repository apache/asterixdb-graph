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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.asterix.common.api.IMetadataLockManager;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.graphix.app.translator.GraphixQueryTranslator;
import org.apache.asterix.graphix.common.metadata.EdgeIdentifier;
import org.apache.asterix.graphix.common.metadata.GraphIdentifier;
import org.apache.asterix.graphix.common.metadata.VertexIdentifier;
import org.apache.asterix.graphix.extension.GraphixMetadataExtension;
import org.apache.asterix.graphix.lang.clause.FromGraphClause;
import org.apache.asterix.graphix.lang.expression.GraphConstructor;
import org.apache.asterix.graphix.lang.rewrite.GraphixQueryRewriter;
import org.apache.asterix.graphix.lang.statement.CreateGraphStatement;
import org.apache.asterix.graphix.lang.statement.GraphDropStatement;
import org.apache.asterix.graphix.lang.statement.GraphElementDeclaration;
import org.apache.asterix.graphix.lang.visitor.base.AbstractGraphixQueryVisitor;
import org.apache.asterix.graphix.metadata.entity.dependency.DependencyIdentifier;
import org.apache.asterix.graphix.metadata.entity.dependency.GraphRequirements;
import org.apache.asterix.graphix.metadata.entity.dependency.IEntityRequirements;
import org.apache.asterix.graphix.metadata.entity.schema.Edge;
import org.apache.asterix.graphix.metadata.entity.schema.Graph;
import org.apache.asterix.graphix.metadata.entity.schema.Schema;
import org.apache.asterix.graphix.metadata.entity.schema.Vertex;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;

public final class GraphStatementHandlingUtil {
    public static void acquireGraphExtensionWriteLocks(MetadataProvider metadataProvider,
            DataverseName activeDataverseName, String graphName) throws AlgebricksException {
        // Acquire a READ lock on our dataverse and a WRITE lock on our graph.
        IMetadataLockManager metadataLockManager = metadataProvider.getApplicationContext().getMetadataLockManager();
        metadataLockManager.acquireDataverseReadLock(metadataProvider.getLocks(), activeDataverseName);
        metadataLockManager.acquireExtensionEntityWriteLock(metadataProvider.getLocks(),
                GraphixMetadataExtension.GRAPHIX_METADATA_EXTENSION_ID.getName(), activeDataverseName, graphName);
    }

    public static void throwIfDependentExists(MetadataTransactionContext mdTxnCtx,
            DependencyIdentifier dependencyIdentifier) throws AlgebricksException {
        for (IEntityRequirements requirements : GraphixMetadataExtension.getAllEntityRequirements(mdTxnCtx)) {
            for (DependencyIdentifier dependency : requirements) {
                if (dependency.equals(dependencyIdentifier)) {
                    throw new CompilationException(ErrorCode.CANNOT_DROP_OBJECT_DEPENDENT_EXISTS,
                            dependency.getDependencyKind(), dependency.getDisplayName(),
                            requirements.getDependentKind(), requirements.getDisplayName());
                }
            }
        }
    }

    public static void collectDependenciesOnGraph(Expression expression, DataverseName defaultDataverseName,
            Set<DependencyIdentifier> graphDependencies) throws CompilationException {
        expression.accept(new AbstractGraphixQueryVisitor() {
            @Override
            public Expression visit(FromGraphClause fromGraphClause, ILangExpression arg) {
                if (fromGraphClause.getGraphName() != null) {
                    String graphName = fromGraphClause.getGraphName().getValue();
                    DataverseName dataverseName = (fromGraphClause.getDataverseName() == null) ? defaultDataverseName
                            : fromGraphClause.getDataverseName();
                    DependencyIdentifier.Kind graphKind = DependencyIdentifier.Kind.GRAPH;
                    graphDependencies.add(new DependencyIdentifier(dataverseName, graphName, graphKind));
                }
                return null;
            }
        }, null);
    }

    public static void handleCreateGraph(CreateGraphStatement cgs, MetadataProvider metadataProvider,
            IStatementExecutor statementExecutor, DataverseName activeDataverseName) throws Exception {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);

        // Ensure that our active dataverse exists.
        Dataverse dataverse = MetadataManager.INSTANCE.getDataverse(mdTxnCtx, activeDataverseName);
        if (dataverse == null) {
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
        IEntityRequirements existingRequirements = null;
        if (existingGraph != null) {
            existingRequirements = GraphixMetadataExtension.getAllEntityRequirements(mdTxnCtx).stream()
                    .filter(r -> r.getDataverseName().equals(activeDataverseName))
                    .filter(r -> r.getEntityName().equals(cgs.getGraphName()))
                    .filter(r -> r.getDependentKind().equals(IEntityRequirements.DependentKind.GRAPH)).findFirst()
                    .orElseThrow(() -> new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE,
                            cgs.getSourceLocation(), "Graph dependencies for " + cgs.getGraphName()
                                    + " exist, but the graph itself does not."));
        }

        // Build the graph schema.
        GraphIdentifier graphIdentifier = new GraphIdentifier(activeDataverseName, cgs.getGraphName());
        Schema.Builder schemaBuilder = new Schema.Builder(graphIdentifier);
        List<GraphElementDeclaration> graphElementDeclarations = new ArrayList<>();
        for (GraphConstructor.VertexConstructor vertex : cgs.getVertexElements()) {
            Vertex schemaVertex =
                    schemaBuilder.addVertex(vertex.getLabel(), vertex.getPrimaryKeyFields(), vertex.getDefinition());
            switch (schemaBuilder.getLastError()) {
                case NO_ERROR:
                    VertexIdentifier id = schemaVertex.getIdentifier();
                    GraphElementDeclaration decl = new GraphElementDeclaration(id, vertex.getExpression());
                    decl.setSourceLocation(vertex.getSourceLocation());
                    graphElementDeclarations.add(decl);
                    break;

                case VERTEX_LABEL_CONFLICT:
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, vertex.getSourceLocation(),
                            "Conflicting vertex label found: " + vertex.getLabel());

                default:
                    throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, vertex.getSourceLocation(),
                            "Constructor vertex was not returned, but the error is not a vertex label conflict!");
            }
        }
        for (GraphConstructor.EdgeConstructor edge : cgs.getEdgeElements()) {
            Edge schemaEdge =
                    schemaBuilder.addEdge(edge.getEdgeLabel(), edge.getDestinationLabel(), edge.getSourceLabel(),
                            edge.getDestinationKeyFields(), edge.getSourceKeyFields(), edge.getDefinition());
            switch (schemaBuilder.getLastError()) {
                case NO_ERROR:
                    EdgeIdentifier id = schemaEdge.getIdentifier();
                    GraphElementDeclaration decl = new GraphElementDeclaration(id, edge.getExpression());
                    decl.setSourceLocation(edge.getSourceLocation());
                    graphElementDeclarations.add(decl);
                    break;

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

        GraphixQueryRewriter graphixQueryRewriter = ((GraphixQueryTranslator) statementExecutor).getQueryRewriter();
        metadataProvider.setDefaultDataverse(dataverse);
        DataverseName dataverseName = (cgs.getDataverseName() != null) ? cgs.getDataverseName() : activeDataverseName;
        GraphRequirements requirements = new GraphRequirements(dataverseName, cgs.getGraphName());
        for (GraphElementDeclaration graphElementDeclaration : graphElementDeclarations) {
            // Determine the graph dependencies using the raw body.
            Set<DependencyIdentifier> graphDependencies = new HashSet<>();
            collectDependenciesOnGraph(graphElementDeclaration.getRawBody(), activeDataverseName, graphDependencies);
            requirements.loadGraphDependencies(graphDependencies);

            // Verify that each element definition is usable.
            ((GraphixQueryTranslator) statementExecutor).setGraphElementNormalizedBody(metadataProvider,
                    graphElementDeclaration, graphixQueryRewriter);

            // Determine the non-graph dependencies using the normalized body.
            requirements.loadNonGraphDependencies(graphElementDeclaration.getNormalizedBody(), graphixQueryRewriter);
        }

        // Add / upsert our graph + requirements to our metadata.
        Graph newGraph = new Graph(graphIdentifier, schemaBuilder.build());
        if (existingGraph == null) {
            MetadataManager.INSTANCE.addEntity(mdTxnCtx, newGraph);
            MetadataManager.INSTANCE.addEntity(mdTxnCtx, requirements);

        } else {
            requirements.setPrimaryKeyValue(existingRequirements.getPrimaryKeyValue());
            MetadataManager.INSTANCE.upsertEntity(mdTxnCtx, newGraph);
            MetadataManager.INSTANCE.upsertEntity(mdTxnCtx, requirements);
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

        // Verify that no requirements exist on our graph.
        DataverseName dataverseName = (gds.getDataverseName() == null) ? activeDataverseName : gds.getDataverseName();
        DependencyIdentifier dependencyIdentifier =
                new DependencyIdentifier(dataverseName, gds.getGraphName(), DependencyIdentifier.Kind.GRAPH);
        throwIfDependentExists(mdTxnCtx, dependencyIdentifier);
        Optional<IEntityRequirements> requirements = GraphixMetadataExtension.getAllEntityRequirements(mdTxnCtx)
                .stream().filter(r -> r.getDataverseName().equals(dataverseName))
                .filter(r -> r.getEntityName().equals(gds.getGraphName()))
                .filter(r -> r.getDependentKind().equals(IEntityRequirements.DependentKind.GRAPH)).findFirst();
        if (requirements.isPresent()) {
            MetadataManager.INSTANCE.deleteEntity(mdTxnCtx, requirements.get());
        }

        // Finally, perform the deletion.
        MetadataManager.INSTANCE.deleteEntity(mdTxnCtx, graph);
        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
    }
}
