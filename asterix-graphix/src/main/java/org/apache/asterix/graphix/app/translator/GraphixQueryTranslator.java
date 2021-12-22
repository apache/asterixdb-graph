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
package org.apache.asterix.graphix.app.translator;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import org.apache.asterix.app.translator.QueryTranslator;
import org.apache.asterix.common.api.IResponsePrinter;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.graphix.extension.GraphixMetadataExtension;
import org.apache.asterix.graphix.lang.expression.GraphElementExpr;
import org.apache.asterix.graphix.lang.rewrites.GraphixQueryRewriter;
import org.apache.asterix.graphix.lang.rewrites.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.statement.GraphDropStatement;
import org.apache.asterix.graphix.lang.statement.GraphElementDecl;
import org.apache.asterix.graphix.metadata.entities.Graph;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.statement.DataverseDropStatement;
import org.apache.asterix.lang.common.statement.DropDatasetStatement;
import org.apache.asterix.lang.common.statement.FunctionDropStatement;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.statement.SynonymDropStatement;
import org.apache.asterix.lang.common.statement.ViewDropStatement;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.util.ExpressionUtils;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.DependencyKind;
import org.apache.asterix.translator.IRequestParameters;
import org.apache.asterix.translator.SessionOutput;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.api.client.IHyracksClientConnection;

public class GraphixQueryTranslator extends QueryTranslator {
    public GraphixQueryTranslator(ICcApplicationContext appCtx, List<Statement> statements, SessionOutput output,
            ILangCompilationProvider compilationProvider, ExecutorService executorService,
            IResponsePrinter responsePrinter) {
        super(appCtx, statements, output, compilationProvider, executorService, responsePrinter);
    }

    public GraphixQueryRewriter getQueryRewriter() {
        return (GraphixQueryRewriter) rewriterFactory.createQueryRewriter();
    }

    public void setGraphElementNormalizedBody(MetadataProvider metadataProvider, GraphElementDecl graphElementDecl,
            GraphixQueryRewriter queryRewriter) throws CompilationException {
        // Create a query AST for our rewriter to walk through.
        GraphElementExpr functionCall = new GraphElementExpr(graphElementDecl.getIdentifier());
        Query query = ExpressionUtils.createWrappedQuery(functionCall, graphElementDecl.getSourceLocation());

        // We call our rewriter to set the normalized bodies of {@code graphElementDecl}.
        GraphixRewritingContext graphixRewritingContext =
                new GraphixRewritingContext(metadataProvider, declaredFunctions, null,
                        Collections.singletonList(graphElementDecl), warningCollector, query.getVarCounter());
        queryRewriter.loadNormalizedGraphElements(graphixRewritingContext, query);
    }

    @Override
    protected void handleDropSynonymStatement(MetadataProvider metadataProvider, Statement stmt) throws Exception {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);

        // Forbid dropping the synonym if any graphs depend on this synonym.
        DataverseName workingDataverse = getActiveDataverseName(((SynonymDropStatement) stmt).getDataverseName());
        String synonymName = ((SynonymDropStatement) stmt).getSynonymName();
        throwErrorIfDependentExists(mdTxnCtx, workingDataverse,
                (dependency) -> dependency.first == DependencyKind.SYNONYM
                        && dependency.second.second.equals(synonymName));

        // Finish this transaction and perform the remainder of the DROP SYNONYM statement.
        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        super.handleDropSynonymStatement(metadataProvider, stmt);
    }

    @Override
    protected void handleFunctionDropStatement(MetadataProvider metadataProvider, Statement stmt,
            IRequestParameters requestParameters) throws Exception {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);

        // Forbid dropping the function if any graphs depend on this function.
        FunctionSignature functionSignature = ((FunctionDropStatement) stmt).getFunctionSignature();
        DataverseName workingDataverse = getActiveDataverseName(functionSignature.getDataverseName());
        throwErrorIfDependentExists(mdTxnCtx, workingDataverse,
                (dependency) -> dependency.first == DependencyKind.FUNCTION
                        && dependency.second.second.equals(functionSignature.getName())
                        && dependency.second.third.equals(Integer.toString(functionSignature.getArity())));

        // Finish this transaction and perform the remainder of the DROP FUNCTION statement.
        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        super.handleFunctionDropStatement(metadataProvider, stmt, requestParameters);
    }

    @Override
    public void handleViewDropStatement(MetadataProvider metadataProvider, Statement stmt) throws Exception {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);

        // Forbid dropping the dataset if any graphs depend on this dataset.
        DataverseName workingDataverse = getActiveDataverseName(((ViewDropStatement) stmt).getDataverseName());
        Identifier dsId = ((ViewDropStatement) stmt).getViewName();
        throwErrorIfDependentExists(mdTxnCtx, workingDataverse,
                (dependency) -> dependency.first == DependencyKind.DATASET
                        && dependency.second.second.equals(dsId.getValue()));

        // Finish this transaction and perform the remainder of the DROP VIEW statement.
        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        super.handleViewDropStatement(metadataProvider, stmt);
    }

    @Override
    public void handleDatasetDropStatement(MetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc, IRequestParameters requestParameters) throws Exception {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);

        // Forbid dropping the dataset if any graphs depend on this dataset.
        DataverseName workingDataverse = getActiveDataverseName(((DropDatasetStatement) stmt).getDataverseName());
        Identifier dsId = ((DropDatasetStatement) stmt).getDatasetName();
        throwErrorIfDependentExists(mdTxnCtx, workingDataverse,
                (dependency) -> dependency.first == DependencyKind.DATASET
                        && dependency.second.second.equals(dsId.getValue()));

        // Finish this transaction and perform the remainder of the DROP DATASET statement.
        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        super.handleDatasetDropStatement(metadataProvider, stmt, hcc, requestParameters);
    }

    private void throwErrorIfDependentExists(MetadataTransactionContext mdTxnCtx, DataverseName workingDataverse,
            Function<Pair<DependencyKind, Triple<DataverseName, String, String>>, Boolean> isEntityDependent)
            throws AlgebricksException {
        List<Graph> allGraphs = GraphixMetadataExtension.getGraphs(mdTxnCtx, workingDataverse);
        for (Graph graph : allGraphs) {
            if (!graph.getDataverseName().equals(workingDataverse)) {
                continue;
            }
            Iterator<Pair<DependencyKind, Triple<DataverseName, String, String>>> dependencyIterator =
                    graph.getDependencies().getIterator();
            while (dependencyIterator.hasNext()) {
                Pair<DependencyKind, Triple<DataverseName, String, String>> dependency = dependencyIterator.next();
                if (isEntityDependent.apply(dependency)) {
                    throw new CompilationException(ErrorCode.CANNOT_DROP_OBJECT_DEPENDENT_EXISTS, dependency.first,
                            dependency.first.getDependencyDisplayName(dependency.second), "graph",
                            graph.getGraphName());
                }
            }
        }
    }

    @Override
    protected void handleDataverseDropStatement(MetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc, IRequestParameters requestParameters) throws Exception {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);

        // Forbid dropping this dataverse if other graphs that are outside this dataverse depend on this dataverse.
        DataverseName droppedDataverse = ((DataverseDropStatement) stmt).getDataverseName();
        List<Graph> allGraphs = GraphixMetadataExtension.getGraphs(mdTxnCtx, null);
        for (Graph graph : allGraphs) {
            if (graph.getDataverseName().equals(droppedDataverse)) {
                continue;
            }
            Iterator<Pair<DependencyKind, Triple<DataverseName, String, String>>> dependencyIterator =
                    graph.getDependencies().getIterator();
            while (dependencyIterator.hasNext()) {
                Pair<DependencyKind, Triple<DataverseName, String, String>> dependency = dependencyIterator.next();
                if (dependency.second.first.equals(droppedDataverse)) {
                    throw new CompilationException(ErrorCode.CANNOT_DROP_DATAVERSE_DEPENDENT_EXISTS, dependency.first,
                            dependency.first.getDependencyDisplayName(dependency.second), "graph",
                            graph.getGraphName());
                }
            }
        }

        // Perform a drop for all graphs contained in this dataverse.
        MetadataProvider tempMdProvider = MetadataProvider.create(appCtx, metadataProvider.getDefaultDataverse());
        tempMdProvider.getConfig().putAll(metadataProvider.getConfig());
        for (Graph graph : allGraphs) {
            if (!graph.getDataverseName().equals(droppedDataverse)) {
                continue;
            }
            tempMdProvider.getLocks().reset();
            GraphDropStatement gds = new GraphDropStatement(droppedDataverse, graph.getGraphName(), false);
            gds.handle(hcc, this, requestParameters, tempMdProvider, 0);
        }

        // Finish this transaction and perform the remainder of the DROP DATAVERSE statement.
        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        super.handleDataverseDropStatement(metadataProvider, stmt, hcc, requestParameters);
    }
}
