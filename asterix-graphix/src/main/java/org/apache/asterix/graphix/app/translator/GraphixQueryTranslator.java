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

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.apache.asterix.app.translator.QueryTranslator;
import org.apache.asterix.common.api.IResponsePrinter;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.graphix.extension.GraphixMetadataExtension;
import org.apache.asterix.graphix.lang.rewrites.GraphixQueryRewriter;
import org.apache.asterix.graphix.lang.rewrites.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.statement.DeclareGraphStatement;
import org.apache.asterix.graphix.lang.statement.GraphDropStatement;
import org.apache.asterix.graphix.lang.statement.GraphElementDeclaration;
import org.apache.asterix.graphix.lang.util.GraphStatementHandlingUtil;
import org.apache.asterix.graphix.metadata.entity.dependency.DependencyIdentifier;
import org.apache.asterix.graphix.metadata.entity.dependency.FunctionRequirements;
import org.apache.asterix.graphix.metadata.entity.dependency.IEntityRequirements;
import org.apache.asterix.graphix.metadata.entity.dependency.ViewRequirements;
import org.apache.asterix.graphix.metadata.entity.schema.Graph;
import org.apache.asterix.lang.common.base.IStatementRewriter;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.statement.CreateFunctionStatement;
import org.apache.asterix.lang.common.statement.CreateViewStatement;
import org.apache.asterix.lang.common.statement.DataverseDropStatement;
import org.apache.asterix.lang.common.statement.DropDatasetStatement;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.statement.FunctionDropStatement;
import org.apache.asterix.lang.common.statement.SynonymDropStatement;
import org.apache.asterix.lang.common.statement.ViewDecl;
import org.apache.asterix.lang.common.statement.ViewDropStatement;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.translator.IRequestParameters;
import org.apache.asterix.translator.SessionOutput;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.exceptions.IWarningCollector;

public class GraphixQueryTranslator extends QueryTranslator {
    Set<DeclareGraphStatement> declareGraphStatements = new HashSet<>();

    public GraphixQueryTranslator(ICcApplicationContext appCtx, List<Statement> statements, SessionOutput output,
            ILangCompilationProvider compilationProvider, ExecutorService executorService,
            IResponsePrinter responsePrinter) {
        super(appCtx, statements, output, compilationProvider, executorService, responsePrinter);
    }

    public GraphixQueryRewriter getQueryRewriter() {
        return (GraphixQueryRewriter) rewriterFactory.createQueryRewriter();
    }

    public void addDeclaredGraph(DeclareGraphStatement declareGraphStatement) {
        declareGraphStatements.add(declareGraphStatement);
    }

    public void setGraphElementNormalizedBody(MetadataProvider metadataProvider,
            GraphElementDeclaration graphElementDeclaration, GraphixQueryRewriter queryRewriter)
            throws CompilationException {
        queryRewriter.loadNormalizedGraphElement((GraphixRewritingContext) createLangRewritingContext(metadataProvider,
                declaredFunctions, null, warningCollector, 0), graphElementDeclaration);
    }

    @Override
    protected LangRewritingContext createLangRewritingContext(MetadataProvider metadataProvider,
            List<FunctionDecl> declaredFunctions, List<ViewDecl> declaredViews, IWarningCollector warningCollector,
            int varCounter) {
        return new GraphixRewritingContext(metadataProvider, declaredFunctions, declaredViews, declareGraphStatements,
                warningCollector, varCounter);
    }

    /**
     * To create a view, we must perform the following:
     * a) Check the view body for any named graphs.
     * b) Rewrite graph expressions into pure SQL++ expressions. The dependencies associated with the rewritten
     * expressions will be recorded in the "Dataset" dataset.
     * c) Record any graph-related dependencies for the view in our metadata.
     */
    @Override
    protected CreateResult doCreateView(MetadataProvider metadataProvider, CreateViewStatement cvs,
            DataverseName dataverseName, String viewName, DataverseName itemTypeDataverseName, String itemTypeName,
            IStatementRewriter stmtRewriter, IRequestParameters requestParameters) throws Exception {
        // Before executing our parent, analyze our view body for graph dependencies.
        Set<DependencyIdentifier> graphDependencies = new HashSet<>();
        GraphStatementHandlingUtil.collectDependenciesOnGraph(cvs.getViewBodyExpression(), dataverseName,
                graphDependencies);

        // Now execute the parent CREATE-VIEW function. Ensure that our VIEW is valid.
        CreateResult createResult = super.doCreateView(metadataProvider, cvs, dataverseName, viewName,
                itemTypeDataverseName, itemTypeName, stmtRewriter, requestParameters);
        if (createResult == CreateResult.NOOP) {
            return createResult;
        }

        // Our view is valid. Proceed by inserting / upserting a new dependence record.
        GraphStatementHandlingUtil.acquireGraphExtensionWriteLocks(metadataProvider, dataverseName, viewName);
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);

        // If we have an existing graph-dependency, fetch it first.
        Optional<IEntityRequirements> existingRequirements = GraphixMetadataExtension.getAllEntityRequirements(mdTxnCtx)
                .stream().filter(r -> r.getDataverseName().equals(dataverseName))
                .filter(r -> r.getEntityName().equals(cvs.getViewName()))
                .filter(r -> r.getDependentKind() == IEntityRequirements.DependentKind.VIEW).findFirst();
        if (existingRequirements.isPresent() && !cvs.getReplaceIfExists()) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, cvs.getSourceLocation(),
                    "Graph dependency record for " + cvs.getViewName() + " already exists.");
        }

        // Insert / upsert into our GraphDependency dataset.
        if (!graphDependencies.isEmpty()) {
            ViewRequirements viewRequirements = new ViewRequirements(getActiveDataverseName(cvs.getDataverseName()),
                    cvs.getViewName(), graphDependencies);
            if (!existingRequirements.isPresent()) {
                MetadataManager.INSTANCE.addEntity(mdTxnCtx, viewRequirements);

            } else {
                MetadataManager.INSTANCE.upsertEntity(mdTxnCtx, viewRequirements);
            }
        }
        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);

        // Exit here. Our parent will release all locks.
        return createResult;
    }

    /**
     * To create a function, we must perform the following:
     * a) Check the function body for any named graphs.
     * b) Rewrite graph expressions into pure SQL++ expressions. The dependencies associated with the rewritten
     * expressions will be recorded in the "Function" dataset.
     * c) Record any graph-related dependencies for the function in our metadata.
     */
    @Override
    protected CreateResult doCreateFunction(MetadataProvider metadataProvider, CreateFunctionStatement cfs,
            FunctionSignature functionSignature, IStatementRewriter stmtRewriter, IRequestParameters requestParameters)
            throws Exception {
        // Before executing our parent, analyze our function body for graph dependencies.
        Set<DependencyIdentifier> graphDependencies = new HashSet<>();
        GraphStatementHandlingUtil.collectDependenciesOnGraph(cfs.getFunctionBodyExpression(),
                cfs.getFunctionSignature().getDataverseName(), graphDependencies);

        // Execute the parent CREATE-FUNCTION function. Ensure that our FUNCTION is valid.
        CreateResult createResult =
                super.doCreateFunction(metadataProvider, cfs, functionSignature, stmtRewriter, requestParameters);
        if (createResult == CreateResult.NOOP) {
            return createResult;
        }

        // Our function is valid. Proceed by inserting / upserting a new dependence record.
        DataverseName dataverseName = functionSignature.getDataverseName();
        GraphStatementHandlingUtil.acquireGraphExtensionWriteLocks(metadataProvider, dataverseName,
                cfs.getFunctionSignature().toString());
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);

        // If we have an existing requirement record, fetch it first.
        DataverseName activeDataverseName = functionSignature.getDataverseName();
        Optional<FunctionRequirements> existingRequirements = GraphixMetadataExtension
                .getAllEntityRequirements(mdTxnCtx).stream()
                .filter(r -> r.getDataverseName().equals(activeDataverseName))
                .filter(r -> r.getDependentKind() == IEntityRequirements.DependentKind.FUNCTION)
                .map(r -> (FunctionRequirements) r).filter(f -> f.getEntityName().equals(functionSignature.getName()))
                .filter(f -> Integer.parseInt(f.getArityAsString()) == functionSignature.getArity()).findFirst();
        if (existingRequirements.isPresent() && !cfs.getReplaceIfExists()) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, cfs.getSourceLocation(),
                    "Graph dependency record for " + functionSignature.getName() + " already exists.");
        }

        // Insert / upsert into our GraphDependency dataset.
        if (!graphDependencies.isEmpty()) {
            FunctionRequirements requirements = new FunctionRequirements(cfs.getFunctionSignature(), graphDependencies);
            if (!existingRequirements.isPresent()) {
                MetadataManager.INSTANCE.addEntity(mdTxnCtx, requirements);

            } else {
                MetadataManager.INSTANCE.upsertEntity(mdTxnCtx, requirements);
            }
        }
        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);

        // Exit here. Our parent will release all locks.
        return createResult;
    }

    @Override
    protected void handleDropSynonymStatement(MetadataProvider metadataProvider, Statement stmt) throws Exception {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);

        // Forbid dropping the synonym if any graphs depend on this synonym.
        SynonymDropStatement sds = (SynonymDropStatement) stmt;
        DataverseName workingDataverse = getActiveDataverseName(sds.getDataverseName());
        DependencyIdentifier dependencyIdentifier =
                new DependencyIdentifier(workingDataverse, sds.getSynonymName(), DependencyIdentifier.Kind.SYNONYM);
        GraphStatementHandlingUtil.throwIfDependentExists(mdTxnCtx, dependencyIdentifier);

        // Finish this transaction and perform the remainder of the DROP SYNONYM statement.
        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        super.handleDropSynonymStatement(metadataProvider, stmt);
    }

    /**
     * Before dropping a function, we perform the following:
     * 1. Check if any of our existing graphs depend on the function to-be-dropped.
     * 2. Remove the GraphDependency record for the function to-be-dropped if it exists.
     */
    @Override
    protected void handleFunctionDropStatement(MetadataProvider metadataProvider, Statement stmt,
            IRequestParameters requestParameters) throws Exception {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);

        // Forbid dropping the function if any graphs depend on this function.
        FunctionSignature functionSignature = ((FunctionDropStatement) stmt).getFunctionSignature();
        DataverseName workingDataverse = getActiveDataverseName(functionSignature.getDataverseName());
        DependencyIdentifier dependencyIdentifier =
                new DependencyIdentifier(workingDataverse, functionSignature.getName(),
                        String.valueOf(functionSignature.getArity()), DependencyIdentifier.Kind.FUNCTION);
        GraphStatementHandlingUtil.throwIfDependentExists(mdTxnCtx, dependencyIdentifier);

        // Drop the GraphDependency record associated with this function, if it exists.
        Optional<FunctionRequirements> existingRequirements = GraphixMetadataExtension
                .getAllEntityRequirements(mdTxnCtx).stream().filter(r -> r.getDataverseName().equals(workingDataverse))
                .filter(r -> r.getEntityName().equals(functionSignature.getName()))
                .filter(r -> r.getDependentKind() == IEntityRequirements.DependentKind.FUNCTION)
                .map(r -> (FunctionRequirements) r)
                .filter(f -> Integer.parseInt(f.getArityAsString()) == functionSignature.getArity()).findFirst();
        if (existingRequirements.isPresent()) {
            MetadataManager.INSTANCE.deleteEntity(mdTxnCtx, existingRequirements.get());
        }

        // Finish this transaction and perform the remainder of the DROP FUNCTION statement.
        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        super.handleFunctionDropStatement(metadataProvider, stmt, requestParameters);
    }

    /**
     * Before dropping a view, we perform the following:
     * 1. Check if any of our existing graphs depend on the view to-be-dropped.
     * 2. Remove the GraphDependency record for the view to-be-dropped if it exists.
     */
    @Override
    public void handleViewDropStatement(MetadataProvider metadataProvider, Statement stmt) throws Exception {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);

        // Forbid dropping the dataset if any graphs depend on this view (dataset).
        ViewDropStatement vds = (ViewDropStatement) stmt;
        DataverseName workingDataverse = getActiveDataverseName(vds.getDataverseName());
        DependencyIdentifier dependencyIdentifier = new DependencyIdentifier(workingDataverse,
                vds.getViewName().getValue(), DependencyIdentifier.Kind.DATASET);
        GraphStatementHandlingUtil.throwIfDependentExists(mdTxnCtx, dependencyIdentifier);

        // Drop the GraphDependency record associated with this view, if it exists.
        Optional<IEntityRequirements> existingRequirements = GraphixMetadataExtension.getAllEntityRequirements(mdTxnCtx)
                .stream().filter(r -> r.getDataverseName().equals(workingDataverse))
                .filter(r -> r.getEntityName().equals(vds.getViewName().getValue()))
                .filter(r -> r.getDependentKind() == IEntityRequirements.DependentKind.FUNCTION).findFirst();
        if (existingRequirements.isPresent()) {
            MetadataManager.INSTANCE.deleteEntity(mdTxnCtx, existingRequirements.get());
        }

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
        DropDatasetStatement dds = (DropDatasetStatement) stmt;
        DataverseName workingDataverse = getActiveDataverseName(dds.getDataverseName());
        DependencyIdentifier dependencyIdentifier = new DependencyIdentifier(workingDataverse,
                dds.getDatasetName().toString(), DependencyIdentifier.Kind.DATASET);
        GraphStatementHandlingUtil.throwIfDependentExists(mdTxnCtx, dependencyIdentifier);

        // Finish this transaction and perform the remainder of the DROP DATASET statement.
        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        super.handleDatasetDropStatement(metadataProvider, stmt, hcc, requestParameters);
    }

    /**
     * Before dropping a dataverse, we perform the following:
     * 1. Check if any other entities outside the dataverse to-be-dropped depend on any entities inside the dataverse.
     * 2. Remove all GraphDependency records associated with the dataverse to-be-dropped.
     * 3. Remove all Graph records associated with the dataverse to-be-dropped.
     */
    @Override
    protected void handleDataverseDropStatement(MetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc, IRequestParameters requestParameters) throws Exception {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);

        // Forbid dropping this dataverse if other entities that are outside this dataverse depend on this dataverse.
        DataverseName droppedDataverse = ((DataverseDropStatement) stmt).getDataverseName();
        List<IEntityRequirements> requirementsList = GraphixMetadataExtension.getAllEntityRequirements(mdTxnCtx);
        for (IEntityRequirements requirements : requirementsList) {
            if (requirements.getDataverseName().equals(droppedDataverse)) {
                continue;
            }
            for (DependencyIdentifier dependency : requirements) {
                if (dependency.getDataverseName().equals(droppedDataverse)) {
                    throw new CompilationException(ErrorCode.CANNOT_DROP_DATAVERSE_DEPENDENT_EXISTS,
                            dependency.getDependencyKind(), dependency.getDisplayName(),
                            requirements.getDependentKind(), requirements.getDisplayName());
                }
            }
        }

        // Perform a drop for all GraphDependency records contained in this dataverse.
        for (IEntityRequirements requirements : requirementsList) {
            if (!requirements.getDataverseName().equals(droppedDataverse)) {
                continue;
            }
            MetadataManager.INSTANCE.deleteEntity(mdTxnCtx, requirements);
        }

        // Perform a drop for all graphs contained in this dataverse.
        MetadataProvider tempMdProvider = MetadataProvider.create(appCtx, metadataProvider.getDefaultDataverse());
        tempMdProvider.getConfig().putAll(metadataProvider.getConfig());
        for (Graph graph : GraphixMetadataExtension.getAllGraphs(mdTxnCtx, droppedDataverse)) {
            tempMdProvider.getLocks().reset();
            GraphDropStatement gds = new GraphDropStatement(droppedDataverse, graph.getGraphName(), false);
            gds.handle(hcc, this, requestParameters, tempMdProvider, 0);
        }

        // Finish this transaction and perform the remainder of the DROP DATAVERSE statement.
        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        super.handleDataverseDropStatement(metadataProvider, stmt, hcc, requestParameters);
    }
}
