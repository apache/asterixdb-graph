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
package org.apache.asterix.graphix.lang.rewrite;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.config.CompilerProperties;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.graphix.common.metadata.GraphIdentifier;
import org.apache.asterix.graphix.lang.clause.FromGraphClause;
import org.apache.asterix.graphix.lang.clause.LowerListClause;
import org.apache.asterix.graphix.lang.clause.LowerSwitchClause;
import org.apache.asterix.graphix.lang.parser.GraphixParserFactory;
import org.apache.asterix.graphix.lang.rewrite.common.BranchLookupTable;
import org.apache.asterix.graphix.lang.rewrite.common.ElementLookupTable;
import org.apache.asterix.graphix.lang.rewrite.print.SqlppASTPrintQueryVisitor;
import org.apache.asterix.graphix.lang.rewrite.resolve.ExhaustiveSearchResolver;
import org.apache.asterix.graphix.lang.rewrite.resolve.SchemaKnowledgeTable;
import org.apache.asterix.graphix.lang.rewrite.visitor.ElementLookupTableVisitor;
import org.apache.asterix.graphix.lang.rewrite.visitor.FunctionResolutionVisitor;
import org.apache.asterix.graphix.lang.rewrite.visitor.GraphixFunctionCallVisitor;
import org.apache.asterix.graphix.lang.rewrite.visitor.GraphixLoweringVisitor;
import org.apache.asterix.graphix.lang.rewrite.visitor.PatternGraphGroupVisitor;
import org.apache.asterix.graphix.lang.rewrite.visitor.PopulateUnknownsVisitor;
import org.apache.asterix.graphix.lang.rewrite.visitor.PostRewriteCheckVisitor;
import org.apache.asterix.graphix.lang.rewrite.visitor.PreRewriteCheckVisitor;
import org.apache.asterix.graphix.lang.rewrite.visitor.QueryCanonicalizationVisitor;
import org.apache.asterix.graphix.lang.rewrite.visitor.SubqueryVertexJoinVisitor;
import org.apache.asterix.graphix.lang.rewrite.visitor.VariableScopingCheckVisitor;
import org.apache.asterix.graphix.lang.statement.GraphElementDeclaration;
import org.apache.asterix.graphix.lang.struct.PatternGroup;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.base.IParserFactory;
import org.apache.asterix.lang.common.base.IReturningStatement;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.common.util.ExpressionUtils;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.SqlppFunctionBodyRewriter;
import org.apache.asterix.lang.sqlpp.rewrites.SqlppQueryRewriter;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.util.LogRedactionUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Rewriter for Graphix queries, which will lower all graph AST nodes into SQL++ AST nodes. We perform the following:
 * <ol>
 *  <li>Perform an error-checking on the fresh AST (immediately after parsing).</li>
 *  <li>Populate the unknowns in our AST (e.g. vertex / edge variables, projections, GROUP-BY keys).</li>
 *  <li>Perform a variable-scoping pass to identify illegal variables (either duplicate or out-of-scope).</li>
 *  <li>Resolve all of our function calls (Graphix, SQL++, and user-defined).</li>
 *  <li>Perform resolution of unlabeled vertices / edges, as well as edge directions.</li>
 *  <li>For all Graphix subqueries whose vertices are correlated, rewrite this correlation to be explicit.</li>
 *  <li>Using the labels of the vertices / edges in our AST, fetch the relevant graph elements from our metadata.</li>
 *  <li>Perform a canonical Graphix lowering pass to remove ambiguities (e.g. undirected edges).</li>
 *  <li>Perform a lowering pass to transform Graphix AST nodes to SQL++ AST nodes.</li>
 *  <li>Perform another lowering pass to transform Graphix CALL-EXPR nodes to SQL++ AST nodes.</li>
 *  <li>Perform all SQL++ rewrites on our newly lowered AST.</li>
 * </ol>
 */
public class GraphixQueryRewriter extends SqlppQueryRewriter {
    private static final Logger LOGGER = LogManager.getLogger(GraphixQueryRewriter.class);

    private final GraphixParserFactory parserFactory;
    private final SqlppQueryRewriter bodyRewriter;

    public GraphixQueryRewriter(IParserFactory parserFactory) {
        super(parserFactory);
        this.parserFactory = (GraphixParserFactory) parserFactory;
        this.bodyRewriter = getFunctionAndViewBodyRewriter();
    }

    @Override
    public void rewrite(LangRewritingContext langRewritingContext, IReturningStatement topStatement,
            boolean allowNonStoredUDFCalls, boolean inlineUdfsAndViews, Collection<VarIdentifier> externalVars)
            throws CompilationException {
        LOGGER.debug("Starting Graphix AST rewrites.");

        // Perform an initial error-checking pass to validate our user query.
        LOGGER.trace("Performing pre-Graphix-rewrite check (user query validation).");
        GraphixRewritingContext graphixRewritingContext = (GraphixRewritingContext) langRewritingContext;
        topStatement.accept(new PreRewriteCheckVisitor(graphixRewritingContext), null);

        // Perform the Graphix rewrites.
        rewriteGraphixASTNodes(graphixRewritingContext, topStatement, allowNonStoredUDFCalls);

        // Sanity check: ensure that no graph AST nodes exist after this point.
        Map<String, Object> queryConfig = graphixRewritingContext.getMetadataProvider().getConfig();
        if (queryConfig.containsKey(CompilerProperties.COMPILER_INTERNAL_SANITYCHECK_KEY)) {
            String configValue = (String) queryConfig.get(CompilerProperties.COMPILER_INTERNAL_SANITYCHECK_KEY);
            if (!configValue.equalsIgnoreCase("false")) {
                LOGGER.trace("Performing post-Graphix-rewrite check (making sure no graph AST nodes exist).");
                topStatement.accept(new PostRewriteCheckVisitor(), null);
            }
        }

        // Perform the remainder of the SQL++ rewrites.
        LOGGER.debug("Ending Graphix AST rewrites. Now starting SQL++ AST rewrites.");
        rewriteSQLPPASTNodes(langRewritingContext, topStatement, allowNonStoredUDFCalls, inlineUdfsAndViews,
                externalVars);

        // Update the variable counter on our context.
        topStatement.setVarCounter(graphixRewritingContext.getVarCounter().get());
        LOGGER.debug("Ending SQL++ AST rewrites.");
    }

    public void loadNormalizedGraphElement(GraphixRewritingContext graphixRewritingContext,
            GraphElementDeclaration graphElementDeclaration) throws CompilationException {
        if (graphElementDeclaration.getNormalizedBody() == null) {
            Dataverse defaultDataverse = graphixRewritingContext.getMetadataProvider().getDefaultDataverse();
            Dataverse targetDataverse;

            // We might need to change our dataverse, if the element definition requires a different one.
            GraphIdentifier graphIdentifier = graphElementDeclaration.getGraphIdentifier();
            DataverseName graphDataverseName = graphIdentifier.getDataverseName();
            if (graphDataverseName == null || graphDataverseName.equals(defaultDataverse.getDataverseName())) {
                targetDataverse = defaultDataverse;

            } else {
                try {
                    targetDataverse = graphixRewritingContext.getMetadataProvider().findDataverse(graphDataverseName);

                } catch (AlgebricksException e) {
                    throw new CompilationException(ErrorCode.UNKNOWN_DATAVERSE, e,
                            graphElementDeclaration.getSourceLocation(), graphDataverseName);
                }
            }
            graphixRewritingContext.getMetadataProvider().setDefaultDataverse(targetDataverse);

            // Get the body of the rewritten query.
            Expression rawBody = graphElementDeclaration.getRawBody();
            try {
                SourceLocation sourceLocation = graphElementDeclaration.getSourceLocation();
                Query wrappedQuery = ExpressionUtils.createWrappedQuery(rawBody, sourceLocation);
                bodyRewriter.rewrite(graphixRewritingContext, wrappedQuery, false, false, List.of());
                graphElementDeclaration.setNormalizedBody(wrappedQuery.getBody());

            } catch (CompilationException e) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, rawBody.getSourceLocation(),
                        "Bad definition for a graph element: " + e.getMessage());

            } finally {
                // Switch back to the working dataverse.
                graphixRewritingContext.getMetadataProvider().setDefaultDataverse(defaultDataverse);
            }
        }
    }

    /**
     * Lower a Graphix AST into a SQLPP AST. The only nodes that should survive are the following:
     * 1. {@link org.apache.asterix.graphix.lang.clause.FromGraphClause}
     * 2. {@link LowerListClause}
     * 3. {@link LowerSwitchClause}
     */
    public void rewriteGraphixASTNodes(GraphixRewritingContext graphixRewritingContext,
            IReturningStatement topStatement, boolean allowNonStoredUDFCalls) throws CompilationException {
        // Generate names for unnamed graph elements, projections in our SELECT CLAUSE, and keys in our GROUP BY.
        LOGGER.trace("Populating unknowns (both graph and non-graph) in our AST.");
        rewriteExpr(topStatement, new PopulateUnknownsVisitor(graphixRewritingContext));

        // Verify that variables are properly within scope.
        LOGGER.trace("Verifying that variables are unique and are properly scoped.");
        rewriteExpr(topStatement, new VariableScopingCheckVisitor(graphixRewritingContext));

        // Resolve all of our (Graphix, SQL++, and user-defined) function calls.
        LOGGER.trace("Resolving Graphix, SQL++, and user-defined function calls.");
        rewriteExpr(topStatement, new FunctionResolutionVisitor(graphixRewritingContext, allowNonStoredUDFCalls));

        // Rewrite implicit correlated vertex JOINs as explicit JOINs.
        LOGGER.trace("Rewriting correlated implicit vertex JOINs into explicit JOINs.");
        rewriteExpr(topStatement, new SubqueryVertexJoinVisitor(graphixRewritingContext));

        // Resolve our vertex labels, edge labels, and edge directions.
        LOGGER.trace("Performing label and edge direction resolution.");
        Map<GraphIdentifier, SchemaKnowledgeTable> knowledgeTableMap = new HashMap<>();
        Map<GraphIdentifier, PatternGroup> resolutionPatternMap = new HashMap<>();
        topStatement.accept(new PatternGraphGroupVisitor(resolutionPatternMap, graphixRewritingContext) {
            @Override
            public Expression visit(FromGraphClause fromGraphClause, ILangExpression arg) throws CompilationException {
                GraphIdentifier graphIdentifier = fromGraphClause.getGraphIdentifier(metadataProvider);
                SchemaKnowledgeTable schemaTable = new SchemaKnowledgeTable(fromGraphClause, graphixRewritingContext);
                knowledgeTableMap.put(graphIdentifier, schemaTable);
                return super.visit(fromGraphClause, arg);
            }
        }, null);
        for (Map.Entry<GraphIdentifier, PatternGroup> mapEntry : resolutionPatternMap.entrySet()) {
            SchemaKnowledgeTable knowledgeTable = knowledgeTableMap.get(mapEntry.getKey());
            new ExhaustiveSearchResolver(knowledgeTable).resolve(mapEntry.getValue());
        }

        // Fetch all relevant graph element declarations, using the element labels.
        LOGGER.trace("Fetching relevant edge and vertex bodies from our graph schema.");
        ElementLookupTable elementLookupTable = new ElementLookupTable();
        ElementLookupTableVisitor elementLookupTableVisitor =
                new ElementLookupTableVisitor(graphixRewritingContext, elementLookupTable, parserFactory);
        rewriteExpr(topStatement, elementLookupTableVisitor);
        for (GraphElementDeclaration graphElementDeclaration : elementLookupTable) {
            loadNormalizedGraphElement(graphixRewritingContext, graphElementDeclaration);
        }

        // Expand / enumerate vertex and edge patterns to snuff out all ambiguities.
        LOGGER.trace("Performing a canonicalization pass to expand or enumerate patterns.");
        BranchLookupTable branchLookupTable = new BranchLookupTable();
        QueryCanonicalizationVisitor queryCanonicalizationVisitor =
                new QueryCanonicalizationVisitor(branchLookupTable, graphixRewritingContext);
        rewriteExpr(topStatement, queryCanonicalizationVisitor);

        // Transform all graph AST nodes (i.e. perform the representation lowering).
        LOGGER.trace("Lowering the Graphix AST-specific nodes representation to a SQL++ representation.");
        GraphixLoweringVisitor graphixLoweringVisitor =
                new GraphixLoweringVisitor(graphixRewritingContext, elementLookupTable, branchLookupTable);
        rewriteExpr(topStatement, graphixLoweringVisitor);

        // Lower all of our Graphix function calls (and perform schema-enrichment).
        LOGGER.trace("Lowering the Graphix CALL-EXPR nodes to a pure SQL++ representation.");
        rewriteExpr(topStatement, new GraphixFunctionCallVisitor(graphixRewritingContext));
    }

    /**
     * Rewrite a SQLPP AST. We do not perform the following:
     * <ul>
     *  <li>Function call resolution (this is handled in {@link FunctionResolutionVisitor}).</li>
     *  <li>Column name generation (this is handled in {@link PopulateUnknownsVisitor}).</li>
     *  <li>SQL-compat rewrites (not supported).</li>
     * </ul>
     */
    public void rewriteSQLPPASTNodes(LangRewritingContext langRewritingContext, IReturningStatement topStatement,
            boolean allowNonStoredUDFCalls, boolean inlineUdfsAndViews, Collection<VarIdentifier> externalVars)
            throws CompilationException {
        super.setup(langRewritingContext, topStatement, externalVars, allowNonStoredUDFCalls, inlineUdfsAndViews);
        super.substituteGroupbyKeyExpression();
        super.rewriteGroupBys();
        super.rewriteSetOperations();
        super.inlineColumnAlias();
        super.rewriteWindowExpressions();
        super.rewriteGroupingSets();
        super.variableCheckAndRewrite();
        super.extractAggregatesFromCaseExpressions();
        super.rewriteGroupByAggregationSugar();
        super.rewriteWindowAggregationSugar();
        super.rewriteOperatorExpression();
        super.rewriteCaseExpressions();
        super.rewriteListInputFunctions();
        super.rewriteRightJoins();
        super.loadAndInlineUdfsAndViews();
        super.rewriteSpecialFunctionNames();
        super.inlineWithExpressions();
    }

    @Override
    protected SqlppFunctionBodyRewriter getFunctionAndViewBodyRewriter() {
        return new SqlppFunctionBodyRewriter(parserFactory) {
            @Override
            public void rewrite(LangRewritingContext langRewritingContext, IReturningStatement topStatement,
                    boolean allowNonStoredUDFCalls, boolean inlineUdfsAndViews, Collection<VarIdentifier> externalVars)
                    throws CompilationException {
                // Perform an initial error-checking pass to validate our body.
                GraphixRewritingContext graphixRewritingContext = (GraphixRewritingContext) langRewritingContext;
                topStatement.accept(new PreRewriteCheckVisitor(graphixRewritingContext), null);

                // Perform the Graphix rewrites.
                rewriteGraphixASTNodes(graphixRewritingContext, topStatement, allowNonStoredUDFCalls);

                // Sanity check: ensure that no graph AST nodes exist after this point.
                Map<String, Object> queryConfig = graphixRewritingContext.getMetadataProvider().getConfig();
                if (queryConfig.containsKey(CompilerProperties.COMPILER_INTERNAL_SANITYCHECK_KEY)) {
                    String configValue = (String) queryConfig.get(CompilerProperties.COMPILER_INTERNAL_SANITYCHECK_KEY);
                    if (!configValue.equalsIgnoreCase("false")) {
                        topStatement.accept(new PostRewriteCheckVisitor(), null);
                    }
                }

                // Perform the remainder of the SQL++ (body specific) rewrites.
                super.setup(langRewritingContext, topStatement, externalVars, allowNonStoredUDFCalls,
                        inlineUdfsAndViews);
                super.substituteGroupbyKeyExpression();
                super.rewriteGroupBys();
                super.rewriteSetOperations();
                super.inlineColumnAlias();
                super.rewriteWindowExpressions();
                super.rewriteGroupingSets();
                super.variableCheckAndRewrite();
                super.extractAggregatesFromCaseExpressions();
                super.rewriteGroupByAggregationSugar();
                super.rewriteWindowAggregationSugar();
                super.rewriteOperatorExpression();
                super.rewriteCaseExpressions();
                super.rewriteListInputFunctions();
                super.rewriteRightJoins();

                // Update the variable counter in our context.
                topStatement.setVarCounter(langRewritingContext.getVarCounter().get());
            }
        };
    }

    private <R, T> void rewriteExpr(IReturningStatement returningStatement, ILangVisitor<R, T> visitor)
            throws CompilationException {
        if (LOGGER.isTraceEnabled()) {
            StringWriter stringWriter = new StringWriter();
            PrintWriter printWriter = new PrintWriter(stringWriter);
            returningStatement.accept(new SqlppASTPrintQueryVisitor(printWriter), null);
            String planAsString = LogRedactionUtil.userData(stringWriter.toString());
            LOGGER.trace("Plan before rewrite: {}\n", planAsString);
        }
        returningStatement.accept(visitor, null);
        if (LOGGER.isTraceEnabled()) {
            StringWriter stringWriter = new StringWriter();
            PrintWriter printWriter = new PrintWriter(stringWriter);
            returningStatement.accept(new SqlppASTPrintQueryVisitor(printWriter), null);
            String planAsString = LogRedactionUtil.userData(stringWriter.toString());
            LOGGER.trace("Plan after rewrite: {}\n", planAsString);
        }
    }
}
