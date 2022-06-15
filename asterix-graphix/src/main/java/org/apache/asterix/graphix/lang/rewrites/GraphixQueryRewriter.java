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
package org.apache.asterix.graphix.lang.rewrites;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collection;
import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.graphix.algebra.compiler.provider.GraphixCompilationProvider;
import org.apache.asterix.graphix.lang.parser.GraphixParserFactory;
import org.apache.asterix.graphix.lang.rewrites.common.ElementLookupTable;
import org.apache.asterix.graphix.lang.rewrites.print.SqlppASTPrintQueryVisitor;
import org.apache.asterix.graphix.lang.rewrites.visitor.CanonicalExpansionVisitor;
import org.apache.asterix.graphix.lang.rewrites.visitor.ElementLookupTableVisitor;
import org.apache.asterix.graphix.lang.rewrites.visitor.FunctionResolutionVisitor;
import org.apache.asterix.graphix.lang.rewrites.visitor.GraphixFunctionCallVisitor;
import org.apache.asterix.graphix.lang.rewrites.visitor.GraphixLoweringVisitor;
import org.apache.asterix.graphix.lang.rewrites.visitor.GroupByAggSugarVisitor;
import org.apache.asterix.graphix.lang.rewrites.visitor.PopulateUnknownsVisitor;
import org.apache.asterix.graphix.lang.rewrites.visitor.PostRewriteCheckVisitor;
import org.apache.asterix.graphix.lang.rewrites.visitor.PostRewriteVariableVisitor;
import org.apache.asterix.graphix.lang.rewrites.visitor.PreRewriteCheckVisitor;
import org.apache.asterix.graphix.lang.rewrites.visitor.ScopingCheckVisitor;
import org.apache.asterix.graphix.lang.rewrites.visitor.StructureAnalysisVisitor;
import org.apache.asterix.graphix.lang.rewrites.visitor.StructureResolutionVisitor;
import org.apache.asterix.graphix.lang.statement.GraphElementDeclaration;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.IParserFactory;
import org.apache.asterix.lang.common.base.IReturningStatement;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.common.util.ExpressionUtils;
import org.apache.asterix.lang.sqlpp.rewrites.SqlppFunctionBodyRewriter;
import org.apache.asterix.lang.sqlpp.rewrites.SqlppQueryRewriter;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Rewriter for Graphix queries, which will lower all graph AST nodes into SQL++ AST nodes. We perform the following:
 * 1. Perform an error-checking on the fresh AST (immediately after parsing).
 * 2. Populate the unknowns in our AST (e.g. vertex / edge variables, projections, GROUP-BY keys).
 * 3. Perform a variable-scoping pass to identify illegal variables (either duplicate or out-of-scope).
 * 4. Resolve all of our function calls (Graphix, SQL++, and user-defined).
 * 5. Perform resolution of unlabeled vertices / edges, as well as edge directions.
 * 6. Using the labels of the vertices / edges in our AST, fetch the relevant graph elements from our metadata.
 * 7. Perform a canonical Graphix lowering pass to remove ambiguities (e.g. undirected edges).
 * 8. Perform a lowering pass to transform Graphix AST nodes to SQL++ AST nodes.
 * 9. Perform another lowering pass to transform Graphix CALL-EXPR nodes to SQL++ AST nodes.
 * 10. Perform all SQL++ rewrites on our newly lowered AST.
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
        PreRewriteCheckVisitor preRewriteCheckVisitor = new PreRewriteCheckVisitor(graphixRewritingContext);
        topStatement.getBody().accept(preRewriteCheckVisitor, null);

        // Perform the Graphix rewrites.
        rewriteGraphixASTNodes(graphixRewritingContext, topStatement, allowNonStoredUDFCalls);

        // Sanity check: ensure that no graph AST nodes exist after this point.
        LOGGER.trace("Performing post-Graphix-rewrite check (making sure no graph AST nodes exist).");
        PostRewriteCheckVisitor postRewriteCheckVisitor = new PostRewriteCheckVisitor();
        topStatement.getBody().accept(postRewriteCheckVisitor, null);

        // Perform the remainder of the SQL++ rewrites.
        LOGGER.debug("Ending Graphix AST rewrites. Now starting SQL++ AST rewrites.");
        rewriteSQLPPASTNodes(langRewritingContext, topStatement, allowNonStoredUDFCalls, inlineUdfsAndViews,
                externalVars);

        // If desired, log the SQL++ query equivalent (i.e. turn the AST back into a query and log this).
        MetadataProvider metadataProvider = graphixRewritingContext.getMetadataProvider();
        String printRewriteMetadataKeyName = GraphixCompilationProvider.PRINT_REWRITE_METADATA_CONFIG;
        String printRewriteOption = (String) metadataProvider.getConfig().get(printRewriteMetadataKeyName);
        if ((printRewriteOption != null && printRewriteOption.equalsIgnoreCase("true")) || LOGGER.isTraceEnabled()) {
            StringWriter stringWriter = new StringWriter();
            PrintWriter printWriter = new PrintWriter(stringWriter);
            new SqlppASTPrintQueryVisitor(printWriter).visit((Query) topStatement, null);
            LOGGER.log(LOGGER.getLevel(), "Rewritten Graphix query: " + stringWriter);
        }

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
            DataverseName elementName = graphElementDeclaration.getIdentifier().getGraphIdentifier().getDataverseName();
            if (elementName.equals(defaultDataverse.getDataverseName())) {
                targetDataverse = defaultDataverse;

            } else {
                try {
                    targetDataverse = graphixRewritingContext.getMetadataProvider().findDataverse(elementName);

                } catch (AlgebricksException e) {
                    throw new CompilationException(ErrorCode.UNKNOWN_DATAVERSE, e,
                            graphElementDeclaration.getSourceLocation(), elementName);
                }
            }
            graphixRewritingContext.getMetadataProvider().setDefaultDataverse(targetDataverse);

            // Get the body of the rewritten query.
            Expression rawBody = graphElementDeclaration.getRawBody();
            try {
                Query wrappedQuery =
                        ExpressionUtils.createWrappedQuery(rawBody, graphElementDeclaration.getSourceLocation());
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

    public void rewriteGraphixASTNodes(GraphixRewritingContext graphixRewritingContext,
            IReturningStatement topStatement, boolean allowNonStoredUDFCalls) throws CompilationException {
        // Generate names for unnamed graph elements, projections in our SELECT CLAUSE, and keys in our GROUP BY.
        LOGGER.trace("Populating unknowns (both graph and non-graph) in our AST.");
        PopulateUnknownsVisitor populateUnknownsVisitor = new PopulateUnknownsVisitor(graphixRewritingContext);
        topStatement.getBody().accept(populateUnknownsVisitor, null);

        // Verify that variables are properly within scope.
        LOGGER.trace("Verifying that variables are unique and are properly scoped.");
        ScopingCheckVisitor scopingCheckVisitor = new ScopingCheckVisitor(graphixRewritingContext);
        topStatement.getBody().accept(scopingCheckVisitor, null);

        // Resolve all of our (Graphix, SQL++, and user-defined) function calls.
        LOGGER.trace("Resolving Graphix, SQL++, and user-defined function calls.");
        FunctionResolutionVisitor functionResolutionVisitor =
                new FunctionResolutionVisitor(graphixRewritingContext, allowNonStoredUDFCalls);
        topStatement.getBody().accept(functionResolutionVisitor, null);

        // Attempt to resolve our vertex labels, edge labels, and edge directions.
        LOGGER.trace("Performing label and edge direction resolution.");
        StructureResolutionVisitor structureResolutionVisitor = new StructureResolutionVisitor(graphixRewritingContext);
        topStatement.getBody().accept(structureResolutionVisitor, null);

        // Fetch all relevant graph element declarations, using the element labels.
        LOGGER.trace("Fetching relevant edge and vertex bodies from our graph schema.");
        ElementLookupTable elementLookupTable = new ElementLookupTable();
        ElementLookupTableVisitor elementLookupTableVisitor =
                new ElementLookupTableVisitor(graphixRewritingContext, elementLookupTable, parserFactory);
        topStatement.getBody().accept(elementLookupTableVisitor, null);
        for (GraphElementDeclaration graphElementDeclaration : elementLookupTable) {
            loadNormalizedGraphElement(graphixRewritingContext, graphElementDeclaration);
        }

        // Expand vertex and edge patterns to remove all ambiguities (into a canonical form).
        LOGGER.trace("Expanding vertex and edge patterns to a canonical form.");
        CanonicalExpansionVisitor canonicalExpansionVisitor = new CanonicalExpansionVisitor(graphixRewritingContext);
        topStatement.setBody(topStatement.getBody().accept(canonicalExpansionVisitor, null));

        // Perform an analysis pass to get our edge dependencies, dangling vertices, and FROM-GRAPH-CLAUSE variables.
        LOGGER.trace("Collecting edge dependencies, dangling vertices, and FROM-GRAPH-CLAUSE specific variables.");
        StructureAnalysisVisitor structureAnalysisVisitor = new StructureAnalysisVisitor(graphixRewritingContext);
        topStatement.getBody().accept(structureAnalysisVisitor, null);

        // Transform all graph AST nodes (i.e. perform the representation lowering).
        LOGGER.trace("Lowering the Graphix AST-specific nodes representation to a pure SQL++ representation.");
        GraphixLoweringVisitor graphixLoweringVisitor = new GraphixLoweringVisitor(graphixRewritingContext,
                elementLookupTable, structureAnalysisVisitor.getFromGraphClauseContextMap());
        topStatement.setBody(topStatement.getBody().accept(graphixLoweringVisitor, null));

        // Lower all of our Graphix function calls (and perform schema-enrichment).
        LOGGER.trace("Lowering the Graphix CALL-EXPR nodes to a pure SQL++ representation.");
        GraphixFunctionCallVisitor graphixFunctionCallVisitor = new GraphixFunctionCallVisitor(graphixRewritingContext);
        topStatement.getBody().accept(graphixFunctionCallVisitor, null);
    }

    public void rewriteSQLPPASTNodes(LangRewritingContext langRewritingContext, IReturningStatement topStatement,
            boolean allowNonStoredUDFCalls, boolean inlineUdfsAndViews, Collection<VarIdentifier> externalVars)
            throws CompilationException {
        super.setup(langRewritingContext, topStatement, externalVars, allowNonStoredUDFCalls, inlineUdfsAndViews);
        super.substituteGroupbyKeyExpression();
        super.rewriteGroupBys();
        super.inlineColumnAlias();
        super.rewriteWindowExpressions();
        super.rewriteGroupingSets();

        // We need to override the default behavior of our variable check + rewrite visitor...
        PostRewriteVariableVisitor postRewriteVariableVisitor = new PostRewriteVariableVisitor(langRewritingContext,
                langRewritingContext.getMetadataProvider(), externalVars);
        topStatement.accept(postRewriteVariableVisitor, null);
        super.extractAggregatesFromCaseExpressions();

        // ...and our GROUP-BY rewrite visitor.
        GroupByAggSugarVisitor groupByAggSugarVisitor = new GroupByAggSugarVisitor(langRewritingContext, externalVars);
        topStatement.accept(groupByAggSugarVisitor, null);

        // The remainder of our rewrites are the same.
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
                LOGGER.trace("Performing pre-Graphix-rewrite check (user query validation).");
                GraphixRewritingContext graphixRewritingContext = (GraphixRewritingContext) langRewritingContext;
                PreRewriteCheckVisitor preRewriteCheckVisitor = new PreRewriteCheckVisitor(graphixRewritingContext);
                topStatement.getBody().accept(preRewriteCheckVisitor, null);

                // Perform the Graphix rewrites.
                rewriteGraphixASTNodes(graphixRewritingContext, topStatement, allowNonStoredUDFCalls);

                // Sanity check: ensure that no graph AST nodes exist after this point.
                LOGGER.trace("Performing post-Graphix-rewrite check (making sure no graph AST nodes exist).");
                PostRewriteCheckVisitor postRewriteCheckVisitor = new PostRewriteCheckVisitor();
                topStatement.getBody().accept(postRewriteCheckVisitor, null);

                // Perform the remainder of the SQL++ rewrites.
                LOGGER.debug("Ending Graphix AST rewrites. Now starting SQL++ AST rewrites.");
                rewriteSQLPPASTNodes(langRewritingContext, topStatement, allowNonStoredUDFCalls, inlineUdfsAndViews,
                        externalVars);
                LOGGER.debug("Ending SQL++ AST rewrites.");

                // Update the variable counter in our context.
                topStatement.setVarCounter(langRewritingContext.getVarCounter().get());
            }
        };
    }
}
