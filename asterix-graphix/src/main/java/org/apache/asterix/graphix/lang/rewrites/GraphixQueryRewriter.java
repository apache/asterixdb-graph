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
import java.util.Map;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.graphix.algebra.compiler.provider.GraphixCompilationProvider;
import org.apache.asterix.graphix.common.metadata.GraphElementIdentifier;
import org.apache.asterix.graphix.lang.clause.FromGraphClause;
import org.apache.asterix.graphix.lang.parser.GraphixParserFactory;
import org.apache.asterix.graphix.lang.rewrites.common.ElementLookupTable;
import org.apache.asterix.graphix.lang.rewrites.print.SqlppASTPrintQueryVisitor;
import org.apache.asterix.graphix.lang.rewrites.visitor.ElementAnalysisVisitor;
import org.apache.asterix.graphix.lang.rewrites.visitor.ElementLookupTableVisitor;
import org.apache.asterix.graphix.lang.rewrites.visitor.ElementResolutionVisitor;
import org.apache.asterix.graphix.lang.rewrites.visitor.GenerateVariableVisitor;
import org.apache.asterix.graphix.lang.rewrites.visitor.GraphixFunctionCallVisitor;
import org.apache.asterix.graphix.lang.rewrites.visitor.GraphixLoweringVisitor;
import org.apache.asterix.graphix.lang.rewrites.visitor.PostRewriteCheckVisitor;
import org.apache.asterix.graphix.lang.rewrites.visitor.PreRewriteCheckVisitor;
import org.apache.asterix.graphix.lang.rewrites.visitor.ScopingCheckVisitor;
import org.apache.asterix.graphix.lang.statement.GraphElementDecl;
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
 * 4. Resolve our Graphix function calls
 * 4. For the remainder of our Graphix function calls, rewrite each call into a subtree w/o Graphix functions.
 * 5. Perform resolution of unlabeled vertices / edges, as well as edge directions.
 * 6. Using the labels of the vertices / edges in our AST, fetch the relevant graph elements from our metadata.
 * 7. Perform an analysis pass to find a) all vertices that are not attached to an edge (i.e. dangling vertices), b)
 * a list of edges, ordered by their dependencies on other edges, and c) a list of optional vertices (from LEFT-MATCH
 * clauses).
 * 8. Perform Graphix AST node lowering to SQL++ AST nodes.
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
            boolean allowNonStoredUdfCalls, boolean inlineUdfsAndViews, Collection<VarIdentifier> externalVars)
            throws CompilationException {
        LOGGER.debug("Starting Graphix AST rewrites.");

        // Perform an initial error-checking pass to validate our user query.
        LOGGER.trace("Performing pre-rewrite check (user query validation).");
        PreRewriteCheckVisitor preRewriteCheckVisitor = new PreRewriteCheckVisitor(langRewritingContext);
        topStatement.getBody().accept(preRewriteCheckVisitor, null);

        // Perform the Graphix rewrites.
        rewriteGraphixASTNodes(langRewritingContext, topStatement);

        // Sanity check: ensure that no graph AST nodes exist after this point.
        LOGGER.trace("Performing post-rewrite check (making sure no graph AST nodes exist).");
        PostRewriteCheckVisitor postRewriteCheckVisitor = new PostRewriteCheckVisitor();
        topStatement.getBody().accept(postRewriteCheckVisitor, null);

        // Perform the remainder of the SQL++ rewrites.
        LOGGER.debug("Ending Graphix AST rewrites. Now starting SQL++ AST rewrites.");
        super.rewrite(langRewritingContext, topStatement, allowNonStoredUdfCalls, inlineUdfsAndViews, externalVars);
        LOGGER.debug("Ending SQL++ AST rewrites.");

        // If desired, log the SQL++ query equivalent (i.e. turn the AST back into a query and log this).
        MetadataProvider metadataProvider = langRewritingContext.getMetadataProvider();
        String printRewriteMetadataKeyName = GraphixCompilationProvider.PRINT_REWRITE_METADATA_CONFIG;
        String printRewriteOption = (String) metadataProvider.getConfig().get(printRewriteMetadataKeyName);
        if ((printRewriteOption != null && printRewriteOption.equalsIgnoreCase("true")) || LOGGER.isTraceEnabled()) {
            StringWriter stringWriter = new StringWriter();
            PrintWriter printWriter = new PrintWriter(stringWriter);
            new SqlppASTPrintQueryVisitor(printWriter).visit((Query) topStatement, null);
            LOGGER.log(LOGGER.getLevel(), "Rewritten Graphix query: " + stringWriter);
        }
    }

    public void loadNormalizedGraphElement(GraphixRewritingContext graphixRewritingContext,
            GraphElementDecl graphElementDecl) throws CompilationException {
        List<Expression> normalizedBodies = graphElementDecl.getNormalizedBodies();
        if (normalizedBodies.size() != graphElementDecl.getBodies().size()) {
            for (Expression body : graphElementDecl.getBodies()) {
                Dataverse defaultDataverse = graphixRewritingContext.getMetadataProvider().getDefaultDataverse();
                Dataverse targetDataverse;

                // We might need to change our dataverse, if the element definition requires a different one.
                DataverseName elementName = graphElementDecl.getIdentifier().getGraphIdentifier().getDataverseName();
                if (elementName.equals(defaultDataverse.getDataverseName())) {
                    targetDataverse = defaultDataverse;

                } else {
                    try {
                        targetDataverse = graphixRewritingContext.getMetadataProvider().findDataverse(elementName);

                    } catch (AlgebricksException e) {
                        throw new CompilationException(ErrorCode.UNKNOWN_DATAVERSE, e,
                                graphElementDecl.getSourceLocation(), elementName);
                    }
                }
                graphixRewritingContext.getMetadataProvider().setDefaultDataverse(targetDataverse);

                // Get the body of the rewritten query.
                try {
                    Query wrappedQuery = ExpressionUtils.createWrappedQuery(body, graphElementDecl.getSourceLocation());
                    LangRewritingContext langRewritingContext = graphixRewritingContext.getLangRewritingContext();
                    bodyRewriter.rewrite(langRewritingContext, wrappedQuery, false, false, List.of());
                    normalizedBodies.add(wrappedQuery.getBody());

                } catch (CompilationException e) {
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, body.getSourceLocation(),
                            "Bad definition for a graph element: " + e.getMessage());

                } finally {
                    // Switch back to the working dataverse.
                    graphixRewritingContext.getMetadataProvider().setDefaultDataverse(defaultDataverse);
                }
            }
        }
    }

    public void rewriteGraphixASTNodes(LangRewritingContext langRewritingContext, IReturningStatement topStatement)
            throws CompilationException {
        // Generate names for unnamed graph elements, projections in our SELECT CLAUSE, and keys in our GROUP BY.
        LOGGER.trace("Generating names for unnamed elements (both graph and non-graph) in our AST.");
        GraphixRewritingContext graphixRewritingContext = new GraphixRewritingContext(langRewritingContext);
        GenerateVariableVisitor generateVariableVisitor = new GenerateVariableVisitor(graphixRewritingContext);
        topStatement.getBody().accept(generateVariableVisitor, null);

        // Verify that variables are properly within scope.
        LOGGER.trace("Verifying that variables are unique and are properly scoped.");
        ScopingCheckVisitor scopingCheckVisitor = new ScopingCheckVisitor(langRewritingContext);
        topStatement.getBody().accept(scopingCheckVisitor, null);

        // Handle all Graphix-specific function calls to SQL++ rewrites.
        LOGGER.trace("Rewriting all Graphix function calls to a SQL++ subtree or a function-less Graphix subtree.");
        GraphixFunctionCallVisitor graphixFunctionCallVisitor = new GraphixFunctionCallVisitor(graphixRewritingContext);
        topStatement.getBody().accept(graphixFunctionCallVisitor, null);

        // Attempt to resolve our vertex labels, edge labels, and edge directions.
        LOGGER.trace("Performing label and edge direction resolution.");
        ElementResolutionVisitor elementResolutionVisitor = new ElementResolutionVisitor(
                graphixRewritingContext.getMetadataProvider(), graphixRewritingContext.getWarningCollector());
        topStatement.getBody().accept(elementResolutionVisitor, null);

        // Fetch all relevant graph element declarations, using the element labels.
        LOGGER.trace("Fetching relevant edge and vertex bodies from our graph schema.");
        ElementLookupTable<GraphElementIdentifier> elementLookupTable = new ElementLookupTable<>();
        ElementLookupTableVisitor elementLookupTableVisitor =
                new ElementLookupTableVisitor(elementLookupTable, graphixRewritingContext.getMetadataProvider(),
                        parserFactory, graphixRewritingContext.getWarningCollector());
        topStatement.getBody().accept(elementLookupTableVisitor, null);
        for (GraphElementDecl graphElementDecl : elementLookupTable) {
            loadNormalizedGraphElement(graphixRewritingContext, graphElementDecl);
        }

        // Perform an analysis pass to get our edge dependencies, dangling vertices, and FROM-GRAPH-CLAUSE variables.
        LOGGER.trace("Collecting edge dependencies, dangling vertices, and FROM-GRAPH-CLAUSE specific variables.");
        ElementAnalysisVisitor elementAnalysisVisitor = new ElementAnalysisVisitor();
        topStatement.getBody().accept(elementAnalysisVisitor, null);
        Map<FromGraphClause, ElementAnalysisVisitor.FromGraphClauseContext> fromGraphClauseContextMap =
                elementAnalysisVisitor.getFromGraphClauseContextMap();

        // Transform all graph AST nodes (i.e. perform the representation lowering).
        LOGGER.trace("Lowering the Graphix AST representation to a pure SQL++ representation.");
        GraphixLoweringVisitor graphixLoweringVisitor =
                new GraphixLoweringVisitor(fromGraphClauseContextMap, elementLookupTable, graphixRewritingContext);
        topStatement.getBody().accept(graphixLoweringVisitor, null);
    }

    @Override
    protected SqlppFunctionBodyRewriter getFunctionAndViewBodyRewriter() {
        return new SqlppFunctionBodyRewriter(parserFactory) {
            @Override
            public void rewrite(LangRewritingContext langRewritingContext, IReturningStatement topStatement,
                    boolean allowNonStoredUdfCalls, boolean inlineUdfsAndViews, Collection<VarIdentifier> externalVars)
                    throws CompilationException {
                // Perform an initial error-checking pass to validate our body.
                LOGGER.trace("Performing pre-rewrite check (user query validation).");
                PreRewriteCheckVisitor preRewriteCheckVisitor = new PreRewriteCheckVisitor(langRewritingContext);
                topStatement.getBody().accept(preRewriteCheckVisitor, null);

                // Perform the Graphix rewrites.
                rewriteGraphixASTNodes(langRewritingContext, topStatement);

                // Sanity check: ensure that no graph AST nodes exist after this point.
                LOGGER.trace("Performing post-rewrite check (making sure no graph AST nodes exist).");
                PostRewriteCheckVisitor postRewriteCheckVisitor = new PostRewriteCheckVisitor();
                topStatement.getBody().accept(postRewriteCheckVisitor, null);

                // Perform the remainder of the SQL++ rewrites.
                LOGGER.debug("Ending Graphix AST rewrites. Now starting SQL++ AST rewrites.");
                super.rewrite(langRewritingContext, topStatement, allowNonStoredUdfCalls, inlineUdfsAndViews,
                        externalVars);
                LOGGER.debug("Ending SQL++ AST rewrites.");
            }
        };
    }
}
