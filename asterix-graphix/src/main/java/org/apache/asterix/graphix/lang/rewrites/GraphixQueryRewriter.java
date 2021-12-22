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

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.graphix.common.metadata.GraphElementIdentifier;
import org.apache.asterix.graphix.common.metadata.GraphIdentifier;
import org.apache.asterix.graphix.extension.GraphixMetadataExtension;
import org.apache.asterix.graphix.lang.expression.GraphElementExpr;
import org.apache.asterix.graphix.lang.parser.GraphElementBodyParser;
import org.apache.asterix.graphix.lang.parser.GraphixParserFactory;
import org.apache.asterix.graphix.lang.statement.GraphElementDecl;
import org.apache.asterix.graphix.metadata.entities.Graph;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.IParserFactory;
import org.apache.asterix.lang.common.base.IReturningStatement;
import org.apache.asterix.lang.common.expression.AbstractCallExpression;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.common.util.ExpressionUtils;
import org.apache.asterix.lang.sqlpp.rewrites.SqlppQueryRewriter;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.SqlppGatherFunctionCallsVisitor;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class GraphixQueryRewriter extends SqlppQueryRewriter {
    private final GraphixParserFactory parserFactory;
    private final SqlppQueryRewriter bodyRewriter;

    public GraphixQueryRewriter(IParserFactory parserFactory) {
        super(parserFactory);

        // We can safely downcast to our specific parser factory here.
        this.parserFactory = (GraphixParserFactory) parserFactory;
        this.bodyRewriter = new SqlppQueryRewriter(parserFactory);
    }

    public void rewrite(GraphixRewritingContext rewriteContext, IReturningStatement topStatement,
            boolean allowNonStoredUdfCalls, boolean inlineUdfsAndViews, Collection<VarIdentifier> externalVars)
            throws CompilationException {
        // Get the graph elements in the top statement.
        Map<GraphElementIdentifier, GraphElementDecl> graphElements =
                loadNormalizedGraphElements(rewriteContext, topStatement);

        // Perform the remainder of our rewrites in our parent.
        super.rewrite(rewriteContext.getLangRewritingContext(), topStatement, allowNonStoredUdfCalls,
                inlineUdfsAndViews, externalVars);
    }

    public Map<GraphElementIdentifier, GraphElementDecl> loadNormalizedGraphElements(
            GraphixRewritingContext rewriteContext, IReturningStatement topExpr) throws CompilationException {
        Map<GraphElementIdentifier, GraphElementDecl> graphElements = new HashMap<>();

        // Gather all function calls.
        Deque<AbstractCallExpression> workQueue = new ArrayDeque<>();
        SqlppGatherFunctionCallsVisitor callVisitor = new SqlppGatherFunctionCallsVisitor(workQueue);
        for (Expression expr : topExpr.getDirectlyEnclosedExpressions()) {
            expr.accept(callVisitor, null);
        }

        AbstractCallExpression fnCall;
        while ((fnCall = workQueue.poll()) != null) {
            // Load only the graph element declarations (we will load the rest of the elements in the parent).
            if (!fnCall.getKind().equals(Expression.Kind.CALL_EXPRESSION)
                    || !fnCall.getFunctionSignature().getName().equals(GraphElementExpr.GRAPH_ELEMENT_FUNCTION_NAME)) {
                continue;
            }
            GraphElementExpr graphElementExpr = (GraphElementExpr) fnCall;
            GraphElementIdentifier identifier = graphElementExpr.getIdentifier();
            if (!graphElements.containsKey(identifier)) {

                // First, check if we have already loaded this graph element.
                GraphElementDecl elementDecl = rewriteContext.getDeclaredGraphElements().get(identifier);

                // If we cannot find the graph element in our context, search our metadata.
                if (elementDecl == null) {
                    GraphIdentifier graphIdentifier = identifier.getGraphIdentifier();
                    Graph graph;
                    try {
                        graph = GraphixMetadataExtension.getGraph(
                                rewriteContext.getMetadataProvider().getMetadataTxnContext(),
                                graphIdentifier.getDataverseName(), graphIdentifier.getGraphName());

                    } catch (AlgebricksException e) {
                        throw new CompilationException(ErrorCode.COMPILATION_ERROR,
                                graphElementExpr.getSourceLocation(),
                                "Graph " + graphIdentifier.getGraphName() + " does not exist.");
                    }

                    // Parse our graph element.
                    if (graph == null) {
                        throw new CompilationException(ErrorCode.COMPILATION_ERROR,
                                graphElementExpr.getSourceLocation(),
                                "Graph " + graphIdentifier.getGraphName() + " does not exist.");
                    }
                    Graph.Element element = graph.getGraphSchema().getElement(identifier);
                    elementDecl = GraphElementBodyParser.parse(element, parserFactory,
                            rewriteContext.getLangRewritingContext().getWarningCollector());
                }

                // Get our normalized element bodies.
                List<Expression> normalizedBodies = elementDecl.getNormalizedBodies();
                if (normalizedBodies.size() != elementDecl.getBodies().size()) {
                    GraphIdentifier graphIdentifier = elementDecl.getGraphIdentifier();
                    for (Expression body : elementDecl.getBodies()) {
                        Expression normalizedBody =
                                rewriteGraphElementBody(rewriteContext, graphIdentifier.getDataverseName(), body,
                                        Collections.emptyList(), elementDecl.getSourceLocation());
                        normalizedBodies.add(normalizedBody);
                    }
                }

                // Store the element declaration in our map, and iterate over this body.
                graphElements.put(identifier, elementDecl);
                for (Expression e : elementDecl.getNormalizedBodies()) {
                    e.accept(callVisitor, null);
                }
            }
        }

        return graphElements;
    }

    private Expression rewriteGraphElementBody(GraphixRewritingContext rewriteContext, DataverseName elementDataverse,
            Expression bodyExpr, List<VarIdentifier> externalVars, SourceLocation sourceLoc)
            throws CompilationException {
        Dataverse defaultDataverse = rewriteContext.getMetadataProvider().getDefaultDataverse();
        Dataverse targetDataverse;

        // We might need to change our dataverse, if the element definition requires a different one.
        if (elementDataverse.equals(defaultDataverse.getDataverseName())) {
            targetDataverse = defaultDataverse;

        } else {
            try {
                targetDataverse = rewriteContext.getMetadataProvider().findDataverse(elementDataverse);

            } catch (AlgebricksException e) {
                throw new CompilationException(ErrorCode.UNKNOWN_DATAVERSE, e, sourceLoc, elementDataverse);
            }
        }
        rewriteContext.getMetadataProvider().setDefaultDataverse(targetDataverse);

        // Get the body of the rewritten query.
        try {
            Query wrappedQuery = ExpressionUtils.createWrappedQuery(bodyExpr, sourceLoc);
            bodyRewriter.rewrite(rewriteContext.getLangRewritingContext(), wrappedQuery, false, false, externalVars);
            return wrappedQuery.getBody();

        } catch (CompilationException e) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, bodyExpr.getSourceLocation(),
                    "Bad definition for a graph element: " + e.getMessage());

        } finally {
            // Switch back to the working dataverse.
            rewriteContext.getMetadataProvider().setDefaultDataverse(defaultDataverse);
        }
    }
}
