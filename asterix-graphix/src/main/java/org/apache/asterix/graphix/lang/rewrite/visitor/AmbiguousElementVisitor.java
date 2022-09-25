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
package org.apache.asterix.graphix.lang.rewrite.visitor;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.algebra.compiler.option.ElementEvaluationOption;
import org.apache.asterix.graphix.algebra.compiler.option.IGraphixCompilerOption;
import org.apache.asterix.graphix.lang.annotation.ElementEvaluationAnnotation;
import org.apache.asterix.graphix.lang.annotation.SubqueryVertexJoinAnnotation;
import org.apache.asterix.graphix.lang.clause.FromGraphClause;
import org.apache.asterix.graphix.lang.expression.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.PathPatternExpr;
import org.apache.asterix.graphix.lang.expression.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrite.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.rewrite.canonical.CanonicalElementGeneratorFactory;
import org.apache.asterix.graphix.lang.rewrite.resolve.SchemaKnowledgeTable;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.graphix.lang.visitor.base.AbstractGraphixQueryVisitor;
import org.apache.asterix.lang.common.base.AbstractExpression;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;

/**
 * @see QueryCanonicalizationVisitor
 */
public class AmbiguousElementVisitor extends AbstractGraphixQueryVisitor {
    private final Map<AbstractExpression, Context> elementContextMap;
    private final Deque<CanonicalElementGeneratorFactory> factoryStack;
    private final Deque<SelectBlock> selectBlockStack;

    private final GraphixRewritingContext graphixRewritingContext;
    private final ElementEvaluationOption defaultElementEvaluation;

    private static class Context {
        final CanonicalElementGeneratorFactory generatorFactory;
        final ElementEvaluationOption elementEvaluationOption;
        final SelectBlock sourceSelectBlock;

        private Context(CanonicalElementGeneratorFactory generatorFactory,
                ElementEvaluationOption elementEvaluationOption, SelectBlock sourceSelectBlock) {
            this.generatorFactory = generatorFactory;
            this.elementEvaluationOption = elementEvaluationOption;
            this.sourceSelectBlock = sourceSelectBlock;
        }
    }

    public AmbiguousElementVisitor(GraphixRewritingContext graphixRewritingContext) throws CompilationException {
        IGraphixCompilerOption setting = graphixRewritingContext.getSetting(ElementEvaluationOption.OPTION_KEY_NAME);
        this.defaultElementEvaluation = (ElementEvaluationOption) setting;
        this.graphixRewritingContext = graphixRewritingContext;
        this.elementContextMap = new HashMap<>();
        this.selectBlockStack = new ArrayDeque<>();
        this.factoryStack = new ArrayDeque<>();
    }

    @Override
    public Expression visit(SelectBlock selectBlock, ILangExpression arg) throws CompilationException {
        selectBlockStack.push(selectBlock);
        super.visit(selectBlock, arg);
        selectBlockStack.pop();
        return null;
    }

    @Override
    public Expression visit(FromGraphClause fromGraphClause, ILangExpression arg) throws CompilationException {
        SchemaKnowledgeTable knowledgeTable = new SchemaKnowledgeTable(fromGraphClause, graphixRewritingContext);
        factoryStack.push(new CanonicalElementGeneratorFactory(graphixRewritingContext, knowledgeTable));
        super.visit(fromGraphClause, arg);
        factoryStack.pop();
        return null;
    }

    @Override
    public Expression visit(PathPatternExpr pathPatternExpr, ILangExpression arg) throws CompilationException {
        Set<VariableExpr> visitedVertices = new HashSet<>();
        for (EdgePatternExpr edgeExpression : pathPatternExpr.getEdgeExpressions()) {
            edgeExpression.accept(this, arg);
            visitedVertices.add(edgeExpression.getLeftVertex().getVariableExpr());
            visitedVertices.add(edgeExpression.getRightVertex().getVariableExpr());
        }
        for (VertexPatternExpr vertexExpression : pathPatternExpr.getVertexExpressions()) {
            if (!visitedVertices.contains(vertexExpression.getVariableExpr())) {
                vertexExpression.accept(this, arg);
            }
        }
        return pathPatternExpr;
    }

    @Override
    public Expression visit(EdgePatternExpr edgePatternExpr, ILangExpression arg) throws CompilationException {
        VertexPatternExpr leftVertex = edgePatternExpr.getLeftVertex();
        VertexPatternExpr rightVertex = edgePatternExpr.getRightVertex();
        EdgeDescriptor edgeDescriptor = edgePatternExpr.getEdgeDescriptor();
        if ((edgeDescriptor.getEdgeLabels().size() + leftVertex.getLabels().size() + rightVertex.getLabels().size()) > 3
                || edgeDescriptor.getPatternType() == EdgeDescriptor.PatternType.PATH
                || edgeDescriptor.getEdgeDirection() == EdgeDescriptor.EdgeDirection.UNDIRECTED) {
            elementContextMap.put(edgePatternExpr, new Context(factoryStack.peek(),
                    getEvaluationFromAnnotation(edgePatternExpr), selectBlockStack.peek()));
        }
        return super.visit(edgePatternExpr, arg);
    }

    @Override
    public Expression visit(VertexPatternExpr vertexPatternExpr, ILangExpression arg) throws CompilationException {
        if (vertexPatternExpr.getLabels().size() > 1
                && vertexPatternExpr.findHint(SubqueryVertexJoinAnnotation.class) == null) {
            elementContextMap.put(vertexPatternExpr, new Context(factoryStack.peek(),
                    ElementEvaluationOption.EXPAND_AND_UNION, selectBlockStack.peek()));
        }
        return super.visit(vertexPatternExpr, arg);
    }

    private ElementEvaluationOption getEvaluationFromAnnotation(AbstractExpression expression) {
        ElementEvaluationAnnotation hint = expression.findHint(ElementEvaluationAnnotation.class);
        if (hint == null) {
            return defaultElementEvaluation;

        } else if (hint.getKind() == ElementEvaluationAnnotation.Kind.EXPAND_AND_UNION) {
            return ElementEvaluationOption.EXPAND_AND_UNION;

        } else { // hint.getKind() == ElementEvaluationAnnotation.Kind.SWITCH_AND_CYCLE
            return ElementEvaluationOption.SWITCH_AND_CYCLE;
        }
    }

    public Set<AbstractExpression> getAmbiguousElements() {
        return elementContextMap.keySet();
    }

    public CanonicalElementGeneratorFactory getGeneratorFactory(AbstractExpression ambiguousElement) {
        return elementContextMap.get(ambiguousElement).generatorFactory;
    }

    public SelectBlock getSourceSelectBlock(AbstractExpression ambiguousElement) {
        return elementContextMap.get(ambiguousElement).sourceSelectBlock;
    }

    public ElementEvaluationOption getElementEvaluationOption(AbstractExpression ambiguousElement) {
        return elementContextMap.get(ambiguousElement).elementEvaluationOption;
    }
}
