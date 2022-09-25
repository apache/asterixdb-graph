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
package org.apache.asterix.graphix.lang.rewrite.canonical;

import static org.apache.asterix.graphix.lang.rewrite.lower.action.PathPatternAction.buildPathRecord;
import static org.apache.asterix.graphix.lang.rewrite.util.CanonicalElementUtil.replaceEdgeInIterator;
import static org.apache.asterix.graphix.lang.rewrite.util.CanonicalElementUtil.replaceVertexInIterator;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.annotation.SubqueryVertexJoinAnnotation;
import org.apache.asterix.graphix.lang.expression.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.PathPatternExpr;
import org.apache.asterix.graphix.lang.expression.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrite.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.rewrite.visitor.GraphixDeepCopyVisitor;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.graphix.lang.visitor.base.AbstractGraphixQueryVisitor;
import org.apache.asterix.lang.common.base.AbstractExpression;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.SelectSetOperation;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.optype.SetOpType;
import org.apache.asterix.lang.sqlpp.struct.SetOperationInput;
import org.apache.asterix.lang.sqlpp.struct.SetOperationRight;

public class CanonicalElementExpansionConsumer implements ICanonicalElementConsumer {
    private final GraphixDeepCopyVisitor deepCopyVisitor = new GraphixDeepCopyVisitor();
    private final Deque<SelectBlock> blackSelectBlockStack = new ArrayDeque<>();
    private final Deque<SelectBlock> redSelectBlockStack = new ArrayDeque<>();
    private final GraphixRewritingContext graphixRewritingContext;

    // We should return a copy of our original SELECT-EXPR.
    private final Set<SetOperationInput> remainingSetOpInputs;

    public CanonicalElementExpansionConsumer(SelectExpression originalSelectExpression,
            GraphixRewritingContext graphixRewritingContext) {
        this.graphixRewritingContext = graphixRewritingContext;
        this.remainingSetOpInputs = new HashSet<>();
        this.remainingSetOpInputs.add(originalSelectExpression.getSelectSetOperation().getLeftInput());
        this.remainingSetOpInputs.addAll(originalSelectExpression.getSelectSetOperation().getRightInputs().stream()
                .map(SetOperationRight::getSetOperationRightInput).collect(Collectors.toSet()));
    }

    @Override
    public void accept(AbstractExpression ambiguousElement, List<? extends AbstractExpression> canonicalElements)
            throws CompilationException {
        Deque<SelectBlock> readStack, writeStack;
        if (blackSelectBlockStack.isEmpty()) {
            writeStack = blackSelectBlockStack;
            readStack = redSelectBlockStack;

        } else {
            writeStack = redSelectBlockStack;
            readStack = blackSelectBlockStack;
        }

        ICanonicalPatternUpdater pathPatternReplacer;
        if (ambiguousElement instanceof VertexPatternExpr) {
            pathPatternReplacer = new VertexPatternUpdater(ambiguousElement);

        } else { // ambiguousElement instanceof EdgePatternExpr
            EdgePatternExpr ambiguousEdgePattern = (EdgePatternExpr) ambiguousElement;
            EdgeDescriptor ambiguousEdgeDescriptor = ambiguousEdgePattern.getEdgeDescriptor();
            if (ambiguousEdgeDescriptor.getPatternType() == EdgeDescriptor.PatternType.EDGE) {
                pathPatternReplacer = new EdgePatternUpdater(ambiguousElement);

            } else { // ambiguousEdgeDescriptor().getPatternType() == EdgeDescriptor.PatternType.PATH
                pathPatternReplacer = new PathPatternUpdater(ambiguousElement);
            }
        }

        // Consume all of our read stack.
        while (!readStack.isEmpty()) {
            SelectBlock workingSelectBlock = readStack.pop();
            for (AbstractExpression canonicalElement : canonicalElements) {
                SelectBlock workingSelectBlockCopy = deepCopyVisitor.visit(workingSelectBlock, null);
                workingSelectBlockCopy.accept(new AbstractGraphixQueryVisitor() {
                    @Override
                    public Expression visit(PathPatternExpr pathPatternExpr, ILangExpression arg)
                            throws CompilationException {
                        pathPatternReplacer.accept(canonicalElement, pathPatternExpr);
                        return pathPatternExpr;
                    }
                }, null);
                writeStack.push(workingSelectBlockCopy);
            }
        }
    }

    public void finalize(SelectExpression selectExpression, Consumer<SelectBlock> selectBlockCallback) {
        SelectSetOperation selectSetOperation = selectExpression.getSelectSetOperation();
        SetOperationInput leftSetOperationInput = selectSetOperation.getLeftInput();

        // Exhaust all of our generated SELECT-BLOCKs.
        Deque<SelectBlock> finalStack = (redSelectBlockStack.isEmpty()) ? blackSelectBlockStack : redSelectBlockStack;
        if (!remainingSetOpInputs.contains(leftSetOperationInput)) {
            leftSetOperationInput.setSelectBlock(finalStack.peek());
            selectBlockCallback.accept(finalStack.pop());
        }
        for (SetOperationRight rightInput : selectSetOperation.getRightInputs()) {
            SetOperationInput rightSetOperationInput = rightInput.getSetOperationRightInput();
            if (!remainingSetOpInputs.contains(rightSetOperationInput)) {
                rightSetOperationInput.setSelectBlock(finalStack.peek());
                selectBlockCallback.accept(finalStack.pop());
            }
        }
        while (!finalStack.isEmpty()) {
            SetOperationInput newSetOpInput = new SetOperationInput(finalStack.peek(), null);
            selectSetOperation.getRightInputs().add(new SetOperationRight(SetOpType.UNION, false, newSetOpInput));
            selectBlockCallback.accept(finalStack.pop());
        }
    }

    public void resetSelectBlock(SelectBlock selectBlock) {
        blackSelectBlockStack.clear();
        redSelectBlockStack.clear();
        blackSelectBlockStack.push(selectBlock);

        // Remove from our original SET-OP input set, the SET-OP this SELECT-BLOCK corresponds to.
        remainingSetOpInputs.removeIf(s -> s.selectBlock() && s.getSelectBlock().equals(selectBlock));
    }

    // We provide the following interface to handle dangling vertices, edges, and paths.
    @FunctionalInterface
    private interface ICanonicalPatternUpdater {
        void accept(AbstractExpression canonicalExpr, PathPatternExpr pathPatternExpr) throws CompilationException;
    }

    private class VertexPatternUpdater implements ICanonicalPatternUpdater {
        private final AbstractExpression ambiguousElement;

        private VertexPatternUpdater(AbstractExpression ambiguousElement) {
            this.ambiguousElement = ambiguousElement;
        }

        @Override
        public void accept(AbstractExpression canonicalExpr, PathPatternExpr pathPatternExpr)
                throws CompilationException {
            List<VertexPatternExpr> vertexExpressions = pathPatternExpr.getVertexExpressions();
            ListIterator<VertexPatternExpr> vertexIterator = vertexExpressions.listIterator();
            VertexPatternExpr ambiguousVertex = (VertexPatternExpr) ambiguousElement;
            VertexPatternExpr canonicalVertex = (VertexPatternExpr) canonicalExpr;

            // Our replacement map must also include any subquery-correlated-join vertices.
            Map<VertexPatternExpr, VertexPatternExpr> replacementMap = new HashMap<>();
            replacementMap.put(ambiguousVertex, canonicalVertex);
            for (VertexPatternExpr vertexPatternExpr : vertexExpressions) {
                if (vertexPatternExpr.findHint(SubqueryVertexJoinAnnotation.class) != null) {
                    SubqueryVertexJoinAnnotation hint = vertexPatternExpr.findHint(SubqueryVertexJoinAnnotation.class);
                    if (hint.getSourceVertexVariable().equals(ambiguousVertex.getVariableExpr())) {
                        VertexPatternExpr canonicalVertexCopy = deepCopyVisitor.visit(canonicalVertex, null);
                        canonicalVertexCopy.setVariableExpr(vertexPatternExpr.getVariableExpr());
                        replacementMap.put(vertexPatternExpr, canonicalVertexCopy);
                    }
                }
            }
            replaceVertexInIterator(replacementMap, vertexIterator, deepCopyVisitor);
        }
    }

    private class EdgePatternUpdater implements ICanonicalPatternUpdater {
        private final AbstractExpression ambiguousElement;

        private EdgePatternUpdater(AbstractExpression ambiguousElement) {
            this.ambiguousElement = ambiguousElement;
        }

        @Override
        public void accept(AbstractExpression canonicalExpr, PathPatternExpr pathPatternExpr)
                throws CompilationException {
            EdgePatternExpr ambiguousEdgePattern = (EdgePatternExpr) ambiguousElement;
            VertexPatternExpr ambiguousLeftVertex = ambiguousEdgePattern.getLeftVertex();
            VertexPatternExpr ambiguousRightVertex = ambiguousEdgePattern.getRightVertex();
            EdgePatternExpr canonicalEdge = (EdgePatternExpr) canonicalExpr;
            VertexPatternExpr canonicalLeftVertex = canonicalEdge.getLeftVertex();
            VertexPatternExpr canonicalRightVertex = canonicalEdge.getRightVertex();

            // Iterate through our edge list.
            List<EdgePatternExpr> edgeExpressions = pathPatternExpr.getEdgeExpressions();
            ListIterator<EdgePatternExpr> edgeIterator = edgeExpressions.listIterator();
            replaceEdgeInIterator(Map.of(ambiguousEdgePattern, canonicalEdge), edgeIterator, deepCopyVisitor);

            // Iterate through our vertex list.
            replaceEdgeVerticesAndJoinCopiesInIterator(pathPatternExpr, ambiguousLeftVertex, ambiguousRightVertex,
                    canonicalLeftVertex, canonicalRightVertex);
        }
    }

    private class PathPatternUpdater implements ICanonicalPatternUpdater {
        private final AbstractExpression ambiguousElement;

        private PathPatternUpdater(AbstractExpression ambiguousElement) {
            this.ambiguousElement = ambiguousElement;
        }

        @Override
        public void accept(AbstractExpression canonicalExpr, PathPatternExpr pathPatternExpr)
                throws CompilationException {
            EdgePatternExpr ambiguousEdgePattern = (EdgePatternExpr) ambiguousElement;
            VertexPatternExpr ambiguousLeftVertex = ambiguousEdgePattern.getLeftVertex();
            VertexPatternExpr ambiguousRightVertex = ambiguousEdgePattern.getRightVertex();
            EdgeDescriptor ambiguousEdgeDescriptor = ambiguousEdgePattern.getEdgeDescriptor();
            VariableExpr edgeVariable = ambiguousEdgeDescriptor.getVariableExpr();
            PathPatternExpr canonicalPathPatternExpr = (PathPatternExpr) canonicalExpr;
            List<EdgePatternExpr> canonicalEdges = canonicalPathPatternExpr.getEdgeExpressions();
            VertexPatternExpr canonicalLeftVertex = canonicalEdges.get(0).getLeftVertex();
            VertexPatternExpr canonicalRightVertex = canonicalEdges.get(canonicalEdges.size() - 1).getRightVertex();

            // Iterate through our edge list.
            List<EdgePatternExpr> edgeExpressions = pathPatternExpr.getEdgeExpressions();
            ListIterator<EdgePatternExpr> edgeIterator = edgeExpressions.listIterator();
            while (edgeIterator.hasNext()) {
                EdgePatternExpr workingEdge = edgeIterator.next();
                if (workingEdge.equals(ambiguousEdgePattern)) {
                    edgeIterator.remove();

                    // We need to generate new variables for each edge in our new path.
                    List<EdgePatternExpr> canonicalEdgeListCopy = new ArrayList<>();
                    for (EdgePatternExpr canonicalEdge : canonicalEdges) {
                        EdgePatternExpr canonicalEdgeCopy = deepCopyVisitor.visit(canonicalEdge, null);
                        EdgeDescriptor canonicalEdgeCopyDescriptor = canonicalEdgeCopy.getEdgeDescriptor();
                        VariableExpr edgeVariableCopy = graphixRewritingContext.getGraphixVariableCopy(edgeVariable);
                        canonicalEdgeCopyDescriptor.setVariableExpr(edgeVariableCopy);
                        canonicalEdgeListCopy.add(canonicalEdgeCopy);
                        edgeIterator.add(canonicalEdgeCopy);
                    }

                    // Determine our new vertex list (we want to keep the order of our current vertex list).
                    List<VertexPatternExpr> canonicalVertexListCopy = new ArrayList<>();
                    ListIterator<VertexPatternExpr> pathPatternVertexIterator =
                            pathPatternExpr.getVertexExpressions().listIterator();
                    while (pathPatternVertexIterator.hasNext()) {
                        VertexPatternExpr currentPathPatternVertex = pathPatternVertexIterator.next();
                        VariableExpr currentVariable = currentPathPatternVertex.getVariableExpr();
                        VariableExpr ambiguousLeftVar = ambiguousLeftVertex.getVariableExpr();
                        VariableExpr ambiguousRightVar = ambiguousRightVertex.getVariableExpr();
                        if (currentVariable.equals(ambiguousLeftVar)) {
                            // Add canonical path vertices.
                            List<VertexPatternExpr> canonicalVertices = canonicalPathPatternExpr.getVertexExpressions();
                            for (VertexPatternExpr vertexExpr : canonicalVertices) {
                                canonicalVertexListCopy.add(deepCopyVisitor.visit(vertexExpr, null));
                                VariableExpr vertexVar = vertexExpr.getVariableExpr();
                                if (!vertexVar.equals(ambiguousLeftVar) && !vertexVar.equals(ambiguousRightVar)) {
                                    pathPatternVertexIterator.add(vertexExpr);
                                }
                            }
                        }
                    }

                    // Build a new path record.
                    RecordConstructor pathRecord = new RecordConstructor();
                    pathRecord.setSourceLocation(workingEdge.getSourceLocation());
                    buildPathRecord(canonicalVertexListCopy, canonicalEdgeListCopy, pathRecord);
                    LetClause pathBinding = new LetClause(deepCopyVisitor.visit(edgeVariable, null), pathRecord);
                    pathPatternExpr.getReboundSubPathList().add(pathBinding);

                } else {
                    if (workingEdge.getLeftVertex().equals(ambiguousLeftVertex)) {
                        workingEdge.setLeftVertex(deepCopyVisitor.visit(canonicalLeftVertex, null));
                    }
                    if (workingEdge.getRightVertex().equals(ambiguousRightVertex)) {
                        workingEdge.setRightVertex(deepCopyVisitor.visit(canonicalRightVertex, null));
                    }
                }
            }

            // Iterate through our vertex list.
            replaceEdgeVerticesAndJoinCopiesInIterator(pathPatternExpr, ambiguousLeftVertex, ambiguousRightVertex,
                    canonicalLeftVertex, canonicalRightVertex);
        }
    }

    private void replaceEdgeVerticesAndJoinCopiesInIterator(PathPatternExpr pathPatternExpr,
            VertexPatternExpr ambiguousLeftVertex, VertexPatternExpr ambiguousRightVertex,
            VertexPatternExpr canonicalLeftVertex, VertexPatternExpr canonicalRightVertex) throws CompilationException {
        List<VertexPatternExpr> vertexExpressions = pathPatternExpr.getVertexExpressions();
        ListIterator<VertexPatternExpr> vertexIterator = vertexExpressions.listIterator();
        Map<VertexPatternExpr, VertexPatternExpr> replacementVertexMap = new HashMap<>();
        replacementVertexMap.put(ambiguousLeftVertex, canonicalLeftVertex);
        replacementVertexMap.put(ambiguousRightVertex, canonicalRightVertex);
        for (VertexPatternExpr vertexPatternExpr : vertexExpressions) {
            if (vertexPatternExpr.findHint(SubqueryVertexJoinAnnotation.class) != null) {
                SubqueryVertexJoinAnnotation hint = vertexPatternExpr.findHint(SubqueryVertexJoinAnnotation.class);
                if (hint.getSourceVertexVariable().equals(ambiguousLeftVertex.getVariableExpr())) {
                    VertexPatternExpr canonicalVertexCopy = deepCopyVisitor.visit(canonicalLeftVertex, null);
                    canonicalVertexCopy.setVariableExpr(vertexPatternExpr.getVariableExpr());
                    replacementVertexMap.put(vertexPatternExpr, canonicalVertexCopy);

                } else if (hint.getSourceVertexVariable().equals(ambiguousRightVertex.getVariableExpr())) {
                    VertexPatternExpr canonicalVertexCopy = deepCopyVisitor.visit(canonicalRightVertex, null);
                    canonicalVertexCopy.setVariableExpr(vertexPatternExpr.getVariableExpr());
                    replacementVertexMap.put(vertexPatternExpr, canonicalVertexCopy);
                }
            }
        }
        replaceVertexInIterator(replacementVertexMap, vertexIterator, deepCopyVisitor);
    }
}
