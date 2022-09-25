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
package org.apache.asterix.graphix.lang.expression;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.visitor.base.IGraphixLangVisitor;
import org.apache.asterix.lang.common.base.AbstractExpression;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;

/**
 * A path is composed of:
 * <ul>
 *  <li>A list of {@link VertexPatternExpr} instances.</li>
 *  <li>A list of {@link EdgePatternExpr} instances that utilize the aforementioned vertices.</li>
 *  <li>An optional variable binding all vertices and edges to a path record.</li>
 *  <li>A list of {@link LetClause} nodes that represent expanded sub-paths.</li>
 * </ul>
 */
public class PathPatternExpr extends AbstractExpression {
    private final List<LetClause> reboundSubPathExpressions;
    private final List<VertexPatternExpr> vertexExpressions;
    private final List<EdgePatternExpr> edgeExpressions;
    private VariableExpr variableExpr;

    public PathPatternExpr(List<VertexPatternExpr> vertexExpressions, List<EdgePatternExpr> edgeExpressions,
            VariableExpr variableExpr) {
        this.vertexExpressions = Objects.requireNonNull(vertexExpressions);
        this.edgeExpressions = Objects.requireNonNull(edgeExpressions);
        this.variableExpr = variableExpr;

        // We will build this list on canonicalization.
        this.reboundSubPathExpressions = new ArrayList<>();
    }

    public List<VertexPatternExpr> getVertexExpressions() {
        return vertexExpressions;
    }

    public List<EdgePatternExpr> getEdgeExpressions() {
        return edgeExpressions;
    }

    public VariableExpr getVariableExpr() {
        return variableExpr;
    }

    public List<LetClause> getReboundSubPathList() {
        return reboundSubPathExpressions;
    }

    public void setVariableExpr(VariableExpr variableExpr) {
        this.variableExpr = variableExpr;
    }

    @Override
    public Kind getKind() {
        return null;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return ((IGraphixLangVisitor<R, T>) visitor).visit(this, arg);
    }

    @Override
    public String toString() {
        String edgeString = edgeExpressions.stream().map(EdgePatternExpr::toString).collect(Collectors.joining(","));
        String variableString = (variableExpr != null) ? (" AS " + variableExpr) : "";
        return String.format("%s%s%s",
                vertexExpressions.stream().map(VertexPatternExpr::toString).collect(Collectors.joining(",")),
                (edgeString.equals("") ? "" : ", " + edgeString), variableString);
    }
}
