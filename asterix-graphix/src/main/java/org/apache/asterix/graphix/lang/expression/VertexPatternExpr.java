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

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.common.metadata.GraphIdentifier;
import org.apache.asterix.graphix.common.metadata.VertexIdentifier;
import org.apache.asterix.graphix.lang.struct.ElementLabel;
import org.apache.asterix.graphix.lang.visitor.base.IGraphixLangVisitor;
import org.apache.asterix.lang.common.base.AbstractExpression;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;

/**
 * A query vertex (not to be confused with a vertex constructor) is composed of:
 * <ul>
 *  <li>A set of labels (which may be empty).</li>
 *  <li>A variable (which may initially be null).</li>
 *  <li>A filter expression (which may be null).</li>
 * </ul>
 */
public class VertexPatternExpr extends AbstractExpression {
    private final Set<ElementLabel> labels;
    private final Expression filterExpr;
    private VariableExpr variableExpr;

    public VertexPatternExpr(VariableExpr variableExpr, Expression filterExpr, Set<ElementLabel> labels) {
        this.variableExpr = variableExpr;
        this.filterExpr = filterExpr;
        this.labels = labels;
    }

    public Set<ElementLabel> getLabels() {
        return labels;
    }

    public Expression getFilterExpr() {
        return filterExpr;
    }

    public VariableExpr getVariableExpr() {
        return variableExpr;
    }

    public void setVariableExpr(VariableExpr variableExpr) {
        this.variableExpr = variableExpr;
    }

    public List<VertexIdentifier> generateIdentifiers(GraphIdentifier graphIdentifier) {
        return labels.stream().map(v -> new VertexIdentifier(graphIdentifier, v)).collect(Collectors.toList());
    }

    @Override
    public int hashCode() {
        return Objects.hash(labels, variableExpr);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof VertexPatternExpr)) {
            return false;
        }
        VertexPatternExpr that = (VertexPatternExpr) object;
        return Objects.equals(this.labels, that.labels) && Objects.equals(this.variableExpr, that.variableExpr)
                && Objects.equals(this.filterExpr, that.filterExpr);
    }

    @Override
    public String toString() {
        String labelsString = labels.stream().map(ElementLabel::toString).collect(Collectors.joining("|"));
        String variableString = (variableExpr != null) ? variableExpr.getVar().toString() : "";
        String filterString = (filterExpr != null) ? (" WHERE " + filterExpr + " ") : "";
        return String.format("(%s:%s%s)", variableString, labelsString, filterString);
    }

    @Override
    public Kind getKind() {
        return null;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return ((IGraphixLangVisitor<R, T>) visitor).visit(this, arg);
    }
}
