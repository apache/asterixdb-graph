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

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.rewrites.visitor.IGraphixLangVisitor;
import org.apache.asterix.lang.common.base.AbstractExpression;
import org.apache.asterix.lang.common.base.AbstractLangExpression;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;

public class GraphConstructor extends AbstractExpression {
    private final List<VertexElement> vertexElements;
    private final List<EdgeElement> edgeElements;

    public GraphConstructor(List<VertexElement> vertexElements, List<EdgeElement> edgeElements) {
        this.vertexElements = vertexElements;
        this.edgeElements = edgeElements;
    }

    public List<VertexElement> getVertexElements() {
        return vertexElements;
    }

    public List<EdgeElement> getEdgeElements() {
        return edgeElements;
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
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof GraphConstructor)) {
            return false;
        }
        GraphConstructor that = (GraphConstructor) object;
        return vertexElements.equals(that.vertexElements) && edgeElements.equals(that.edgeElements);
    }

    public static class VertexElement extends AbstractLangExpression {
        private final List<Integer> primaryKeySourceIndicators;
        private final List<List<String>> primaryKeyFields;
        private final Expression expression;
        private final String definition;
        private final String label;

        public VertexElement(String label, List<List<String>> primaryKeyFields,
                List<Integer> primaryKeySourceIndicators, Expression expression, String definition) {
            this.primaryKeySourceIndicators = primaryKeySourceIndicators;
            this.primaryKeyFields = primaryKeyFields;
            this.expression = expression;
            this.definition = definition;
            this.label = label;
        }

        public List<List<String>> getPrimaryKeyFields() {
            return primaryKeyFields;
        }

        public List<Integer> getPrimaryKeySourceIndicators() {
            return primaryKeySourceIndicators;
        }

        public Expression getExpression() {
            return expression;
        }

        public String getDefinition() {
            return definition;
        }

        public String getLabel() {
            return label;
        }

        @Override
        public String toString() {
            return "(:" + label + ") AS " + definition;
        }

        @Override
        public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
            return ((IGraphixLangVisitor<R, T>) visitor).visit(this, arg);
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (!(object instanceof VertexElement)) {
                return false;
            }
            VertexElement that = (VertexElement) object;
            return primaryKeySourceIndicators.equals(that.primaryKeySourceIndicators)
                    && primaryKeyFields.equals(that.primaryKeyFields) && expression.equals(that.expression)
                    && definition.equals(that.definition) && label.equals(that.label);
        }
    }

    public static class EdgeElement extends AbstractLangExpression {
        private final List<Integer> destinationKeySourceIndicators;
        private final List<Integer> sourceKeySourceIndicators;
        private final List<Integer> primaryKeySourceIndicators;

        private final List<List<String>> destinationKeyFields;
        private final List<List<String>> sourceKeyFields;
        private final List<List<String>> primaryKeyFields;

        private final String destinationLabel, edgeLabel, sourceLabel;
        private final Expression expression;
        private final String definition;

        public EdgeElement(String edgeLabel, String destinationLabel, String sourceLabel,
                List<List<String>> primaryKeyFields, List<Integer> primaryKeySourceIndicators,
                List<List<String>> destinationKeyFields, List<Integer> destinationKeySourceIndicators,
                List<List<String>> sourceKeyFields, List<Integer> sourceKeySourceIndicators, Expression expression,
                String definition) {
            this.destinationKeySourceIndicators = destinationKeySourceIndicators;
            this.sourceKeySourceIndicators = sourceKeySourceIndicators;
            this.primaryKeySourceIndicators = primaryKeySourceIndicators;
            this.destinationKeyFields = destinationKeyFields;
            this.sourceKeyFields = sourceKeyFields;
            this.primaryKeyFields = primaryKeyFields;
            this.destinationLabel = destinationLabel;
            this.edgeLabel = edgeLabel;
            this.sourceLabel = sourceLabel;
            this.expression = expression;
            this.definition = definition;
        }

        public List<Integer> getDestinationKeySourceIndicators() {
            return destinationKeySourceIndicators;
        }

        public List<Integer> getSourceKeySourceIndicators() {
            return sourceKeySourceIndicators;
        }

        public List<Integer> getPrimaryKeySourceIndicators() {
            return primaryKeySourceIndicators;
        }

        public List<List<String>> getDestinationKeyFields() {
            return destinationKeyFields;
        }

        public List<List<String>> getSourceKeyFields() {
            return sourceKeyFields;
        }

        public List<List<String>> getPrimaryKeyFields() {
            return primaryKeyFields;
        }

        public String getDestinationLabel() {
            return destinationLabel;
        }

        public String getEdgeLabel() {
            return edgeLabel;
        }

        public String getSourceLabel() {
            return sourceLabel;
        }

        public Expression getExpression() {
            return expression;
        }

        public String getDefinition() {
            return definition;
        }

        @Override
        public String toString() {
            String edgeBodyPattern = "[:" + edgeLabel + "]";
            String sourceNodePattern = "(:" + sourceLabel + ")";
            String destinationNodePattern = "(:" + destinationLabel + ")";
            String edgePattern = sourceNodePattern + "-" + edgeBodyPattern + "->" + destinationNodePattern;
            return (definition == null) ? edgePattern : (edgePattern + " AS " + definition);
        }

        @Override
        public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
            return ((IGraphixLangVisitor<R, T>) visitor).visit(this, arg);
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (!(object instanceof EdgeElement)) {
                return false;
            }
            EdgeElement that = (EdgeElement) object;
            return destinationKeySourceIndicators.equals(that.destinationKeySourceIndicators)
                    && sourceKeySourceIndicators.equals(that.sourceKeySourceIndicators)
                    && primaryKeySourceIndicators.equals(that.primaryKeySourceIndicators)
                    && destinationKeyFields.equals(that.destinationKeyFields)
                    && sourceKeyFields.equals(that.sourceKeyFields) && primaryKeyFields.equals(that.primaryKeyFields)
                    && destinationLabel.equals(that.destinationLabel) && edgeLabel.equals(that.edgeLabel)
                    && sourceLabel.equals(that.sourceLabel) && expression.equals(that.expression)
                    && definition.equals(that.definition);
        }
    }
}
