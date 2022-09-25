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
package org.apache.asterix.graphix.lang.struct;

import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.asterix.graphix.lang.expression.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.VertexPatternExpr;
import org.apache.asterix.lang.common.base.AbstractExpression;

/**
 * A container for a collection of vertices and edges. In contrast to the expression
 * {@link org.apache.asterix.graphix.lang.expression.PathPatternExpr}, the following elements do not have to be
 * connected.
 */
public class PatternGroup implements Iterable<AbstractExpression> {
    // Users should add to the following sets directly.
    private final Set<VertexPatternExpr> vertexPatternSet = new HashSet<>();
    private final Set<EdgePatternExpr> edgePatternSet = new HashSet<>();

    public Set<VertexPatternExpr> getVertexPatternSet() {
        return vertexPatternSet;
    }

    public Set<EdgePatternExpr> getEdgePatternSet() {
        return edgePatternSet;
    }

    public void replace(VertexPatternExpr searchExpression, VertexPatternExpr replaceExpression) {
        if (!vertexPatternSet.contains(searchExpression)) {
            throw new IllegalArgumentException("Vertex pattern not found in group!");
        }
        vertexPatternSet.remove(searchExpression);
        vertexPatternSet.add(replaceExpression);
    }

    public void replace(EdgePatternExpr searchExpression, EdgePatternExpr replaceExpression) {
        if (!edgePatternSet.contains(searchExpression)) {
            throw new IllegalArgumentException("Edge pattern not found in group!");
        }
        edgePatternSet.remove(searchExpression);
        edgePatternSet.add(replaceExpression);
    }

    @Override
    public Iterator<AbstractExpression> iterator() {
        Iterator<VertexPatternExpr> vertexPatternIterator = vertexPatternSet.iterator();
        Iterator<EdgePatternExpr> edgePatternIterator = edgePatternSet.iterator();
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return vertexPatternIterator.hasNext() || edgePatternIterator.hasNext();
            }

            @Override
            public AbstractExpression next() {
                if (vertexPatternIterator.hasNext()) {
                    return vertexPatternIterator.next();
                }
                if (edgePatternIterator.hasNext()) {
                    return edgePatternIterator.next();
                }
                throw new NoSuchElementException();
            }
        };
    }
}
