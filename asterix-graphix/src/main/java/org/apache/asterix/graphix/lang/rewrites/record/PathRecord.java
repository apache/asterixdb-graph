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
package org.apache.asterix.graphix.lang.rewrites.record;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.expression.FieldBinding;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.literal.NullLiteral;
import org.apache.asterix.lang.common.literal.StringLiteral;

/**
 * Construct the record associated with an edge of a path. For vertices that do not belong to any edge, we build a
 * pseudo-edge record composed of: dangling vertex, NULL, NULL. This pseudo-edge record is also constructed in Neo4J.
 *
 * @see org.apache.asterix.graphix.lang.rewrites.visitor.GraphixLoweringVisitor
 */
public class PathRecord implements IElementRecord {
    private final Expression pathExpression;

    // A path record is composed of the following fields.
    public static final String EDGE_FIELD_NAME = "Edge";
    public static final String LEFT_VERTEX_FIELD_NAME = "LeftVertex";
    public static final String RIGHT_VERTEX_FIELD_NAME = "RightVertex";

    public PathRecord(Expression leftVertexExpr, Expression edgeExpr, Expression rightVertexExpr) {
        List<FieldBinding> bindings = new ArrayList<>();
        bindings.add(new FieldBinding(new LiteralExpr(new StringLiteral(LEFT_VERTEX_FIELD_NAME)), leftVertexExpr));
        bindings.add(new FieldBinding(new LiteralExpr(new StringLiteral(EDGE_FIELD_NAME)), edgeExpr));
        bindings.add(new FieldBinding(new LiteralExpr(new StringLiteral(RIGHT_VERTEX_FIELD_NAME)), rightVertexExpr));
        this.pathExpression = new RecordConstructor(bindings);
    }

    public PathRecord(Expression leftVertexExpr) {
        Expression rightVertexExpr = new LiteralExpr(NullLiteral.INSTANCE);
        Expression edgeExpr = new LiteralExpr(NullLiteral.INSTANCE);

        // Build our path expression.
        List<FieldBinding> bindings = new ArrayList<>();
        bindings.add(new FieldBinding(new LiteralExpr(new StringLiteral(LEFT_VERTEX_FIELD_NAME)), leftVertexExpr));
        bindings.add(new FieldBinding(new LiteralExpr(new StringLiteral(EDGE_FIELD_NAME)), edgeExpr));
        bindings.add(new FieldBinding(new LiteralExpr(new StringLiteral(RIGHT_VERTEX_FIELD_NAME)), rightVertexExpr));
        this.pathExpression = new RecordConstructor(bindings);
    }

    @Override
    public Expression getExpression() {
        return pathExpression;
    }
}
