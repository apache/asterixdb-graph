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
package org.apache.asterix.graphix.function.prepare;

import org.apache.asterix.graphix.lang.expression.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.VertexPatternExpr;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.expression.FieldBinding;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.common.struct.Identifier;

public class EdgeSourceVertexPrepare extends AbstractElementPrepare {
    public static final Identifier IDENTIFIER = new Identifier("EdgeSourceVertex");

    @Override
    protected void transformRecord(RecordConstructor schemaRecord, Expression inputExpr, Expression sourceExpr) {
        if (!(inputExpr instanceof EdgePatternExpr)) {
            return;
        }
        EdgePatternExpr edgePatternExpr = (EdgePatternExpr) inputExpr;
        EdgeDescriptor edgeDescriptor = edgePatternExpr.getEdgeDescriptor();
        EdgeDescriptor.EdgeDirection edgeDirection = edgeDescriptor.getEdgeDirection();
        VertexPatternExpr sourceVertexExpr = (edgeDirection == EdgeDescriptor.EdgeDirection.LEFT_TO_RIGHT)
                ? edgePatternExpr.getLeftVertex() : edgePatternExpr.getRightVertex();
        VariableExpr sourceVariableExprCopy = new VariableExpr(sourceVertexExpr.getVariableExpr().getVar());
        LiteralExpr fieldNameExpr = new LiteralExpr(new StringLiteral(IDENTIFIER.getValue()));
        FieldBinding fieldBinding = new FieldBinding(fieldNameExpr, sourceVariableExprCopy);
        schemaRecord.getFbList().add(fieldBinding);
    }
}
