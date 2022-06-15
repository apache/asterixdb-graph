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

import static org.apache.asterix.lang.common.expression.ListConstructor.Type.ORDERED_LIST_CONSTRUCTOR;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.common.metadata.GraphElementIdentifier;
import org.apache.asterix.graphix.lang.expression.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrites.util.LowerRewritingUtil;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.expression.FieldBinding;
import org.apache.asterix.lang.common.expression.ListConstructor;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.common.struct.Identifier;

public class VertexDetailPrepare extends AbstractElementPrepare {
    public static final Identifier IDENTIFIER = new Identifier("VertexDetail");
    public static final Identifier KEY_IDENTIFIER = new Identifier("VertexKey");

    @Override
    protected void transformRecord(RecordConstructor schemaRecord, Expression inputExpr, Expression sourceExpr)
            throws CompilationException {
        if (!(inputExpr instanceof VertexPatternExpr)) {
            return;
        }
        VertexPatternExpr vertexPatternExpr = (VertexPatternExpr) inputExpr;

        // Insert our detail record into our schema.
        RecordConstructor detailRecord = new RecordConstructor(new ArrayList<>());
        LiteralExpr fieldNameExpr = new LiteralExpr(new StringLiteral(IDENTIFIER.getValue()));
        FieldBinding detailRecordBinding = new FieldBinding(fieldNameExpr, detailRecord);
        schemaRecord.getFbList().add(detailRecordBinding);

        // Insert our element-label into our detail record.
        ElementLabelPrepare elementLabelPrepare = new ElementLabelPrepare();
        elementLabelPrepare.transformRecord(detailRecord, inputExpr, sourceExpr);

        // Insert our vertex-key into our detail record.
        GraphElementIdentifier vertexIdentifier = vertexPatternExpr.generateIdentifiers(graphIdentifier).get(0);
        List<List<String>> vertexKey = elementLookupTable.getVertexKey(vertexIdentifier);
        List<Expression> vertexKeyExprList = LowerRewritingUtil.buildAccessorList(sourceExpr, vertexKey).stream()
                .map(e -> (Expression) e).collect(Collectors.toList());
        ListConstructor keyFieldValueExpr = new ListConstructor(ORDERED_LIST_CONSTRUCTOR, vertexKeyExprList);
        LiteralExpr keyFieldNameExpr = new LiteralExpr(new StringLiteral(KEY_IDENTIFIER.getValue()));
        detailRecord.getFbList().add(new FieldBinding(keyFieldNameExpr, keyFieldValueExpr));
    }
}
