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
import org.apache.asterix.graphix.lang.expression.EdgePatternExpr;
import org.apache.asterix.graphix.lang.rewrites.util.LowerRewritingUtil;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.expression.FieldBinding;
import org.apache.asterix.lang.common.expression.ListConstructor;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.common.struct.Identifier;

public class EdgeDetailPrepare extends AbstractElementPrepare {
    public static final Identifier IDENTIFIER = new Identifier("EdgeDetail");
    public static final Identifier SOURCE_KEY_IDENTIFIER = new Identifier("SourceKey");
    public static final Identifier DEST_KEY_IDENTIFIER = new Identifier("DestinationKey");

    @Override
    protected void transformRecord(RecordConstructor schemaRecord, Expression inputExpr, Expression sourceExpr)
            throws CompilationException {
        if (!(inputExpr instanceof EdgePatternExpr)) {
            return;
        }
        EdgePatternExpr edgePatternExpr = (EdgePatternExpr) inputExpr;
        EdgeDescriptor edgeDescriptor = edgePatternExpr.getEdgeDescriptor();

        // Insert our detail record into our schema.
        RecordConstructor detailRecord = new RecordConstructor(new ArrayList<>());
        LiteralExpr fieldNameExpr = new LiteralExpr(new StringLiteral(IDENTIFIER.getValue()));
        FieldBinding detailRecordBinding = new FieldBinding(fieldNameExpr, detailRecord);
        schemaRecord.getFbList().add(detailRecordBinding);

        // Insert our element-label into our detail record.
        ElementLabelPrepare elementLabelPrepare = new ElementLabelPrepare();
        elementLabelPrepare.transformRecord(detailRecord, inputExpr, sourceExpr);

        // Insert our edge-direction into our detail record.
        EdgeDirectionPrepare edgeDirectionPrepare = new EdgeDirectionPrepare();
        edgeDirectionPrepare.transformRecord(detailRecord, inputExpr, sourceExpr);

        // Insert our source-key into our detail record.
        GraphElementIdentifier edgeIdentifier = edgeDescriptor.generateIdentifiers(graphIdentifier).get(0);
        List<List<String>> edgeSourceKey = elementLookupTable.getEdgeSourceKey(edgeIdentifier);
        List<Expression> sourceKeyExprList = LowerRewritingUtil.buildAccessorList(sourceExpr, edgeSourceKey).stream()
                .map(e -> (Expression) e).collect(Collectors.toList());
        ListConstructor sourceKeyFieldValueExpr = new ListConstructor(ORDERED_LIST_CONSTRUCTOR, sourceKeyExprList);
        LiteralExpr sourceKeyFieldNameExpr = new LiteralExpr(new StringLiteral(SOURCE_KEY_IDENTIFIER.getValue()));
        detailRecord.getFbList().add(new FieldBinding(sourceKeyFieldNameExpr, sourceKeyFieldValueExpr));

        // Insert our dest-key into our detail record.
        List<List<String>> edgeDestKey = elementLookupTable.getEdgeDestKey(edgeIdentifier);
        List<Expression> destKeyExprList = LowerRewritingUtil.buildAccessorList(sourceExpr, edgeDestKey).stream()
                .map(e -> (Expression) e).collect(Collectors.toList());
        ListConstructor destKeyFieldValueExpr = new ListConstructor(ORDERED_LIST_CONSTRUCTOR, destKeyExprList);
        LiteralExpr destKeyFieldNameExpr = new LiteralExpr(new StringLiteral(DEST_KEY_IDENTIFIER.getValue()));
        detailRecord.getFbList().add(new FieldBinding(destKeyFieldNameExpr, destKeyFieldValueExpr));
    }
}
