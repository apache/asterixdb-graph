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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.graphix.common.metadata.GraphIdentifier;
import org.apache.asterix.graphix.lang.annotation.GraphixSchemaAnnotation;
import org.apache.asterix.graphix.lang.rewrite.common.ElementLookupTable;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.FieldBinding;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.om.functions.BuiltinFunctions;

/**
 * We want to attach all of our schema detail to this "_GraphixSchemaDetail" field. If we are given a
 * {@link RecordConstructor}, then we just add a new field binding. If we are given anything else, we need to wrap our
 * input expression with a {@link BuiltinFunctions#RECORD_ADD} function call that includes this "_GraphixSchemaDetail"
 * field.
 */
public abstract class AbstractElementPrepare implements IFunctionPrepare {
    public static final Identifier GRAPHIX_SCHEMA_IDENTIFIER = new Identifier("_GraphixSchemaDetail");

    // We need to pass this info down to our child.
    protected ElementLookupTable elementLookupTable;
    protected GraphIdentifier graphIdentifier;

    protected abstract void transformRecord(RecordConstructor schemaRecord, Expression inputExpr, Expression sourceExpr)
            throws CompilationException;

    @Override
    public Expression prepare(Expression sourceExpr, Expression inputExpr, GraphIdentifier graphIdentifier,
            ElementLookupTable elementLookupTable) throws CompilationException {
        this.elementLookupTable = elementLookupTable;
        this.graphIdentifier = graphIdentifier;

        // Transform our record expression.
        if (sourceExpr.getKind() == Expression.Kind.RECORD_CONSTRUCTOR_EXPRESSION) {
            RecordConstructor recordConstructor = (RecordConstructor) sourceExpr;
            List<FieldBinding> fieldBindingList = recordConstructor.getFbList();
            Optional<FieldBinding> schemaBinding = fieldBindingList.stream().filter(f -> {
                LiteralExpr fieldNameExpr = (LiteralExpr) f.getLeftExpr();
                StringLiteral fieldNameValue = (StringLiteral) fieldNameExpr.getValue();
                return fieldNameValue.getStringValue().equals(GRAPHIX_SCHEMA_IDENTIFIER.getValue());
            }).findFirst();

            if (schemaBinding.isPresent()) {
                // We have previously introduced schema detail into this expression. Add to our existing record.
                FieldBinding schemaRecordBinding = schemaBinding.get();
                RecordConstructor schemaRecord = (RecordConstructor) schemaRecordBinding.getRightExpr();
                transformRecord(schemaRecord, inputExpr, sourceExpr);

            } else {
                // We need to introduce a schema detail record.
                RecordConstructor schemaRecord = new RecordConstructor(new ArrayList<>());
                schemaRecord.addHint(GraphixSchemaAnnotation.INSTANCE);
                schemaRecord.setSourceLocation(sourceExpr.getSourceLocation());
                transformRecord(schemaRecord, inputExpr, sourceExpr);
                LiteralExpr fieldNameExpr = new LiteralExpr(new StringLiteral(GRAPHIX_SCHEMA_IDENTIFIER.getValue()));
                FieldBinding newFieldBinding = new FieldBinding(fieldNameExpr, schemaRecord);
                fieldBindingList.add(newFieldBinding);

            }
            return sourceExpr;

        } else if (sourceExpr.getKind() == Expression.Kind.CALL_EXPRESSION) {
            CallExpr callExpr = (CallExpr) sourceExpr;
            if (callExpr.findHint(GraphixSchemaAnnotation.class) == GraphixSchemaAnnotation.INSTANCE) {
                // We have a previously decorated Graphix CALL-EXPR. Add to this record.
                RecordConstructor schemaRecord = (RecordConstructor) callExpr.getExprList().get(2);
                transformRecord(schemaRecord, inputExpr, sourceExpr);
                return sourceExpr;
            }
        }

        // We have a non-record-constructor expression that we need to add a schema detail record to.
        FunctionSignature functionSignature = new FunctionSignature(BuiltinFunctions.RECORD_ADD);
        List<Expression> callExprArguments = new ArrayList<>();
        RecordConstructor schemaRecord = new RecordConstructor(new ArrayList<>());
        transformRecord(schemaRecord, inputExpr, sourceExpr);

        // Finalize our new decorated schema detail record.
        callExprArguments.add(sourceExpr);
        callExprArguments.add(new LiteralExpr(new StringLiteral(GRAPHIX_SCHEMA_IDENTIFIER.getValue())));
        callExprArguments.add(schemaRecord);
        CallExpr callExpr = new CallExpr(functionSignature, callExprArguments);
        callExpr.setSourceLocation(sourceExpr.getSourceLocation());
        callExpr.addHint(GraphixSchemaAnnotation.INSTANCE);
        return callExpr;
    }
}
