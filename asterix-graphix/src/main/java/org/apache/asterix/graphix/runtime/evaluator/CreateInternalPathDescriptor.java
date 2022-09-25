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
package org.apache.asterix.graphix.runtime.evaluator;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.graphix.function.GraphixFunctionIdentifiers;
import org.apache.asterix.graphix.runtime.pointable.InternalPathPointable;
import org.apache.asterix.graphix.runtime.pointable.SinglyLinkedListPointable;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * Create an initial path, which will consist of a single vertex (our argument) and zero edges.
 */
public class CreateInternalPathDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {
                return new IScalarEvaluator() {
                    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private final DataOutput dataOutput = resultStorage.getDataOutput();
                    private final IScalarEvaluator arg0Eval = args[0].createScalarEvaluator(ctx);
                    private final IPointable arg0Ptr = new VoidPointable();

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        arg0Eval.evaluate(tuple, arg0Ptr);
                        if (PointableHelper.checkAndSetMissingOrNull(result, arg0Ptr)) {
                            return;
                        }
                        resultStorage.reset();

                        try {
                            // Build our path header. We start with our type tag.
                            dataOutput.writeByte(InternalPathPointable.PATH_SERIALIZED_TYPE_TAG);

                            // Write the size of our vertex as a list item twice (to indicate we have no edges).
                            int vertexItemSize = arg0Ptr.getLength() + SinglyLinkedListPointable.LIST_ITEM_LENGTH_SIZE;
                            int startOfEdgeList = vertexItemSize + InternalPathPointable.PATH_HEADER_LENGTH;
                            dataOutput.writeInt(startOfEdgeList);
                            dataOutput.writeInt(startOfEdgeList);

                            // Write our vertex item (size + the item itself).
                            dataOutput.writeInt(vertexItemSize);
                            dataOutput.write(arg0Ptr.getByteArray(), arg0Ptr.getStartOffset(), arg0Ptr.getLength());

                        } catch (IOException e) {
                            throw HyracksDataException.create(e);
                        }
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return GraphixFunctionIdentifiers.CREATE_INTERNAL_PATH;
    }
}
