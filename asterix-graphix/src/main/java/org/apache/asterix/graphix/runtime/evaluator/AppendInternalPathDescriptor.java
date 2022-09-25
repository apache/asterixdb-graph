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

import static org.apache.asterix.graphix.runtime.pointable.InternalPathPointable.HEADER_EDGE_LIST_END;
import static org.apache.asterix.graphix.runtime.pointable.InternalPathPointable.HEADER_VERTEX_LIST_END;
import static org.apache.asterix.graphix.runtime.pointable.InternalPathPointable.PATH_HEADER_LENGTH;
import static org.apache.asterix.graphix.runtime.pointable.InternalPathPointable.PATH_SERIALIZED_TYPE_TAG;
import static org.apache.asterix.graphix.runtime.pointable.SinglyLinkedListPointable.LIST_ITEM_LENGTH_SIZE;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.graphix.function.GraphixFunctionIdentifiers;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * Given a vertex (our first argument), an edge (our second argument), and an existing path (our third argument), create
 * a new path that includes our given vertex and edge. This action is append-only, so we do not peer inside the existing
 * vertex or edge lists.
 */
public class AppendInternalPathDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;

    // We
    private MaterializeInternalPathCallbackFactory materializeInternalPathCallbackFactory;

    @Override
    public void setImmutableStates(Object... states) {
        if (states != null) {
            materializeInternalPathCallbackFactory = (MaterializeInternalPathCallbackFactory) states[0];
        }
    }

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
                    private final IScalarEvaluator arg1Eval = args[1].createScalarEvaluator(ctx);
                    private final IScalarEvaluator arg2Eval = args[2].createScalarEvaluator(ctx);
                    private final IPointable arg0Ptr = new VoidPointable();
                    private final IPointable arg1Ptr = new VoidPointable();
                    private final IPointable arg2Ptr = new VoidPointable();

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        arg0Eval.evaluate(tuple, arg0Ptr);
                        arg1Eval.evaluate(tuple, arg1Ptr);
                        arg2Eval.evaluate(tuple, arg2Ptr);
                        if (PointableHelper.checkAndSetMissingOrNull(result, arg0Ptr, arg1Ptr, arg2Ptr)) {
                            return;
                        }
                        resultStorage.reset();

                        try {
                            // Build our path header. We start with our type tag.
                            dataOutput.writeByte(PATH_SERIALIZED_TYPE_TAG);

                            // Write the end offset of our new vertex list.
                            int oldVertexLocalListEnd = IntegerPointable.getInteger(arg2Ptr.getByteArray(),
                                    arg2Ptr.getStartOffset() + HEADER_VERTEX_LIST_END);
                            int vertexItemSize = arg0Ptr.getLength() + LIST_ITEM_LENGTH_SIZE;
                            dataOutput.writeInt(oldVertexLocalListEnd + vertexItemSize);
                            //                            materializeInternalPathCallbackFactory

                            // Write the end offset of our new edge list.
                            int oldEdgeLocalListEnd = IntegerPointable.getInteger(arg2Ptr.getByteArray(),
                                    arg2Ptr.getStartOffset() + HEADER_EDGE_LIST_END);
                            int edgeItemSize = arg1Ptr.getLength() + LIST_ITEM_LENGTH_SIZE;
                            dataOutput.writeInt(oldEdgeLocalListEnd + edgeItemSize);

                            // Copy all of our old vertices.
                            int oldVertexAbsoluteListStart = arg2Ptr.getStartOffset() + PATH_HEADER_LENGTH;
                            dataOutput.write(arg2Ptr.getByteArray(), oldVertexAbsoluteListStart,
                                    (oldVertexLocalListEnd + arg2Ptr.getStartOffset()) - oldVertexAbsoluteListStart);

                            // Copy our new vertex.
                            dataOutput.writeInt(arg0Ptr.getLength());
                            dataOutput.write(arg0Ptr.getByteArray(), arg0Ptr.getStartOffset(), arg0Ptr.getLength());

                            // Copy all of our new edges.
                            int oldEdgeAbsoluteListStart = oldVertexLocalListEnd + arg2Ptr.getStartOffset();
                            dataOutput.write(arg2Ptr.getByteArray(), oldEdgeAbsoluteListStart,
                                    (oldEdgeLocalListEnd + arg2Ptr.getStartOffset()) - oldEdgeAbsoluteListStart);

                            // Copy our new edge.
                            dataOutput.writeInt(arg1Ptr.getLength());
                            dataOutput.write(arg1Ptr.getByteArray(), arg1Ptr.getStartOffset(), arg1Ptr.getLength());

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
        return GraphixFunctionIdentifiers.APPEND_INTERNAL_PATH;
    }

    public static final class MaterializeInternalPathCallbackFactory {
        //        private Consumer<VoidPointable>
    }
}
