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
package org.apache.asterix.graphix.runtime.function;

import org.apache.asterix.graphix.runtime.evaluator.AppendInternalPathDescriptor;
import org.apache.asterix.graphix.runtime.evaluator.CreateInternalPathDescriptor;
import org.apache.asterix.graphix.runtime.evaluator.IsDistinctEdgeDescriptor;
import org.apache.asterix.graphix.runtime.evaluator.IsDistinctEverythingDescriptor;
import org.apache.asterix.graphix.runtime.evaluator.IsDistinctVertexDescriptor;
import org.apache.asterix.om.functions.IFunctionCollection;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.functions.IFunctionRegistrant;
import org.apache.asterix.om.functions.IFunctionTypeInferer;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;

public class GraphixFunctionRegistrant implements IFunctionRegistrant {
    private static final long serialVersionUID = 1L;

    @Override
    public void register(IFunctionCollection fc) {
        fc.add(IsDistinctVertexDescriptor::new);
        fc.add(IsDistinctEdgeDescriptor::new);
        fc.add(IsDistinctEverythingDescriptor::new);
        fc.add(CreateInternalPathDescriptor::new);

        // We have a callback-factory we need to pass to our APPEND-INTERNAL-PATH evaluator-factory.
        fc.add(new IFunctionDescriptorFactory() {
            @Override
            public IFunctionTypeInferer createFunctionTypeInferer() {
                return (expr, fd, context, compilerProps) -> {
                    AbstractFunctionCallExpression funcCallExpr = (AbstractFunctionCallExpression) expr;
                    fd.setImmutableStates(funcCallExpr.getOpaqueParameters()[0]);
                };
            }

            @Override
            public IFunctionDescriptor createFunctionDescriptor() {
                return new AppendInternalPathDescriptor();
            }
        });
    }
}
