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

import java.util.Collections;

import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.graphix.common.metadata.GraphElementIdentifier;
import org.apache.asterix.graphix.common.metadata.GraphIdentifier;
import org.apache.asterix.graphix.function.GraphixFunctionIdentifiers;
import org.apache.asterix.lang.common.expression.CallExpr;

public class GraphElementBodyExpr extends CallExpr implements IGraphExpr {
    // A graph element is uniquely identified by its identifier.
    private final GraphElementIdentifier identifier;

    public GraphElementBodyExpr(GraphElementIdentifier identifier) {
        super(new FunctionSignature(GraphixFunctionIdentifiers.GRAPH_ELEMENT_BODY), Collections.emptyList(), null);
        this.identifier = identifier;
    }

    public GraphElementIdentifier getIdentifier() {
        return identifier;
    }

    public GraphIdentifier getGraphIdentifier() {
        return identifier.getGraphIdentifier();
    }

    @Override
    public Kind getKind() {
        return null;
    }

    @Override
    public GraphExprKind getGraphExprKind() {
        return GraphExprKind.GRAPH_ELEMENT_BODY;
    }
}
