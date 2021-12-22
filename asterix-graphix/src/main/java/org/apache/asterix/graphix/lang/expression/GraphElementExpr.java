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

import org.apache.asterix.common.functions.FunctionConstants;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.graphix.common.metadata.GraphElementIdentifier;
import org.apache.asterix.graphix.common.metadata.GraphIdentifier;
import org.apache.asterix.graphix.lang.statement.GraphElementDecl;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.util.ExpressionUtils;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public class GraphElementExpr extends CallExpr {
    public static final String GRAPH_ELEMENT_FUNCTION_NAME = "graph-element";
    public static final FunctionIdentifier GRAPH_ELEMENT_FUNCTION_ID =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, GRAPH_ELEMENT_FUNCTION_NAME, 5);

    // A graph element is uniquely identified by its identifier.
    private final GraphElementIdentifier identifier;

    public GraphElementExpr(GraphElementIdentifier identifier) {
        super(new FunctionSignature(GRAPH_ELEMENT_FUNCTION_ID), Collections.emptyList(), null);
        this.identifier = identifier;
    }

    public GraphElementIdentifier getIdentifier() {
        return identifier;
    }

    public GraphIdentifier getGraphIdentifier() {
        return identifier.getGraphIdentifier();
    }

    public static Query createGraphElementAccessorQuery(GraphElementDecl graphElementDecl) {
        GraphElementExpr functionCall = new GraphElementExpr(graphElementDecl.getIdentifier());
        return ExpressionUtils.createWrappedQuery(functionCall, graphElementDecl.getSourceLocation());
    }
}
