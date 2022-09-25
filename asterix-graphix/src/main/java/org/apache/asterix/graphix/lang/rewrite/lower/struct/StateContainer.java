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
package org.apache.asterix.graphix.lang.rewrite.lower.struct;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.apache.asterix.lang.common.expression.VariableExpr;

public class StateContainer implements Iterable<VariableExpr> {
    private final VariableExpr iterationVariable;
    private final VariableExpr joinVariable;

    public StateContainer(VariableExpr iterationVariable, VariableExpr joinVariable) {
        this.iterationVariable = iterationVariable;
        this.joinVariable = joinVariable;
    }

    public VariableExpr getIterationVariable() {
        return iterationVariable;
    }

    public VariableExpr getJoinVariable() {
        return joinVariable;
    }

    @Override
    public int hashCode() {
        return Objects.hash(iterationVariable, joinVariable);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof StateContainer)) {
            return false;
        }
        StateContainer that = (StateContainer) object;
        return Objects.equals(this.iterationVariable, that.iterationVariable)
                && Objects.equals(this.joinVariable, that.joinVariable);
    }

    @Override
    public Iterator<VariableExpr> iterator() {
        return List.of(iterationVariable, joinVariable).iterator();
    }
}
