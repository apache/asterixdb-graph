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
package org.apache.asterix.graphix.metadata.entity.dependency;

import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.om.base.AGeneratedUUID;
import org.apache.asterix.om.base.AUUID;

/**
 * A collection of {@link org.apache.asterix.graphix.metadata.entity.schema.Graph} dependencies associated with a
 * {@link org.apache.asterix.metadata.entities.Function} instance. This does **not** include non-graph dependencies
 * for functions.
 */
public class FunctionRequirements implements IEntityRequirements {
    private final Set<DependencyIdentifier> functionRequirements;
    private final FunctionSignature functionSignature;

    // Physically, our requirements are indexed by a UUID. Logically, we ignore this.
    private final AUUID primaryKeyValue;

    public FunctionRequirements(FunctionSignature functionSignature, Set<DependencyIdentifier> functionRequirements) {
        this.functionRequirements = Objects.requireNonNull(functionRequirements);
        this.functionSignature = Objects.requireNonNull(functionSignature);
        this.primaryKeyValue = new AGeneratedUUID();
    }

    public FunctionRequirements(FunctionSignature functionSignature, Set<DependencyIdentifier> functionRequirements,
            AUUID primaryKeyValue) {
        this.functionRequirements = Objects.requireNonNull(functionRequirements);
        this.functionSignature = Objects.requireNonNull(functionSignature);
        this.primaryKeyValue = Objects.requireNonNull(primaryKeyValue);
    }

    public String getArityAsString() {
        return String.valueOf(functionSignature.getArity());
    }

    @Override
    public AUUID getPrimaryKeyValue() {
        return primaryKeyValue;
    }

    @Override
    public DataverseName getDataverseName() {
        return functionSignature.getDataverseName();
    }

    @Override
    public String getEntityName() {
        // Note: this entity name is not unique! Use this in conjunction with the arity.
        return functionSignature.getName();
    }

    @Override
    public String getDisplayName() {
        return functionSignature.toString(true);
    }

    @Override
    public DependentKind getDependentKind() {
        return DependentKind.FUNCTION;
    }

    @Override
    public Iterator<DependencyIdentifier> iterator() {
        return functionRequirements.iterator();
    }
}
