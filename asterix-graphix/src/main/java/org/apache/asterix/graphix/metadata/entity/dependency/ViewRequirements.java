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

import java.io.IOException;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.metadata.utils.DatasetUtil;
import org.apache.asterix.om.base.AGeneratedUUID;

/**
 * A collection of {@link org.apache.asterix.graphix.metadata.entity.schema.Graph} dependencies associated with a view
 * instance. This does <b>not</b> include non-graph dependencies for views.
 */
public class ViewRequirements implements IEntityRequirements {
    private static final long serialVersionUID = 1L;

    private final Set<DependencyIdentifier> viewRequirements;
    private final DataverseName dataverseName;
    private final String viewName;

    // Physically, our requirements are indexed by the string below. Logically, we ignore this.
    private final String primaryKeyValue;

    public ViewRequirements(DataverseName dataverseName, String viewName, Set<DependencyIdentifier> viewRequirements)
            throws IOException {
        this.viewRequirements = Objects.requireNonNull(viewRequirements);
        this.dataverseName = Objects.requireNonNull(dataverseName);
        this.viewName = Objects.requireNonNull(viewName);

        // Generate a unique primary key from a AUUID.
        StringBuilder sb = new StringBuilder();
        new AGeneratedUUID().appendLiteralOnly(sb);
        this.primaryKeyValue = sb.toString();
    }

    public ViewRequirements(DataverseName dataverseName, String viewName, Set<DependencyIdentifier> viewRequirements,
            String primaryKeyValue) {
        this.viewRequirements = Objects.requireNonNull(viewRequirements);
        this.dataverseName = Objects.requireNonNull(dataverseName);
        this.viewName = Objects.requireNonNull(viewName);
        this.primaryKeyValue = Objects.requireNonNull(primaryKeyValue);
    }

    @Override
    public String getPrimaryKeyValue() {
        return primaryKeyValue;
    }

    @Override
    public DataverseName getDataverseName() {
        return dataverseName;
    }

    @Override
    public String getEntityName() {
        return viewName;
    }

    @Override
    public String getDisplayName() {
        return DatasetUtil.getFullyQualifiedDisplayName(dataverseName, viewName);
    }

    @Override
    public DependentKind getDependentKind() {
        return DependentKind.VIEW;
    }

    @Override
    public Iterator<DependencyIdentifier> iterator() {
        return viewRequirements.iterator();
    }
}
