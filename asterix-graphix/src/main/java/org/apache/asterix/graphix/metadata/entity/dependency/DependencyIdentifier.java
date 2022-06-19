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

import java.io.Serializable;
import java.util.Objects;

import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.metadata.utils.DatasetUtil;
import org.apache.asterix.metadata.utils.MetadataUtil;

/**
 * An identifier for a dependency related to a {@link org.apache.asterix.graphix.metadata.entity.schema.Graph} instance.
 * A graph may depend on datasets, synonyms, functions, and other graphs. Similarly, functions and views may depend
 * on graphs themselves.
 */
public class DependencyIdentifier implements Serializable {
    private static final long serialVersionUID = 1L;

    private final DataverseName dataverseName;
    private final String entityName;
    private final String entityDetail;
    private final Kind dependencyKind;

    public DependencyIdentifier(DataverseName dataverseName, String entityName, Kind dependencyKind) {
        this.dataverseName = Objects.requireNonNull(dataverseName);
        this.entityName = Objects.requireNonNull(entityName);
        this.entityDetail = null;
        this.dependencyKind = Objects.requireNonNull(dependencyKind);
    }

    public DependencyIdentifier(DataverseName dataverseName, String entityName, String entityDetail,
            Kind dependencyKind) {
        this.dataverseName = Objects.requireNonNull(dataverseName);
        this.entityName = Objects.requireNonNull(entityName);
        this.entityDetail = Objects.requireNonNull(entityDetail);
        this.dependencyKind = Objects.requireNonNull(dependencyKind);
    }

    public DataverseName getDataverseName() {
        return dataverseName;
    }

    public String getEntityName() {
        return entityName;
    }

    public String getEntityDetail() {
        return entityDetail;
    }

    public Kind getDependencyKind() {
        return dependencyKind;
    }

    public String getDisplayName() {
        switch (dependencyKind) {
            case DATASET:
                return DatasetUtil.getFullyQualifiedDisplayName(dataverseName, entityName);

            case SYNONYM:
            case GRAPH:
                return MetadataUtil.getFullyQualifiedDisplayName(dataverseName, entityName);

            case FUNCTION:
                return new FunctionSignature(dataverseName, entityName,
                        Integer.parseInt(Objects.requireNonNull(entityDetail))).toString();
        }
        return null;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof DependencyIdentifier)) {
            return false;
        }
        DependencyIdentifier that = (DependencyIdentifier) object;
        return that.getDataverseName().equals(this.getDataverseName())
                && that.getDependencyKind().equals(this.getDependencyKind())
                && that.getEntityName().equals(this.getEntityName())
                && Objects.equals(that.getEntityDetail(), this.getEntityDetail());
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataverseName, dependencyKind, entityName, entityDetail);
    }

    public enum Kind {
        GRAPH,
        DATASET,
        SYNONYM,
        FUNCTION
    }
}
