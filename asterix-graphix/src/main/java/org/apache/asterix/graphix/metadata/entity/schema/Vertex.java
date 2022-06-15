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
package org.apache.asterix.graphix.metadata.entity.schema;

import java.util.List;
import java.util.Objects;

import org.apache.asterix.graphix.common.metadata.GraphElementIdentifier;
import org.apache.asterix.graphix.lang.struct.ElementLabel;

/**
 * Metadata representation of a vertex. A vertex consists of the following:
 * 1. A {@link GraphElementIdentifier}, to uniquely identify the vertex across other graph elements.
 * 2. A list of primary key fields, associated with the definition body.
 * 3. A SQL++ string denoting the definition body.
 */
public class Vertex implements IElement {
    private static final long serialVersionUID = 1L;

    private final GraphElementIdentifier identifier;
    private final List<List<String>> primaryKeyFieldNames;
    private final String definitionBody;

    /**
     * Use {@link Schema.Builder} to build Vertex instances instead of this constructor.
     */
    Vertex(GraphElementIdentifier identifier, List<List<String>> primaryKeyFieldNames, String definitionBody) {
        this.identifier = Objects.requireNonNull(identifier);
        this.primaryKeyFieldNames = primaryKeyFieldNames;
        this.definitionBody = Objects.requireNonNull(definitionBody);
    }

    public List<List<String>> getPrimaryKeyFieldNames() {
        return primaryKeyFieldNames;
    }

    @Override
    public GraphElementIdentifier getIdentifier() {
        return identifier;
    }

    @Override
    public ElementLabel getLabel() {
        return identifier.getElementLabel();
    }

    @Override
    public String getDefinitionBody() {
        return definitionBody;
    }

    @Override
    public String toString() {
        return "(:" + getLabel() + ") AS " + definitionBody;
    }
}
