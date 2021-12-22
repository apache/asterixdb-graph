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
package org.apache.asterix.graphix.common.metadata;

import java.io.Serializable;
import java.util.Objects;

public class GraphElementIdentifier implements Serializable {
    private static final long serialVersionUID = 1L;
    private final GraphIdentifier graphIdentifier;
    private final Kind elementKind;
    private final String labelName;

    public GraphElementIdentifier(GraphIdentifier graphIdentifier, Kind elementKind, String labelName) {
        this.graphIdentifier = graphIdentifier;
        this.elementKind = elementKind;
        this.labelName = labelName;
    }

    public GraphIdentifier getGraphIdentifier() {
        return graphIdentifier;
    }

    public Kind getElementKind() {
        return elementKind;
    }

    public String getLabelName() {
        return labelName;
    }

    @Override
    public String toString() {
        return graphIdentifier + "#" + labelName + " ( " + elementKind + " )";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof GraphElementIdentifier) {
            GraphElementIdentifier that = (GraphElementIdentifier) o;
            return graphIdentifier.equals(that.graphIdentifier) && elementKind.equals(that.elementKind)
                    && labelName.equals(that.labelName);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(graphIdentifier, elementKind, labelName);
    }

    public enum Kind {
        VERTEX,
        EDGE;

        @Override
        public String toString() {
            switch (this) {
                case EDGE:
                    return "edge";
                case VERTEX:
                    return "vertex";
                default:
                    throw new IllegalStateException("Unknown graph element kind.");
            }
        }
    }
}
