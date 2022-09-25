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
package org.apache.asterix.graphix.lang.struct;

import java.io.Serializable;
import java.util.Objects;

/**
 * Label for a vertex, uniquely identified by the label name.
 */
public class ElementLabel implements Serializable {
    private static final long serialVersionUID = 2L;

    private final String labelName;
    private final boolean isNegated;

    public ElementLabel(String labelName, boolean isNegated) {
        this.labelName = Objects.requireNonNull(labelName);
        this.isNegated = isNegated;
    }

    public boolean isNegated() {
        return isNegated;
    }

    public String getLabelName() {
        return labelName;
    }

    @Override
    public String toString() {
        return (isNegated ? "NOT " : "") + labelName;
    }

    @Override
    public int hashCode() {
        return Objects.hash(labelName, isNegated);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof ElementLabel) {
            ElementLabel that = (ElementLabel) o;
            return this.labelName.equals(that.labelName) && this.isNegated == that.isNegated;
        }
        return false;
    }
}
