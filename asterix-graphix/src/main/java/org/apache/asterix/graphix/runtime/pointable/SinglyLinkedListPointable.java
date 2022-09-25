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
package org.apache.asterix.graphix.runtime.pointable;

import java.util.NoSuchElementException;
import java.util.function.Function;

import org.apache.hyracks.data.std.api.AbstractPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;

/**
 * A lean representation of a singly-linked list, where each item in the list only has a 4-byte length. We can only
 * access this SLL from start to finish, as we do not know the size of each item (nor the number of items) apriori.
 */
public class SinglyLinkedListPointable<T> extends AbstractPointable {
    public static final int LIST_ITEM_LENGTH_SIZE = 4;

    private final Function<VoidPointable, T> listItemConsumer;
    private final VoidPointable listItemPointable;

    // This is a **stateful** pointable, whose action is dictated by the given list-item callback.
    private int currentPosition;

    public SinglyLinkedListPointable(Function<VoidPointable, T> listItemConsumer) {
        this.listItemPointable = new VoidPointable();
        this.listItemConsumer = listItemConsumer;
    }

    @Override
    public void set(byte[] bytes, int start, int length) {
        super.set(bytes, start, length);
        currentPosition = 0;
    }

    public boolean hasNext() {
        return currentPosition < length;
    }

    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        // Determine the length of our working item.
        int itemLength = IntegerPointable.getInteger(bytes, start + currentPosition);
        currentPosition = currentPosition + LIST_ITEM_LENGTH_SIZE;

        // Consume our list item.
        listItemPointable.set(bytes, currentPosition, itemLength);
        T result = listItemConsumer.apply(listItemPointable);

        // Advance our cursor.
        currentPosition = currentPosition + itemLength;
        return result;
    }
}
