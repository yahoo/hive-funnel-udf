/*
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.yahoo.hive.udf.funnel;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryArray;

public class ListUtils {
    /**
     * Returns true if list if not empty.
     *
     * @param list
     * @return True if list not empty
     */
    public static boolean isNotEmpty(List<Object> list) {
        return !list.isEmpty();
    }

    /**
     * Removes null values from list.
     *
     * @param list
     * @return List without null values
     */
    public static List<Object> removeNullFromList(List<Object> list) {
        return list.stream()
                   .filter(Objects::nonNull)
                   .collect(Collectors.toList());
    }

    /**
     * Cast an object to a list. Checks if it is a LazyBinaryArray or a
     * regular list.
     *
     * @param object Input object to try and cast to a list.
     * @return List of objects.
     */
    public static List<Object> toList(Object object) {
        List<Object> result;
        if (object instanceof LazyBinaryArray) {
            result = ((LazyBinaryArray) object).getList();
        } else {
            result = (List<Object>) object;
        }
        return result;
    }
}
