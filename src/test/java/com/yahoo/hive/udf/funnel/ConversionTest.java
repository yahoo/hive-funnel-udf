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

import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Assert;
import org.junit.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit test for Conversion.
 */
public class ConversionTest {
    @Test(expected = UDFArgumentLengthException.class)
    public void testTooManyInputs() throws HiveException {
        Conversion udf = new Conversion();

        ObjectInspector[] inputOiList = new ObjectInspector[]{
            ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaLongObjectInspector),
            ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaLongObjectInspector)
        };

        udf.initialize(inputOiList);
    }

    @Test(expected = UDFArgumentTypeException.class)
    public void testBadInputType() throws HiveException {
        Conversion udf = new Conversion();

        ObjectInspector[] inputOiList = new ObjectInspector[]{
            PrimitiveObjectInspectorFactory.javaLongObjectInspector
        };

        udf.initialize(inputOiList);
    }

    @Test
    public void testConvertToConversion() throws HiveException {
        Conversion udf = new Conversion();

        ObjectInspector[] inputOiList = new ObjectInspector[]{
                ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaLongObjectInspector)
        };

        udf.initialize(inputOiList);

        List<Long> inputList = Arrays.asList(10L, 5L, 1L);

        DeferredObject obj1 = mock(DeferredObject.class);
        DeferredObject[] objs = new DeferredObject[] { obj1 };
        when(obj1.get()).thenReturn(inputList);

        Assert.assertEquals(Arrays.asList(1.0, 0.5, 0.2), udf.evaluate(objs));
    }

    @Test
    public void testIncorrectNumberOfArgs() throws HiveException {
        Conversion udf = new Conversion();

        ObjectInspector[] inputOiList = new ObjectInspector[]{
                ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaLongObjectInspector)
        };

        udf.initialize(inputOiList);

        List<Long> inputList = Arrays.asList(10L);

        DeferredObject obj1 = mock(DeferredObject.class);
        DeferredObject[] objs = new DeferredObject[] { obj1, obj1 };

        when(obj1.get()).thenReturn(inputList);

        Assert.assertEquals(null, udf.evaluate(objs));
    }

    @Test
    public void testEmptyFunnel() throws HiveException {
        Conversion udf = new Conversion();

        ObjectInspector[] inputOiList = new ObjectInspector[]{
                ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaLongObjectInspector)
        };

        udf.initialize(inputOiList);

        List<Long> inputList = Arrays.asList();

        DeferredObject obj1 = mock(DeferredObject.class);
        DeferredObject[] objs = new DeferredObject[] { obj1 };
        when(obj1.get()).thenReturn(inputList);

        Assert.assertEquals(Arrays.asList(), udf.evaluate(objs));
    }

    @Test
    public void testConvertToConversionWithZeros() throws HiveException {
        Conversion udf = new Conversion();

        ObjectInspector[] inputOiList = new ObjectInspector[]{
                ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaLongObjectInspector)
        };

        udf.initialize(inputOiList);

        List<Long> inputList = Arrays.asList(10L, 5L, 0L, 0L, 0L);

        DeferredObject obj1 = mock(DeferredObject.class);
        DeferredObject[] objs = new DeferredObject[] { obj1 };
        when(obj1.get()).thenReturn(inputList);

        Assert.assertEquals(Arrays.asList(1.0, 0.5, 0.0, 0.0, 0.0), udf.evaluate(objs));
    }
}
