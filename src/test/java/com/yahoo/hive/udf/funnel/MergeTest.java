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

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.udf.generic.SimpleGenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Assert;
import org.junit.Test;

public class MergeTest {
    @Test(expected = UDFArgumentLengthException.class)
    public void testInvalidNumberOfParams() throws HiveException {
        Merge udaf = new Merge();
        ObjectInspector[] inputObjectInspectorList = new ObjectInspector[]{
                ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaLongObjectInspector),
                ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaLongObjectInspector)
        };

        GenericUDAFParameterInfo paramInfo = new SimpleGenericUDAFParameterInfo(inputObjectInspectorList, false, false);
        GenericUDAFEvaluator udafEvaluator = udaf.getEvaluator(paramInfo);
    }

    @Test(expected = UDFArgumentTypeException.class)
    public void testPrimitiveParam() throws HiveException {
        Merge udaf = new Merge();
        ObjectInspector[] inputObjectInspectorList = new ObjectInspector[]{
            PrimitiveObjectInspectorFactory.javaStringObjectInspector
        };

        GenericUDAFParameterInfo paramInfo = new SimpleGenericUDAFParameterInfo(inputObjectInspectorList, false, false);
        GenericUDAFEvaluator udafEvaluator = udaf.getEvaluator(paramInfo);
    }

    @Test(expected = UDFArgumentTypeException.class)
    public void testArrayOfNonPrimitives() throws HiveException {
        Merge udaf = new Merge();
        ObjectInspector[] inputObjectInspectorList = new ObjectInspector[]{
                ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaLongObjectInspector))
        };

        GenericUDAFParameterInfo paramInfo = new SimpleGenericUDAFParameterInfo(inputObjectInspectorList, false, false);
        GenericUDAFEvaluator udafEvaluator = udaf.getEvaluator(paramInfo);
    }

    @Test(expected = UDFArgumentTypeException.class)
    public void testArrayOfNonNumbers() throws HiveException {
        Merge udaf = new Merge();
        ObjectInspector[] inputObjectInspectorList = new ObjectInspector[]{
                ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
        };

        GenericUDAFParameterInfo paramInfo = new SimpleGenericUDAFParameterInfo(inputObjectInspectorList, false, false);
        GenericUDAFEvaluator udafEvaluator = udaf.getEvaluator(paramInfo);
    }

    @Test
    public void testComplete() throws HiveException {
        Merge udaf = new Merge();
        ObjectInspector[] inputObjectInspectorList = new ObjectInspector[]{
                ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaLongObjectInspector)
        };

        GenericUDAFParameterInfo paramInfo = new SimpleGenericUDAFParameterInfo(inputObjectInspectorList, false, false);
        GenericUDAFEvaluator udafEvaluator = udaf.getEvaluator(paramInfo);

        ObjectInspector outputObjectInspector = udafEvaluator.init(Mode.COMPLETE, inputObjectInspectorList);

        List<Long> funnel1 = new ArrayList<>();
        funnel1.add(1L);
        funnel1.add(1L);
        funnel1.add(0L);

        List<Long> funnel2 = new ArrayList<>();
        funnel2.add(1L);
        funnel2.add(0L);
        funnel2.add(0L);

        Object[] parameters1 = new Object[]{null};
        Object[] parameters2 = new Object[]{funnel1};
        Object[] parameters3 = new Object[]{funnel2};

        AggregationBuffer agg = udafEvaluator.getNewAggregationBuffer();
        udafEvaluator.reset(agg);
        udafEvaluator.iterate(agg, parameters1);
        udafEvaluator.iterate(agg, parameters2);
        udafEvaluator.iterate(agg, parameters3);
        Object result = udafEvaluator.terminate(agg);

        // Expected
        List<Long> expected = new ArrayList<>();
        expected.add(2L);
        expected.add(1L);
        expected.add(0L);

        Assert.assertEquals(expected, result);
    }

    @Test
    public void testPartial2() throws HiveException {
        Merge udaf = new Merge();
        ObjectInspector[] inputObjectInspectorList = new ObjectInspector[]{
                ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaLongObjectInspector)
        };

        GenericUDAFParameterInfo paramInfo = new SimpleGenericUDAFParameterInfo(inputObjectInspectorList, false, false);
        GenericUDAFEvaluator udafEvaluator = udaf.getEvaluator(paramInfo);

        ObjectInspector outputObjectInspector = udafEvaluator.init(Mode.PARTIAL2, inputObjectInspectorList);

        // Setup the two partial results
        List<Long> partialResults1 = new ArrayList<>();
        partialResults1.add(1L);
        partialResults1.add(1L);
        partialResults1.add(0L);

        List<Long> partialResults2 = new ArrayList<>();
        partialResults2.add(1L);
        partialResults2.add(1L);
        partialResults2.add(1L);

        // Merge the partial results
        MergeAggregateBuffer agg = (MergeAggregateBuffer) udafEvaluator.getNewAggregationBuffer();
        udafEvaluator.reset(agg);
        udafEvaluator.merge(agg, partialResults1);
        udafEvaluator.merge(agg, partialResults2);
        Object result = udafEvaluator.terminatePartial(agg);

        // Expected results
        List<Long> expected = new ArrayList<>();
        expected.add(2L);
        expected.add(2L);
        expected.add(1L);

        Assert.assertEquals(expected, result);
    }

    @Test(expected = UDFArgumentTypeException.class)
    public void testCompleteFunnelSizeMismatch() throws HiveException {
        Merge udaf = new Merge();
        ObjectInspector[] inputObjectInspectorList = new ObjectInspector[]{
                ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaLongObjectInspector)
        };

        GenericUDAFParameterInfo paramInfo = new SimpleGenericUDAFParameterInfo(inputObjectInspectorList, false, false);
        GenericUDAFEvaluator udafEvaluator = udaf.getEvaluator(paramInfo);

        ObjectInspector outputObjectInspector = udafEvaluator.init(Mode.COMPLETE, inputObjectInspectorList);

        // Setup two funnels, different sizes.
        List<Long> funnel1 = new ArrayList<>();
        funnel1.add(1L);
        funnel1.add(1L);
        funnel1.add(0L);

        List<Long> funnel2 = new ArrayList<>();
        funnel2.add(1L);
        funnel2.add(0L);

        Object[] parameters1 = new Object[]{funnel1};
        Object[] parameters2 = new Object[]{funnel2};

        // Should cause an error when merging funnels of different sizes
        AggregationBuffer agg = udafEvaluator.getNewAggregationBuffer();
        udafEvaluator.reset(agg);
        udafEvaluator.iterate(agg, parameters1);
        udafEvaluator.iterate(agg, parameters2);
    }

    @Test(expected = UDFArgumentTypeException.class)
    public void testPartial2FunnelSizeMismatch() throws HiveException {
        Merge udaf = new Merge();
        ObjectInspector[] inputObjectInspectorList = new ObjectInspector[]{
                ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaLongObjectInspector)
        };

        GenericUDAFParameterInfo paramInfo = new SimpleGenericUDAFParameterInfo(inputObjectInspectorList, false, false);
        GenericUDAFEvaluator udafEvaluator = udaf.getEvaluator(paramInfo);

        ObjectInspector outputObjectInspector = udafEvaluator.init(Mode.PARTIAL2, inputObjectInspectorList);

        // Setup the two partial results, should fail when merging list of different sizes
        List<Long> partialResults1 = new ArrayList<>();
        partialResults1.add(1L);
        partialResults1.add(1L);

        List<Long> partialResults2 = new ArrayList<>();
        partialResults2.add(1L);
        partialResults2.add(0L);
        partialResults2.add(0L);

        // Merge the partial results, should throw error due to list size difference
        MergeAggregateBuffer agg = (MergeAggregateBuffer) udafEvaluator.getNewAggregationBuffer();
        udafEvaluator.reset(agg);
        udafEvaluator.merge(agg, partialResults1);
        udafEvaluator.merge(agg, partialResults2);
    }
}
