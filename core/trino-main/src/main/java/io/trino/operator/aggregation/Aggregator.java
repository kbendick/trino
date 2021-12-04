/*
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
package io.trino.operator.aggregation;

import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.AggregationNode;

import static com.google.common.base.Preconditions.checkArgument;

public class Aggregator
{
    private final Type intermediateType;
    private final Type finalType;
    private final Accumulator aggregation;
    private final AggregationNode.Step step;
    private final int intermediateChannel;

    public Aggregator(AccumulatorFactory accumulatorFactory, AggregationNode.Step step)
    {
        intermediateType = accumulatorFactory.getIntermediateType();
        finalType = accumulatorFactory.getFinalType();

        if (step.isInputRaw()) {
            intermediateChannel = -1;
            aggregation = accumulatorFactory.createAccumulator();
        }
        else {
            checkArgument(accumulatorFactory.getInputChannels().size() == 1, "expected 1 input channel for intermediate aggregation");
            intermediateChannel = accumulatorFactory.getInputChannels().get(0);
            aggregation = accumulatorFactory.createIntermediateAccumulator();
        }
        this.step = step;
    }

    public Type getType()
    {
        if (step.isOutputPartial()) {
            return intermediateType;
        }
        else {
            return finalType;
        }
    }

    public void processPage(Page page)
    {
        if (step.isInputRaw()) {
            aggregation.addInput(page);
        }
        else {
            aggregation.addIntermediate(page.getBlock(intermediateChannel));
        }
    }

    public void evaluate(BlockBuilder blockBuilder)
    {
        if (step.isOutputPartial()) {
            aggregation.evaluateIntermediate(blockBuilder);
        }
        else {
            aggregation.evaluateFinal(blockBuilder);
        }
    }

    public long getEstimatedSize()
    {
        return aggregation.getEstimatedSize();
    }
}