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

package com.facebook.presto.hll.state;

import com.facebook.presto.operator.aggregation.state.AbstractGroupedAccumulatorState;
import com.facebook.presto.operator.aggregation.state.AccumulatorStateFactory;
import com.facebook.presto.util.array.ObjectBigArray;
import com.facebook.presto.hll.impl.HLL;
import io.airlift.slice.SizeOf;

import static com.google.common.base.Preconditions.checkNotNull;

public class HLLStateFactory
        implements AccumulatorStateFactory<HLLState>
{
    @Override
    public HLLState createSingleState()
    {
        return new SingleHLLState();
    }

    @Override
    public Class<? extends HLLState> getSingleStateClass()
    {
        return SingleHLLState.class;
    }

    @Override
    public HLLState createGroupedState()
    {
        return new GroupedHLLState();
    }

    @Override
    public Class<? extends HLLState> getGroupedStateClass()
    {
        return GroupedHLLState.class;
    }

    public static class GroupedHLLState
            extends AbstractGroupedAccumulatorState
            implements HLLState
    {
        private final ObjectBigArray<HLL> hlls = new ObjectBigArray<>();
        private long size;

        @Override
        public void ensureCapacity(long size)
        {
            hlls.ensureCapacity(size);
        }

        @Override
        public HLL getHLL()
        {
            return hlls.get(getGroupId());
        }

        @Override
        public void setHLL(HLL value)
        {
            checkNotNull(value, "value is null");
            HLL current = hlls.get(getGroupId());
            if (current == null) {
                hlls.set(getGroupId(), value);
            }
            else {
                hlls.set(getGroupId(), (HLL) current.merge(value));
            }
        }

        @Override
        public void addMemoryUsage(int value)
        {
            size += value;
        }

        @Override
        public long getEstimatedSize()
        {
            return size + hlls.sizeOf();
        }
    }

    public static class SingleHLLState
            implements HLLState
    {
        private HLL hll;

        @Override
        public HLL getHLL()
        {
            return hll;
        }

        @Override
        public void setHLL(HLL value)
        {
            hll = value;
        }

        @Override
        public void addMemoryUsage(int value)
        {
            // noop
        }

        @Override
        public long getEstimatedSize()
        {
            if (hll == null) {
                return 0;
            }
            return hll.sizeof() * SizeOf.SIZE_OF_BYTE;
        }
    }
}
