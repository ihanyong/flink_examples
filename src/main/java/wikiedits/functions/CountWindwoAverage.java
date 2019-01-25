package wikiedits.functions;

import org.apache.flink.api.common.functions.RichAggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;

import java.math.BigDecimal;

/**
 * CountWindwoAverage
 *
 * @author yong.han
 * 2019/1/20
 */
public class CountWindwoAverage extends RichAggregateFunction<BigDecimal, BigDecimal, BigDecimal> {
    @Override
    public BigDecimal createAccumulator() {
        return null;
    }

    @Override
    public BigDecimal add(BigDecimal value, BigDecimal accumulator) {
        return null;
    }

    @Override
    public BigDecimal getResult(BigDecimal accumulator) {
        return null;
    }

    @Override
    public BigDecimal merge(BigDecimal a, BigDecimal b) {
        return null;
    }
}
