package org.corfudb.common.util;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TailCallsTest {

    static long sumOfNumbers(long acc, int num) {
        if (num == 0) {
            return acc;
        } else {
            return sumOfNumbers(acc + num, num - 1);
        }
    }

    static TailCall<Long> sumOfNumbersTailRecursive(long acc, int num) {
        if (num == 0) {
            return TailCalls.done(acc);
        } else {
            return TailCalls.call(() -> sumOfNumbersTailRecursive(acc + num, num - 1));
        }

    }

    @Test
    public void testTailRecursion() {
        long initAcc = 0L;
        int endOfSeries = 100000;

        assertThatThrownBy(() -> sumOfNumbers(initAcc, endOfSeries))
                .isInstanceOf(StackOverflowError.class);

        long result = sumOfNumbersTailRecursive(initAcc, endOfSeries)
                .invoke()
                .orElseThrow(() -> new IllegalStateException("Failure"));
        assertThat(result).isEqualTo(5000050000L);
    }
}