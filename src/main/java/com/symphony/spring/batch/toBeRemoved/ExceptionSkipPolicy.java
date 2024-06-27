package com.symphony.spring.batch.toBeRemoved;

import org.springframework.batch.core.step.skip.SkipLimitExceededException;
import org.springframework.batch.core.step.skip.SkipPolicy;

//@Slf4j
public class ExceptionSkipPolicy implements SkipPolicy {

    @Override
    public boolean shouldSkip(Throwable throwable, int i) throws SkipLimitExceededException {
        return throwable instanceof NumberFormatException ;
    }
}
