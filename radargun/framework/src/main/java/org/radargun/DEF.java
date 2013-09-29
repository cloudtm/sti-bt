package org.radargun;

import java.util.concurrent.Future;

public interface DEF {

    <T, K> Future<T> submit(DEFTask<T> task, K... input);
    
}
