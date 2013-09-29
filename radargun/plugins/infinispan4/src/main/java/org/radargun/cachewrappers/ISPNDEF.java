package org.radargun.cachewrappers;

import java.util.concurrent.Future;

import org.infinispan.Cache;
import org.infinispan.distexec.DefaultExecutorService;
import org.radargun.DEF;
import org.radargun.DEFTask;

public class ISPNDEF extends DefaultExecutorService implements DEF {

    public ISPNDEF(Cache<?, ?> masterCacheNode) {
	super(masterCacheNode);
    }

    @Override
    public <T, K> Future<T> submit(DEFTask<T> task, K... input) {
	return super.submit((ISPNDEFTask) task, input);
    }

}
