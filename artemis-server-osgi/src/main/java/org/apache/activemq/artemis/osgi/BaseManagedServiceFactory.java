/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.osgi;

import java.util.Dictionary;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.osgi.framework.BundleContext;
import org.osgi.service.cm.ConfigurationException;
import org.osgi.service.cm.ManagedServiceFactory;

public abstract class BaseManagedServiceFactory<T, R> implements ManagedServiceFactory {

    public static final long DEFAULT_TIMEOUT_BEFORE_INTERRUPT = 30000;

    private final BundleContext context;
    private final String name;
    private final long timeoutBeforeInterrupt;
    private final AtomicBoolean destroyed;
    private final ExecutorService executor;
    private final Map<String, Pair<T, R>> services;

    public BaseManagedServiceFactory(BundleContext context, String name) {
        this(context, name, DEFAULT_TIMEOUT_BEFORE_INTERRUPT);
    }

    public BaseManagedServiceFactory(BundleContext context, String name, long timeoutBeforeInterrupt) {
        this.context = context;
        this.name = name;
        this.timeoutBeforeInterrupt = timeoutBeforeInterrupt;
        this.destroyed = new AtomicBoolean(false);
        this.executor = Executors.newSingleThreadExecutor();
        this.services = new ConcurrentHashMap<>();
    }

    public String getName() {
        return name;
    }

    public BundleContext getContext() {
        return context;
    }

    public void updated(final String pid, final Dictionary<String, ?> properties) throws ConfigurationException {
        if (destroyed.get()) {
            return;
        }
        executor.submit(new Runnable() {
            public void run() {
                try {
                    internalUpdate(pid, properties);
                } catch (Throwable t) {
                    warn("Error destroying service for ManagedServiceFactory " + getName(), t);
                }
            }
        });
    }

    public void deleted(final String pid) {
        if (destroyed.get()) {
            return;
        }
        executor.submit(new Runnable() {
            public void run() {
                try {
                    internalDelete(pid);
                } catch (Throwable throwable) {
                    warn("Error destroying service for ManagedServiceFactory " + getName(), throwable);
                }
            }
        });
    }

    private void internalUpdate(String pid, Dictionary<String, ?> properties) {
        Pair<T, R> pair = services.remove(pid);
        if (pair != null) {
            internalDelete(pid);
        }
        if (destroyed.get()) {
            return;
        }
        try {
            T t = doCreate(properties);
            try {
                if (destroyed.get()) {
                    throw new IllegalStateException("ManagedServiceFactory has been destroyed");
                }
                R registration = register(t, properties);
                services.put(pid, new Pair<>(t, registration));
            } catch (Throwable throwable1) {
                try {
                    doDestroy(t);
                } catch (Throwable throwable2) {
                    // Ignore
                }
                throw throwable1;
            }
        } catch (Throwable throwable) {
            warn("Error creating service for ManagedServiceFactory " + getName(), throwable);
        }
    }

    private void internalDelete(String pid) {
        Pair<T, R> pair = services.remove(pid);
        if (pair != null) {
            try {
                unregister(pair.getSecond());
            } catch (Throwable t) {
                info("Error unregistering service", t);
            }
            try {
                doDestroy(pair.getFirst());
            } catch (Throwable t) {
                info("Error destroying service", t);
            }
        }
    }

    protected abstract T doCreate(Dictionary<String, ?> properties) throws Exception;

    protected abstract void doDestroy(T t) throws Exception;

    protected abstract R register(T t, Dictionary<String, ?> properties);

    protected abstract void unregister(R registration);

    protected abstract void warn(String message, Throwable t);

    protected abstract void info(String message, Throwable t);

    public void destroy() {
        if (destroyed.compareAndSet(false, true)) {
            executor.shutdown();
            try {
                executor.awaitTermination(timeoutBeforeInterrupt, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException("Shutdown interrupted");
            }
            if (!executor.isTerminated()) {
                executor.shutdownNow();
                try {
                    executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
                } catch (InterruptedException e) {
                    throw new RuntimeException("Shutdown interrupted");
                }
            }

            while (!services.isEmpty()) {
                String pid = services.keySet().iterator().next();
                internalDelete(pid);
            }
        }
    }

    static class Pair<U,V> {
        private U first;
        private V second;
        public Pair(U first, V second) {
            this.first = first;
            this.second = second;
        }
        public U getFirst() {
            return first;
        }
        public V getSecond() {
            return second;
        }
        public void setFirst(U first) {
            this.first = first;
        }
        public void setSecond(V second) {
            this.second = second;
        }
    }

}
