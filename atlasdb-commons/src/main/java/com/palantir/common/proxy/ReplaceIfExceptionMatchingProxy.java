/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.common.proxy;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.reflect.AbstractInvocationHandler;

public final class ReplaceIfExceptionMatchingProxy<T> extends AbstractInvocationHandler {
    private static final Logger log = LoggerFactory.getLogger(ReplaceIfExceptionMatchingProxy.class);

    private final Supplier<T> delegateFactory;
    private final Predicate<Throwable> shouldReplace;
    private volatile T delegate;

    private ReplaceIfExceptionMatchingProxy(Supplier<T> delegateFactory, Predicate<Throwable> shouldReplace) {
        this.delegateFactory = delegateFactory;
        this.delegate = delegateFactory.get();
        this.shouldReplace = shouldReplace;
    }

    @Override
    protected Object handleInvocation(Object proxy, Method method, Object[] args) throws Throwable {
        try {
            return method.invoke(delegate, args);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            replaceIfNecessary(cause);
            throw cause;
        }
    }

    private void replaceIfNecessary(Throwable thrown) {
        if (shouldReplace.test(thrown)) {
            synchronized (this) {
                T replacement = delegateFactory.get();
                if (delegate != replacement) {
                    log.info("Replacing underlying proxy due to thrown exception", thrown);
                    delegate = delegateFactory.get();
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T newProxyInstance(
            Class<T> interfaceClass,
            Supplier<T> delegate,
            Predicate<Throwable> shouldReplace) {
        return (T) Proxy.newProxyInstance(interfaceClass.getClassLoader(),
                new Class<?>[] {interfaceClass}, new ReplaceIfExceptionMatchingProxy<>(delegate, shouldReplace));
    }
}
