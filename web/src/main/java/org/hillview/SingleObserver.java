/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hillview;

import rx.Observer;

/**
 * An observer which expects a single item.
 */
public abstract class SingleObserver<T> implements Observer<T> {
    private boolean received = false;

    public void onError(Throwable t) {
        throw new RuntimeException(t);
    }

    public abstract void onSuccess(T t);

    @Override
    public void onCompleted() {
        if (this.received) return;
        this.onError(new RuntimeException("No item received"));
    }

    @Override
    public void onNext(T t) {
        if (this.received) {
            this.onError(new RuntimeException("Multiple items received"));
            return;
        }
        this.onSuccess(t);
        this.received = true;
    }
}
