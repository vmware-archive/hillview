/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
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

package org.hillview.dataset.remoting;

import javax.annotation.Nullable;
import java.io.Serializable;

/**
 * Class used to wrap responses of the IDataSet methods.
 * @param <T> Return type of the result
 */
public class OperationResponse<T> implements Serializable {
    static final long serialVersionUID = 1;

    @Nullable public final T result;

    public OperationResponse(@Nullable final T result) {
        this.result = result;
    }
}
