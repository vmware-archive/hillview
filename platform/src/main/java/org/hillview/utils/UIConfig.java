/*
 * Copyright (c) 2020 VMware Inc. All Rights Reserved.
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

package org.hillview.utils;

import org.hillview.dataset.api.IJsonSketchResult;

/**
 * Parameters controlling the UI display of Hillview.
 * These are read from hillview.properties with the same names.
 * Same as the TypeScript class with the same name.
 */
public class UIConfig implements IJsonSketchResult {
    static final long serialVersionUID = 1;

    public UIConfig() {}

    public boolean enableSaveAs;
    public boolean localDbMenu;
    public boolean showTestMenu;
    public boolean enableManagement;
    public boolean privateIsCsv;
    public boolean hideSuggestions;
    public boolean hideDemoMenu;
}
