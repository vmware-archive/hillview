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

/**
 * Interface implemented by objects that handle drag events.
 */
export interface IDragHandler {
    dragStart(): void,
    dragMove(): void,
    dragEnd(): void
}

/**
 * Interface implemented by objects that handle mouse move events.
 */
export interface IMouseHandler {
    mouseMove(): void,
    mouseEnter(): void,
    mouseLeave(): void
}