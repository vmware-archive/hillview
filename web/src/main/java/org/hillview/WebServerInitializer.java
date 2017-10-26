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

import org.hillview.utils.HillviewLogger;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

/**
 * This class is used by TomCat to initialize the web server before the
 * first client request is processed.
 */
public class WebServerInitializer implements ServletContextListener {
    @Override
    public void contextDestroyed(ServletContextEvent arg) {}

    // This is run before the web application is started.
    @Override
    public void contextInitialized(ServletContextEvent arg) {
        HillviewLogger.initialize("web server", "hillview-web.log");
        HillviewLogger.instance.info("Server started");
    }
}
