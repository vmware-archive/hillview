/*
 * Copyright (c) 2020 VMware Inc. All Rights Reserved.
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

import java.io.*;
import java.nio.charset.StandardCharsets;
import javax.servlet.*;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.*;
import org.apache.commons.io.FileUtils;

@WebServlet("/bookmark")
public class BookmarkServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;
    public static final String bookmarkDirectory = "bookmark";
    public static final String bookmarkExtension = ".txt";
    public static String bookmarkID;
    public static boolean openingBookmark;

    public void init() throws ServletException {
        bookmarkID = "";
        openingBookmark = false;
    }

    public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        // TODO: match the bookmarkID with the client session to enable opening bookmark at the same time
        request.getSession();

        BookmarkServlet.openingBookmark = true;
        BookmarkServlet.bookmarkID = request.getParameter("id");
        response.sendRedirect("/");
    }

    public static String openingBookmark() {
        if (openingBookmark) {
            openingBookmark = false;
            try {
                File file = new File(BookmarkServlet.bookmarkDirectory,
                        BookmarkServlet.bookmarkID + BookmarkServlet.bookmarkExtension);
                return FileUtils.readFileToString(file, StandardCharsets.UTF_8);
            } catch (Exception e) {
                throw new RuntimeException("Cannot read bookmark", e);
            }
        }
        return "";
    }
}