/*
 * Copyright (c) 2018 VMware Inc. All Rights Reserved.
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

import io.krakens.grok.api.Grok;
import io.krakens.grok.api.GrokUtils;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;
import java.util.*;
import java.util.regex.Matcher;

import static java.lang.String.format;

/**
 * Useful functions to complement java-grok.
 */
public class GrokExtra {
    /**
     * Given a pattern and a group name within the pattern, return the pattern
     * that corresponds just to this group.  For example, given
     * TIMESTAMP %{DATE:Date} %{TIME}
     * for the sourcePattern TIMESTAMP and the groupName Date this returns
     * the pattern defined by DATE.
     * @param patternDefinitions  All patterns definitions known.
     * @param sourcePattern  A grok pattern containing sub-patterns.
     * @param groupName      The name of a subgroup within the pattern.
     * @return               The corresponding regular expression, or null if not found.
     */
    @Nullable
    public static String extractGroupPattern(
            Map<String, String> patternDefinitions,
            String sourcePattern, String groupName) {
        // This code is pilfered from the GrokCompiler class.
        if (StringUtils.isBlank(sourcePattern))
            return null;

        String currentPattern = sourcePattern;
        int index = 0;
        /** flag for infinite recursion. */
        int iterationLeft = 1000;
        boolean continueIteration = true;

        // Replace %{foo} with the regex (mostly group name regex)
        // and then compile the regex
        while (continueIteration) {
            continueIteration = false;
            if (iterationLeft <= 0) {
                throw new IllegalArgumentException("Deep recursion during analysis of " + sourcePattern);
            }
            iterationLeft--;

            Set<String> namedGroups = io.krakens.grok.api.GrokUtils.getNameGroups(io.krakens.grok.api.GrokUtils.GROK_PATTERN.pattern());
            Matcher matcher = io.krakens.grok.api.GrokUtils.GROK_PATTERN.matcher(currentPattern);
            // Match %{Foo:bar} -> pattern name and subname
            // Match %{Foo=regex} -> add new regex definition
            if (matcher.find()) {
                continueIteration = true;
                Map<String, String> group = io.krakens.grok.api.GrokUtils.namedGroups(matcher, namedGroups);
                if (group.get("definition") != null) {
                    patternDefinitions.put(group.get("pattern"), group.get("definition"));
                    group.put("name", group.get("name") + "=" + group.get("definition"));
                }
                int count = StringUtils.countMatches(currentPattern, "%{" + group.get("name") + "}");
                for (int i = 0; i < count; i++) {
                    String definitionOfPattern = patternDefinitions.get(group.get("pattern"));
                    if (definitionOfPattern == null) {
                        throw new IllegalArgumentException(format("No definition for key '%s' found, aborting",
                                group.get("pattern")));
                    }
                    String thisGroupName = group.get("subname") != null ? group.get("subname") : group.get("name");
                    if (thisGroupName.equals(groupName)) {
                        String retval = group.get("name");
                        String subname = group.get("subname");
                        if (subname != null)
                            retval = retval.replace(":" + subname, "");
                        return retval;
                    } else {
                        String replacement = String.format("(?<name%d>%s)", index, definitionOfPattern);
                        currentPattern = StringUtils.replace(currentPattern, "%{" + group.get("name") + "}", replacement, 1);
                    }
                    index++;
                }
            }
        }
        return null;
    }

    /**
     * Returns the columns to extract in the order they appear in the grok pattern.
     */
    public static List<String> getColumnsFromPattern(Grok grok) {
        Set<String> columnsFound = new HashSet<String>();

        String currentPattern = grok.getOriginalGrokPattern();
        int index = 0;
        /** flag for infinite recursion. */
        int iterationLeft = 1000;
        boolean continueIteration = true;
        Map<String, String> patternDefinitions = grok.getPatterns();

        // Replace %{foo} with the regex (mostly group name regex)
        // and then compile the regex
        while (continueIteration) {
            continueIteration = false;
            if (iterationLeft <= 0) {
                throw new IllegalArgumentException(
                        "Deep recursion during analysis of " + grok.getOriginalGrokPattern());
            }
            iterationLeft--;

            Set<String> namedGroups = io.krakens.grok.api.GrokUtils.getNameGroups(io.krakens.grok.api.GrokUtils.GROK_PATTERN.pattern());
            Matcher matcher = io.krakens.grok.api.GrokUtils.GROK_PATTERN.matcher(currentPattern);
            // Match %{Foo:bar} -> pattern name and subname
            // Match %{Foo=regex} -> add new regex definition
            if (matcher.find()) {
                continueIteration = true;
                Map<String, String> group = io.krakens.grok.api.GrokUtils.namedGroups(matcher, namedGroups);
                if (group.get("definition") != null) {
                    patternDefinitions.put(group.get("pattern"), group.get("definition"));
                    group.put("name", group.get("name") + "=" + group.get("definition"));
                }
                int count = StringUtils.countMatches(currentPattern, "%{" + group.get("name") + "}");
                for (int i = 0; i < count; i++) {
                    String definitionOfPattern = patternDefinitions.get(group.get("pattern"));
                    if (definitionOfPattern == null) {
                        throw new IllegalArgumentException(format("No definition for key '%s' found, aborting",
                                group.get("pattern")));
                    }

                    String replacement = "";
                    String subname = group.get("subname");
                    if (subname != null) {
                        columnsFound.add(subname);
                    } else {
                        replacement = String.format("(?<name%d>%s)", index, definitionOfPattern);
                    }
                    currentPattern = StringUtils.replace(currentPattern, "%{" + group.get("name") + "}", replacement, 1);
                    index++;
                }
            }
        }

        // Now scan all the groups in order and return them only if they are in the set of columns.
        List<String> result = new ArrayList<String>();
        Set<String> groups = GrokUtils.getNameGroups(grok.getNamedRegex());
        for (String s : groups) {
            String name = grok.getNamedRegexCollectionById(s);
            if (columnsFound.contains(name))
                result.add(name);
        }
        return result;
    }
}
