/*
 * Copyright (c) 2017 VMWare Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

var path = require('path');
var nodepath = path.resolve("/usr/local/lib/node_modules");
// This is very weird: webpack is installed globally,
// but apparently webpack cannot load itself from a global location
// var webpack = require(nodepath + '/' + 'webpack');

module.exports = {
    entry: "./hiero.ts",
    output: {
        filename: "./bundle.js"
    },
    // This is needed to find ts-loader which is installed globally
    resolveLoader: {
        fallback: nodepath
    },
    resolve: {
        // Add '.ts' and '.tsx' as a resolvable extension.
        extensions: ["", ".ts", ".tsx", ".js"]
    },
    module: {
        loaders: [
            // all files with a '.ts' or '.tsx' extension will be handled by 'ts-loader'
            {
                test: /\.tsx?$/,
                loader: "ts-loader"
            }
        ],
    },
    // This is needed for webpack to export some global symbols
    //plugins: [
    //    new webpack.ProvidePlugin({
    //        $: "jquery",
    //        jQuery: "jquery"
    //    })
    //],
    devtool: "source-map"
};
