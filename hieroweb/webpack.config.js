var path = require('path');
var nodepath = path.resolve("/usr/local/lib/node_modules");
// This is very weird: webpack is installed globally,
// but apparently webpack cannot load itself from a global location
var webpack = require(nodepath + '/' + 'webpack');

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
        extensions: ["", ".ts", ".tsx", ".js"],
    },
    module: {
        loaders: [
            // all files with a '.ts' or '.tsx' extension will be handled by 'ts-loader'
            {
                test: /\.tsx?$/,
                loader: "ts-loader",
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