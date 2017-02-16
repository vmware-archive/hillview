// Configuration file for webpack 1.X

var path = require('path');
var nodepath = path.resolve("/usr/local/lib/node_modules");

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
    devtool: "source-map"
};