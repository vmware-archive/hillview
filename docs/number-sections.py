#!/usr/bin/env python3
# This script numbers the section in a markdown document
# It makes some simplifying assumptions about the structure
# of the document: i.e., no references occur in quoted blocks
# pylint: disable=invalid-name,missing-docstring

import re
import os
import datetime

class Headings:
    def __init__(self):
        self.headingCounters = {}
        self.maxDepth = 5
        for i in range(0, self.maxDepth):
            self.headingCounters[i] = 1
        self.lastDepth = 0
        self.toc = []
        self.referenceRewrite = {}

    def clearToc(self):
        """Clear table of contents"""
        self.toc = []

    def makeReference(self, ref):
        """Converts text into a suitable markdown reference"""
        ref = ref.strip()
        return "(#" + ref.lower().replace(".", "").replace(" ", "-") + ")"

    def getHeading(self, row):
        """If row is a heading (starting with a number of #s) it is prepended
           with the position in the hierarcy"""
        depth = 0
        while row[depth] == "#":
            depth = depth + 1
        result = row

        depth -= 1
        if depth == 0:
            pass
        elif depth == self.lastDepth:
            self.headingCounters[depth-1] = self.headingCounters[depth-1] + 1
        elif depth < self.lastDepth:
            for index in range(depth, self.maxDepth):
                self.headingCounters[index] = 1
            self.headingCounters[depth-1] += 1
        elif depth == self.lastDepth + 1:
            self.headingCounters[depth] = 1
        else:
            raise Exception("Incorrect heading hierarchy at " + row)

        if depth > 0:
            sectionNumber = ""
            for index in range(0, depth):
                sectionNumber = sectionNumber + str(self.headingCounters[index]) + "."
            heading = row[depth+1:len(row)]
            origRef = self.makeReference(heading)
            ref = self.makeReference(sectionNumber + heading)
            self.toc.append("|" + sectionNumber + "|[" + heading.strip() + "]" + ref + "|\n")
            result = row[0:depth+2] + sectionNumber + row[depth+1:len(row)]
            self.referenceRewrite[origRef] = ref

        self.lastDepth = depth
        return result

    def fixReference(self, match):
        reference = match.group(0)
        if reference in self.referenceRewrite:
            return self.referenceRewrite[reference]
        raise Exception("No such reference: " + reference)

    def fixReferences(self, row):
        imagePattern = "\(([^)]*\.png)\)"
        images = re.findall(imagePattern, row)
        for image in images:
            if not os.path.isfile(image):
                raise Exception("Could not find image " + image)
        refPattern = "(\(#[^)]*\))"
        row = re.sub(refPattern, self.fixReference, row)
        return row

def process(rows, headings, banner):
    outRows = []
    for row in rows:
        if row.startswith("#"):
            row = headings.getHeading(row)
        outRows.append(row)
    for index in range(0, len(outRows) - 1):
        row = headings.fixReferences(outRows[index])
        outRows[index] = row

    result = [banner]
    index = 0
    while index < len(outRows) and not outRows[index].startswith("##"):
        result.append(outRows[index])
        index += 1

    if headings.toc:
        result.append("Updated on " + datetime.datetime.today().strftime("%Y %b %d") + ".\n\n")
        result.append("# Contents\n")
        result.append("|Section|Reference|\n")
        result.append("|---:|:---|\n")
        result.extend(headings.toc)
    result.extend(outRows[index:])
    return result

def rewrite(source, destination, headings, commentStart, commentEnd):
    headings.clearToc()
    script = "doc/number-sections.py"
    banner = commentStart + " automatically generated from " + source + \
             " by " + script + commentEnd + "\n"
    with open(source, 'r') as f:
        rows = f.readlines()
    rows = process(rows, headings, banner)
    with open(destination, 'w') as f:
        f.writelines(rows)

def main():
    headings = Headings()
    input = "userManual.src"
    output = "userManual.md"
    rewrite(input, output, headings, "<!--", "-->")
    input = "../web/src/main/webapp/ui/helpUrl.src"
    output = "../web/src/main/webapp/ui/helpUrl.ts"
    try:
        rewrite(input, output, headings, "/*", "*/")
    except Exception as e:
        raise Exception("Error fixing file " + input) from e

if __name__ == "__main__":
    main()
