package org.hillview.sketches;

import org.hillview.table.api.ISketchWorkspace;

/**
 * Empty sketch workspace.
 */
public class EmptyWorkspace implements ISketchWorkspace {
    private EmptyWorkspace() {}
    static EmptyWorkspace instance = new EmptyWorkspace();
}
