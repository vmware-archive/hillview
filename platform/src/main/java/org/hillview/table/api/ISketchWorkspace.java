package org.hillview.table.api;

/**
 * Interface implemented by data structures used by incremental sketches
 * while computing.  Sketches have to be immutable, so incremental
 * sketches allocate working data structures of this type.
 */
public interface ISketchWorkspace { }
