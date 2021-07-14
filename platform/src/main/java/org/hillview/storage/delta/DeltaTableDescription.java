package org.hillview.storage.delta;

import javax.annotation.Nullable;
import java.io.Serializable;

/**
 * This class contains necessary information to access a delta table
 */
public class DeltaTableDescription implements Serializable {
    static final long serialVersionUID = 1;

    public String path;
    @Nullable
    public Long snapshotVersion;
}
