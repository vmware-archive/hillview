package org.hillview.utils;

import org.hillview.RpcTarget;

import java.util.function.Consumer;

/**
 * A pending operation on an RpcTarget object.  The object may no longer exists,
 * and then it will need
 * to be recreated.  The action method will be executed on the
 * target when the target is found or recreated.
 */
public abstract class RpcTargetAction {
    public final RpcTarget.Id id;

    public RpcTargetAction(RpcTarget.Id id) {
        this.id = id;
    }

    /**
     * Action to execute when the target has been obtained.
     * @param target  Actual RpcTarget.
     */
    public abstract void action(RpcTarget target);
}
