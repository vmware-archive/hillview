package org.hiero;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

// This annotation is used on methods that are invoked by the RPC layer.
// All these methods have the same interface:
// BiConsumer<RpcRequest, Session>
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface HieroRpc {}
