package org.hiero.sketch.dataset.api;

import rx.Observer;

/**
 * The double is a monotonically increasing deltaValue between 0 and 1 indicating how much of the work
 * has already been deltaDone.
 */
public interface IProgressObserver extends Observer<Double> {}
