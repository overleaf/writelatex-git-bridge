package uk.ac.ic.wlgitbridge.bridge.util;

public interface ThrowingFunction<T, R, E extends Exception> {
    R apply(T param) throws E;
}
