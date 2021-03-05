package uk.ac.ic.wlgitbridge.bridge.util;

import java.util.function.Function;

public class Pair<L, R> {
    private L left;
    private R right;

    public Pair(L left, R right) {
        this.left = left;
        this.right = right;
    }

    public L getLeft() {
        return left;
    }

    public void setLeft(L left) {
        this.left = left;
    }

    public L updateLeft(Function<L, L> f) {
        this.left = f.apply(this.left);
        return this.left;
    }

    public R getRight() {
        return right;
    }

    public void setRight(R right) {
        this.right = right;
    }

    public R updateRight(Function<R, R> f) {
        this.right = f.apply(this.right);
        return this.right;
    }
}
