package core.message;

import common.Node;

import java.util.TreeMap;

public class RaftSnapHotMessage extends RaftMessage {

    private TreeMap<Long, Node> circle = null;


    public TreeMap<Long, Node> getCircle() {
        return circle;
    }

    public void setCircle(TreeMap<Long, Node> circle) {
        this.circle = circle;
    }

    @Override
    public String toString() {
        return super.toString() +
                "circle=" + circle +
                '}';
    }
}
