package ed.inf.adbs.blazedb;

import java.util.ArrayList;

/**
 * The Tuple class represents a row of data.
 *
 * You will need to modify this class, obviously :).
 */
public class Tuple {

    private final ArrayList<Integer> attributes;

    public Tuple(ArrayList<Integer> attributes) {
        this.attributes = attributes;
    }

    public ArrayList<Integer> getTuple() {
        return attributes;
    }

    public Integer getAttribute(int i) {
        return attributes.get(i);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < attributes.size(); i++) {
            sb.append(attributes.get(i));
            if (i < attributes.size() - 1) {
                sb.append(", ");
            }
        }
        return sb.toString();
    }
}