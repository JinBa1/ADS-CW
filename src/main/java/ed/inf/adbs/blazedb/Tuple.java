package ed.inf.adbs.blazedb;

import java.util.ArrayList;
import java.util.Objects;

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


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Tuple other = (Tuple) o;

        // Two tuples are equal if they have the same attributes
        if (attributes.size() != other.attributes.size()) {
            return false;
        }

        for (int i = 0; i < attributes.size(); i++) {
            if (!Objects.equals(attributes.get(i), other.attributes.get(i))) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(attributes);
    }
}