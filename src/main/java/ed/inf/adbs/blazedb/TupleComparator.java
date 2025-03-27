package ed.inf.adbs.blazedb;

import java.util.Comparator;
import java.util.List;

public class TupleComparator implements Comparator<Tuple> {

    private List<Integer> sortColumnIndices;

    public TupleComparator(List<Integer> sortColumnIndices) {
        this.sortColumnIndices = sortColumnIndices;
    }

    @Override
    public int compare(Tuple t1, Tuple t2) {
        for (Integer columnIndex : sortColumnIndices) {
            int value1 = t1.getAttribute(columnIndex);
            int value2 = t2.getAttribute(columnIndex);

            if (value1 != value2) {
                return value1 - value2;
            }
        }
        return 0;
    }
}
