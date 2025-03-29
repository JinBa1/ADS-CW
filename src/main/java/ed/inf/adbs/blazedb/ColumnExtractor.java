package ed.inf.adbs.blazedb;

import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.schema.Column;

import java.util.ArrayList;
import java.util.List;

public class ColumnExtractor extends ExpressionVisitorAdapter {
    private List<Column> columns = new ArrayList<>();

    @Override
    public void visit(Column column) {
        columns.add(column);
    }

    public List<Column> getColumns() {
        return columns;
    }
}
