package simpledb.execution;

import com.sun.deploy.security.SelectableSecurityManager;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int groupField;
    private Type groupFieldType;
    private int aggregateField;
    private Op arrregateOp;

    private ConcurrentMap<Field, Integer> groupMap;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (!what.equals(Op.COUNT))
            throw new IllegalArgumentException("Only COUNT is supported for String fields!");
        this.groupField = gbfield;
        this.groupFieldType = gbfieldtype;
        this.aggregateField = afield;
        this.arrregateOp = what;

        this.groupMap = new ConcurrentHashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        StringField afield = (StringField) tup.getField(this.aggregateField);
        Field gbfield = this.groupField == NO_GROUPING ? null : tup.getField(this.groupField);
        String newValue = afield.getValue();
        if (gbfield != null && gbfield.getType() != this.groupFieldType) {
            throw new IllegalArgumentException("Given tuple has wrong type");
        }
        if (!this.groupMap.containsKey(gbfield))
            this.groupMap.put(gbfield, 1);
        else
            this.groupMap.put(gbfield, this.groupMap.get(gbfield) + 1);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new AggregateIterator(this.groupMap, this.groupFieldType);
        //throw new UnsupportedOperationException("please implement me for lab2");
    }
}

class AggregateIterator implements OpIterator {
    protected Iterator<Map.Entry<Field, Integer>> iterator;
    TupleDesc tupleDesc;
    private Map<Field, Integer> groupMap;
    protected Type itgbfieldType;

    public AggregateIterator(Map<Field, Integer> groupMap, Type gbfieldType) {
        this.groupMap = groupMap;
        this.itgbfieldType = gbfieldType;
        if (this.itgbfieldType == null) {
            this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggregateVal"});
        }
        else {
            this.tupleDesc = new TupleDesc(new Type[]{this.itgbfieldType, Type.INT_TYPE}, new String[]{"groupVal", "aggregateVal"});
        }
    }

    void setFields(Tuple tuple, int value, Field field) {
        if (field == null) {
            tuple.setField(0, new IntField(value));
        } else {
            tuple.setField(0, field);
            tuple.setField(1, new IntField(value));
        }
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        this.iterator = groupMap.entrySet().iterator();
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        return this.iterator.hasNext();
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        Map.Entry<Field, Integer> entry = this.iterator.next();
        Field field = entry.getKey();
        Tuple tuple = new Tuple(this.tupleDesc);
        this.setFields(tuple, entry.getValue(), field);
        return tuple;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        this.iterator = groupMap.entrySet().iterator();
    }

    @Override
    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }

    @Override
    public void close() {
        this.iterator = null;
        this.tupleDesc = null;
    }
}
