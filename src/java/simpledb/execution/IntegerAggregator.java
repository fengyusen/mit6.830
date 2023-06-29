package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldType;
    private int aggField;
    private Op aop;


    private ConcurrentMap<Field, Integer> groupMap;
    private ConcurrentMap<Field, Integer> countMap;
    private Map<Field, List<Integer>> avgMap;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldType = gbfieldtype;
        this.aggField = afield;
        this.aop = what;
        this.groupMap = new ConcurrentHashMap<>();
        this.countMap = new ConcurrentHashMap<>();
        this.avgMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        IntField intField = (IntField) tup.getField(this.aggField);
        Field field = this.gbfield == NO_GROUPING ? null : tup.getField(this.gbfield);
        int newValue = intField.getValue();
        if (field != null && field.getType() != this.gbfieldType) {
            throw new IllegalArgumentException("Given tuple has wrong type");
        }
        switch (this.aop) {
            case MIN:
                if (!this.groupMap.containsKey(field))
                    this.groupMap.put(field, newValue);
                else
                    this.groupMap.put(field, Math.min(this.groupMap.get(field),newValue));
                break;
            case MAX:
                if (!this.groupMap.containsKey(field))
                    this.groupMap.put(field, newValue);
                else
                    this.groupMap.put(field, Math.max(this.groupMap.get(field), newValue));
                break;
            case SUM:
                if (!this.groupMap.containsKey(field))
                    this.groupMap.put(field, newValue);
                else
                    this.groupMap.put(field, this.groupMap.get(field) + newValue);
                break;
            case COUNT:
                if (!this.groupMap.containsKey(field))
                    this.groupMap.put(field, 1);
                else
                    this.groupMap.put(field, this.groupMap.get(field) + 1);
                break;
            case SC_AVG:
                IntField countField = null;
                if (field == null)
                    countField = (IntField) tup.getField(1);
                else
                    countField = (IntField) tup.getField(2);
                int countValue = countField.getValue();
                if (!this.groupMap.containsKey(field)) {
                    this.groupMap.put(field, newValue);
                    this.countMap.put(field, countValue);
                } else {
                    this.groupMap.put(field, this.groupMap.get(field) + newValue);
                    this.countMap.put(field, this.countMap.get(field) + countValue);
                }
            case SUM_COUNT:

            case AVG:
                if (!this.avgMap.containsKey(field)) {
                    List<Integer> l = new ArrayList<>();
                    l.add(newValue);
                    this.avgMap.put(field, l);
                } else {
                    List<Integer> l = this.avgMap.get(field);
                    l.add(newValue);
                }
                break;
            default:
                throw new IllegalArgumentException("Aggregate not supported!");
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new IntegerAggregateIterator();
        //throw new UnsupportedOperationException("please implement me for lab2");
    }


    private class IntegerAggregateIterator extends AggregateIterator {
        private Iterator<Map.Entry<Field, List<Integer>>> avgIterator;
        private boolean isAvg;
        private boolean isSCAvg;
        private boolean isSumCount;

        IntegerAggregateIterator() {
            super(groupMap, gbfieldType);
            this.isAvg = aop.equals(Op.AVG);
            this.isSCAvg = aop.equals(Op.SC_AVG);
            this.isSumCount = aop.equals(Op.SUM_COUNT);
            if (isSumCount) {
                this.tupleDesc = new TupleDesc(new Type[]{this.itgbfieldType, Type.INT_TYPE, Type.INT_TYPE},
                        new String[]{"groupVal", "sumVal", "countVal"});
            }
        }

        private int sumList(List<Integer> l) {
            int sum = 0;
            for (int i : l) {
                sum += i;
            }
            return sum;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            super.open();
            if (this.isAvg || this.isSumCount) {
                this.avgIterator = avgMap.entrySet().iterator();
            } else {
                this.avgIterator = null;
            }
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (this.isAvg || this.isSumCount)
                return avgIterator.hasNext();
            return super.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            Tuple tuple = new Tuple(tupleDesc);
            if (this.isAvg || this.isSumCount) {
                Map.Entry<Field, List<Integer>> entry = this.avgIterator.next();
                Field field = entry.getKey();
                List<Integer> list = entry.getValue();
                if (this.isAvg) {
                    int value = this.sumList(list) / list.size();
                    this.setFields(tuple, value, field);
                } else {
                    this.setFields(tuple, sumList(list), field);
                    if (field != null) {
                        tuple.setField(2, new IntField(list.size()));
                    } else {
                        tuple.setField(1, new IntField(list.size()));
                    }
                }
                return tuple;
            } else if (this.isSCAvg) {
                Map.Entry<Field, Integer> entry = this.iterator.next();
                Field field = entry.getKey();
                this.setFields(tuple, entry.getValue() / countMap.get(field), field);
                return tuple;
            }
            return super.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            super.rewind();
            if (this.isAvg || this.isSumCount) {
                this.avgIterator = avgMap.entrySet().iterator();
            }
        }

        @Override
        public TupleDesc getTupleDesc() {
            return super.getTupleDesc();
        }

        @Override
        public void close() {
            super.close();
            this.avgIterator = null;
        }
    }
}
