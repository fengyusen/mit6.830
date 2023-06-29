package simpledb.optimizer;

import simpledb.execution.Predicate;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int min;
    private int max;
    private int tupleNum = 0;
    private int[] buckets;
    private double width;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.min = min;
        this.max = max;
        this.buckets = new int[buckets];
        this.width = (1. + max - min) / this.buckets.length;
    }


    /**
     * get the index of v in the IntHistogram
     *
     * @param v target value
     * @return index in the IntHistogram
     */
    private int getIndex(int v) {
        if (v < this.min || v > this.max) throw new IllegalArgumentException("value {" + v + "} is illegal");
        return (int) ((v - this.min) / this.width);
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int index = getIndex(v);
        this.buckets[index]++;
        tupleNum++;

    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here
        switch (op) {
            case LESS_THAN:
                if (v <= min) return 0.0;
                else if (v >= max) return 1.0;
                int index = getIndex(v);
                double cnt = 0;
                for (int i = 0; i < index; i++) {
                    cnt += buckets[i];
                }
                cnt += buckets[index] / width * (v - index * width - min);
                return cnt / tupleNum;
            case LESS_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.LESS_THAN, v + 1);
            case GREATER_THAN:
                return 1 - estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, v);
            case GREATER_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.GREATER_THAN, v - 1);
            case EQUALS:
                return estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, v) - estimateSelectivity(Predicate.Op.LESS_THAN, v);
            case NOT_EQUALS:
                return 1 - estimateSelectivity(Predicate.Op.EQUALS, v);
            default:
                throw new IllegalArgumentException("Operation is illegal");
        }
        // return -1.0;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        int cnt = 0;
        for (int bucket : buckets) {
            cnt += bucket;
        }
        return 1.0 * cnt / tupleNum;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return String.format("IntHistogram(buckets=%d, min=%d, max=%d", buckets.length, min, max);
        // return null;
    }
}
