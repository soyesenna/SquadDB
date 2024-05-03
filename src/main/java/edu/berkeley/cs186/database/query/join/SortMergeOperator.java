package edu.berkeley.cs186.database.query.join;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.query.JoinOperator;
import edu.berkeley.cs186.database.query.MaterializeOperator;
import edu.berkeley.cs186.database.query.QueryOperator;
import edu.berkeley.cs186.database.query.SortOperator;
import edu.berkeley.cs186.database.table.Record;

import java.util.*;

public class SortMergeOperator extends JoinOperator {
    public SortMergeOperator(QueryOperator leftSource,
                             QueryOperator rightSource,
                             String leftColumnName,
                             String rightColumnName,
                             TransactionContext transaction) {
        super(prepareLeft(transaction, leftSource, leftColumnName),
              prepareRight(transaction, rightSource, rightColumnName),
              leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);
        this.stats = this.estimateStats();
    }

    private static QueryOperator prepareLeft(TransactionContext transaction,
                                             QueryOperator leftSource,
                                             String leftColumn) {
        leftColumn = leftSource.getSchema().matchFieldName(leftColumn);
        if (leftSource.sortedBy().contains(leftColumn)) return leftSource;
        return new SortOperator(transaction, leftSource, leftColumn);
    }

    private static QueryOperator prepareRight(TransactionContext transaction,
                                              QueryOperator rightSource,
                                              String rightColumn) {
        rightColumn = rightSource.getSchema().matchFieldName(rightColumn);
        if (!rightSource.sortedBy().contains(rightColumn)) {
            return new SortOperator(transaction, rightSource, rightColumn);
        } else if (!rightSource.materialized()) {
            return new MaterializeOperator(rightSource, transaction);
        }
        return rightSource;
    }

    @Override
    public Iterator<Record> iterator() {
        return new SortMergeIterator();
    }

    @Override
    public List<String> sortedBy() {
        return Arrays.asList(getLeftColumnName(), getRightColumnName());
    }

    @Override
    public int estimateIOCost() {
        return 0;
    }

    private class SortMergeIterator implements Iterator<Record> {
        private Iterator<Record> leftIterator;
        private BacktrackingIterator<Record> rightIterator;
        private Record leftRecord;
        private Record nextRecord;
        private Record rightRecord;
        private boolean marked;

        private SortMergeIterator() {
            super();
            leftIterator = getLeftSource().iterator();
            rightIterator = getRightSource().backtrackingIterator();
            rightIterator.markNext();

            if (leftIterator.hasNext() && rightIterator.hasNext()) {
                leftRecord = leftIterator.next();
                rightRecord = rightIterator.next();
            }

            this.marked = false;
        }

        @Override
        public boolean hasNext() {
            if (this.nextRecord == null) this.nextRecord = fetchNextRecord();
            return this.nextRecord != null;
        }

        @Override
        public Record next() {
            if (!this.hasNext()) throw new NoSuchElementException();
            Record nextRecord = this.nextRecord;
            this.nextRecord = null;
            return nextRecord;
        }

        private Record fetchNextRecord() {
            while (leftRecord != null && rightRecord != null) {
                int cmp = compare(leftRecord, rightRecord);
                if (cmp == 0) {
                    if (!marked) {
                        rightIterator.markPrev();
                        marked = true;
                    }
                    Record joined = leftRecord.concat(rightRecord);
                    if (!rightIterator.hasNext()) {
                        rightIterator.reset();
                        rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;
                        leftRecord = leftIterator.hasNext() ? leftIterator.next() : null;
                        marked = false;
                    } else {
                        rightRecord = rightIterator.next();
                    }
                    return joined;
                } else if (cmp < 0) {
                    leftRecord = leftIterator.hasNext() ? leftIterator.next() : null;
                    if (marked) {
                        rightIterator.reset();
                        rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;
                        marked = false;
                    }
                } else {
                    rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;
                    if (marked) {
                        rightIterator.reset();
                        rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;
                    }
                }
            }
            if (marked) {
                rightIterator.reset();
                rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;
                marked = false;
            }
            if (!marked) {
                leftRecord = leftIterator.hasNext() ? leftIterator.next() : null;
            }
            return null;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
