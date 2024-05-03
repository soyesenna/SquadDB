package edu.berkeley.cs186.database.query.join;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.query.JoinOperator;
import edu.berkeley.cs186.database.query.QueryOperator;
import edu.berkeley.cs186.database.table.Record;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Performs an equijoin between two relations on leftColumnName and
 * rightColumnName respectively using the Block Nested Loop Join algorithm.
 */
public class BNLJOperator extends JoinOperator {
    protected int numBuffers;

    public BNLJOperator(QueryOperator leftSource,
                        QueryOperator rightSource,
                        String leftColumnName,
                        String rightColumnName,
                        TransactionContext transaction) {
        super(leftSource, materialize(rightSource, transaction),
                leftColumnName, rightColumnName, transaction, JoinType.BNLJ
        );
        this.numBuffers = transaction.getWorkMemSize();
        this.stats = this.estimateStats();
    }

    @Override
    public Iterator<Record> iterator() {
        return new BNLJIterator();
    }

    @Override
    public int estimateIOCost() {
        //This method implements the IO cost estimation of the Block Nested Loop Join
        int usableBuffers = numBuffers - 2;
        int numLeftPages = getLeftSource().estimateStats().getNumPages();
        int numRightPages = getRightSource().estimateIOCost();
        return ((int) Math.ceil((double) numLeftPages / (double) usableBuffers)) * numRightPages +
               getLeftSource().estimateIOCost();
    }

    /**
     * 간단한 중첩 루프 조인을 위한 로직을 실행하는 레코드 이터레이터입니다.
     * 느낌이 궁금하다면 SNLJOperator의 구현을 살펴보세요.
     * fetchNextRecord() 로직에 대한 구현을 살펴보세요.
     */
    private class BNLJIterator implements Iterator<Record>{
        // Iterator over all the records of the left source
        private Iterator<Record> leftSourceIterator;
        // Iterator over all the records of the right source
        private BacktrackingIterator<Record> rightSourceIterator;
        // Iterator over records in the current block of left pages
        private BacktrackingIterator<Record> leftBlockIterator;
        // Iterator over records in the current right page
        private BacktrackingIterator<Record> rightPageIterator;
        // The current record from the left relation
        private Record leftRecord;
        // The next record to return
        private Record nextRecord;

        private BNLJIterator() {
            super();
            this.leftSourceIterator = getLeftSource().iterator();
            this.fetchNextLeftBlock();

            this.rightSourceIterator = getRightSource().backtrackingIterator();
            this.rightSourceIterator.markNext();
            this.fetchNextRightPage();

            this.nextRecord = null;
        }

        /**
         * 왼쪽 소스에서 다음 레코드 블록을 가져옵니다.
         *          * 왼쪽 소스에서 다음 레코드 블록을 가져오려면 왼쪽BlockIterator는 최대
         *          * 왼쪽 소스에서 레코드의 B-2 페이지까지 역추적 반복기로 설정되어야 하며, leftRecord는
         *          * 이 블록의 첫 번째 레코드로 설정해야 합니다.
         *          *
         *          * 왼쪽 소스에 더 이상 레코드가 없는 경우 이 메서드는
         *          * 아무것도 하지 않습니다.
         *          *
         *          * 여기에서 QueryOperator#getBlockIterator를 유용하게 사용할 수 있습니다.
         */
        private void fetchNextLeftBlock() {
            // TODO(proj3_part1): implement Done
            this.leftBlockIterator = QueryOperator.getBlockIterator(leftSourceIterator, getLeftSource().getSchema(), numBuffers - 2);
            this.leftRecord = this.leftBlockIterator.next();
            this.leftBlockIterator.markPrev();
        }

        /**
         *올바른 소스에서 레코드의 다음 페이지를 가져옵니다.
        *          * 오른쪽 페이지 이터레이터는 다음 페이지까지 역추적 이터레이터로 설정해야 합니다.
        *          * 올바른 소스에서 레코드의 한 페이지까지.
        *          *
        *          * 올바른 소스에 더 이상 레코드가 없는 경우, 이 메서드는 다음을 수행해야 합니다.
        *          * 아무것도 하지 않습니다.
        *          *
        *          * 여기에서 QueryOperator#getBlockIterator를 유용하게 사용할 수 있습니다.
        *          */
        private void fetchNextRightPage() {
            // TODO(proj3_part1): implement Done
            this.rightPageIterator = QueryOperator.getBlockIterator(rightSourceIterator, getRightSource().getSchema(), 1);
            this.rightPageIterator.markNext();
        }

        /**
         * 이 조인에서 산출해야 하는 다음 레코드를 반환합니다,
         * 또는 조인할 레코드가 더 이상 없는 경우 null을 반환합니다.
         *
         * 여기서 JoinOperator#compare가 유용할 수 있습니다. (이 파일에서 직접 비교
         * 함수를 이 파일에서 직접 호출할 수 있습니다.
         하위 클래스이므로 이 파일에서 직접 비교 * 함수를 호출할 수 있습니다.)
         */
        private Record fetchNextRecord() {
            // TODO(proj3_part1): implement DONE

            Record joinResult = null;

            while (joinResult == null) {
                if (this.rightPageIterator.hasNext()) {
                    joinResult = joinRecord();
                    continue;
                }

                if (this.leftBlockIterator.hasNext()) {
                    this.rightPageIterator.reset();
                    this.leftRecord = this.leftBlockIterator.next();
                } else if (this.rightSourceIterator.hasNext()) {
                    this.leftBlockIterator.reset();
                    this.leftRecord = this.leftBlockIterator.next();
                    fetchNextRightPage();
                } else if (this.leftSourceIterator.hasNext()) {
                    fetchNextLeftBlock();
                    this.rightSourceIterator.reset();
                    fetchNextRightPage();
                } else break;

                joinResult = joinRecord();
            }


            return joinResult;
        }

        private Record joinRecord() {
            Record right = this.rightPageIterator.next();
            Record result = null;
            if (canJoin(this.leftRecord, right)) {
                result = this.leftRecord.concat(right);
            }

            return result;
        }

        private boolean canJoin(Record left, Record right) {
            return compare(left, right) == 0;
        }

        /**
         * @return true if this iterator has another record to yield, otherwise
         * false
         */
        @Override
        public boolean hasNext() {
            if (this.nextRecord == null) this.nextRecord = fetchNextRecord();
            return this.nextRecord != null;
        }

        /**
         * @return the next record from this iterator
         * @throws NoSuchElementException if there are no more records to yield
         */
        @Override
        public Record next() {
            if (!this.hasNext()) throw new NoSuchElementException();
            Record nextRecord = this.nextRecord;
            this.nextRecord = null;
            return nextRecord;
        }
    }
}
