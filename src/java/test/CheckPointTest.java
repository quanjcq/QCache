import org.junit.Test;
import store.checkpoint.CheckPoint;

public class CheckPointTest {
    @Test
    public void checkTerm(){
        String filePath = "/home/jcq/store/01";
        CheckPoint checkPoint = new CheckPoint(filePath);
        long term = 1000;
        //set term
        checkPoint.setTerm(term);
        assert term == checkPoint.getTerm();
    }
    @Test
    public void checkLastAppliedIndex(){
        String filePath = "/home/jcq/store/01";
        CheckPoint checkPoint = new CheckPoint(filePath);
        long index = 2000;
        //set term
        checkPoint.setLastAppliedIndex(index);
        assert index == checkPoint.getLastAppliedIndex();
    }
    @Test
    public void checkFlush(){
        String filePath = "/home/jcq/store/01";
        CheckPoint checkPoint = new CheckPoint(filePath);
        long flush = 3000;
        //set term
        checkPoint.setFlush(flush);
        assert flush == checkPoint.getFlush();
    }
    @Test
    public void checkAll(){
        String filePath = "/home/jcq/store/2/0";
        CheckPoint checkPoint = new CheckPoint(filePath);
        System.out.println("Term: " + checkPoint.getTerm());
        System.out.println("Index: " + checkPoint.getLastAppliedIndex());
        System.out.println("flush: " + checkPoint.getFlush());
    }

}
