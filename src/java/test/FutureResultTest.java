import client.async.FutureResult;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class FutureResultTest {
    @Test
    public void futureTest(){
        final FutureResult futureResult = new FutureResult(new Callable() {
            @Override
            public Object call() throws Exception {
                Thread.sleep(1000);
                return new String("future result");
            }
        });
        futureResult.start();
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    //System.out.println("test");
                    System.out.println(futureResult.get(500, TimeUnit.MILLISECONDS));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                } catch (TimeoutException e) {
                    e.printStackTrace();
                }
            }
        });
        thread.start();
        try {
            System.out.println(futureResult.get());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }


    }
}
