import client.CacheClient;
import client.async.AsyncClient;
import client.async.Future;
import client.async.FutureListener;
import constant.CacheOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class AsyncClientDemo {
    private static Logger log = LoggerFactory.getLogger(AsyncClient.class);
    public static void main(String[] args) {
        CacheClient cacheClient = new CacheClient.newBuilder()
                .setNumberOfReplicas(CacheOptions.numberOfReplicas)
                .setNewNode("1:127.0.0.1:9001")
                .setNewNode("2:127.0.0.1:9002")
                .setNewNode("3:127.0.0.1:9003")
                .build();

        AsyncClient asyncClient = new AsyncClient(cacheClient);

        //put key val
        Future futurePut = asyncClient.put("name", "quan", new FutureListener() {
            @Override
            public void onSuccess(Object obj) {
                log.info("put success");
                System.out.println("put success");
            }

            @Override
            public void onException(Throwable throwable) {
                log.error(throwable.toString());
                System.out.println("exception");
            }
        });
        try {
            //立刻返回,任务还没有运行完肯定返回null
            System.out.println(futurePut.getNow());

            System.out.println(futurePut.get());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        //get key
        Future futureGet = asyncClient.get("name", new FutureListener() {
            @Override
            public void onSuccess(Object obj) {
                log.info("get success");
                System.out.println("get success");
            }

            @Override
            public void onException(Throwable throwable) {
                log.error(throwable.toString());
                System.out.println("exception");
            }
        });
        try {
            System.out.println(futureGet.get());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        //del
        Future futureDel = asyncClient.del("name", new FutureListener() {
            @Override
            public void onSuccess(Object obj) {
                log.info("del success");
                System.out.println("del success");
            }

            @Override
            public void onException(Throwable throwable) {
                log.error(throwable.toString());
                System.out.println("exception");
            }
        });
        try {
            System.out.println(futureDel.get());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        //status
        Future futureStatus = asyncClient.status(null);
        try {
            //最多等待1ms
            System.out.println(futureStatus.get(1, TimeUnit.MILLISECONDS));
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }
}
