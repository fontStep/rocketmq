package com.producer;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionCheckListener;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName: TransactionCheckListenerImpl
 * @Description: TOOD
 * @version: v1.0.0
 * @author: wjw
 * @date: 2018/7/21 21:48
 */
public class TransactionCheckListenerImpl implements TransactionCheckListener {

    public LocalTransactionState checkLocalTransactionState(MessageExt msg) {
        Random random = new Random();
        try {
            System.out.println("处理本地事务 Start。。。。。。。。。。。。");
            System.out.println(msg);
            TimeUnit.SECONDS.sleep(random.nextInt(10));
            System.out.println("处理本地事务 End。。。。。。。。。。。。");
            return LocalTransactionState.COMMIT_MESSAGE;
        } catch (InterruptedException e) {
            e.printStackTrace();
            return  LocalTransactionState.ROLLBACK_MESSAGE;
        }

    }
}
