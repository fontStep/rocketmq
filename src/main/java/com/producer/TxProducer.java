package com.producer;

import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionCheckListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * @ClassName: TxProducer
 * @Description: 事务消息
 * @version: v1.0.0
 * @author: wjw
 * @date: 2018/7/21 21:42
 */
public class TxProducer {
    public final static String producerGroup ="transactionproducer";
    public final static String namesrvAddr="192.168.199.100:9876";
    public static void main(String[] args){
        //当RocketMQ发现`Prepared消息`时，会根据这个Listener实现的策略来决断事务
        TransactionCheckListener transactionCheckListener = new TransactionCheckListenerImpl();
        //创建transactionMQProducerc
        TransactionMQProducer transactionMQProducer = new TransactionMQProducer(producerGroup);
        // 设置事务决断处理类
        transactionMQProducer.setTransactionCheckListener(transactionCheckListener);
        transactionMQProducer.setNamesrvAddr(namesrvAddr);
        // 本地事务的处理逻辑，相当于示例中检查Bob账户并扣钱的逻辑
        TransactionExecuterImpl tranExecuter = new TransactionExecuterImpl();
        try {
            transactionMQProducer.start();
            //构建msg
            String body ="事务消息";
            Message msg = new Message("txTopic", "tagA", "1", body.getBytes(RemotingHelper.DEFAULT_CHARSET));
            SendResult sendResult = transactionMQProducer.sendMessageInTransaction(msg,tranExecuter,"1");
            System.out.println(sendResult);
        } catch (Exception e) {
            e.printStackTrace();
        }
        transactionMQProducer.shutdown();
    }
}
