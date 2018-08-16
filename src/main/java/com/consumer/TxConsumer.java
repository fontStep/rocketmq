package com.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @ClassName: TxConsumer
 * @Description: 事务消息消费者
 * @version: v1.0.0
 * @author: wjw
 * @date: 2018/7/22 10:22
 */
public class TxConsumer {
    public final static String consumerGroup ="txconsumer";
    public final static String namesrvAddr="192.168.199.100:9876";
    public static void main(String[] args){
        DefaultMQPushConsumer txConsume = new DefaultMQPushConsumer(consumerGroup);
        txConsume.setNamesrvAddr(namesrvAddr);
        txConsume.setConsumeThreadMin(10);
        txConsume.setConsumeThreadMin(20);
        txConsume.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        //设置订阅地址
        String topic ="txTopic";
        String subExpression = "*";
        try {
            txConsume.subscribe(topic,subExpression);
            //注册监听器
            txConsume.registerMessageListener(new Listener());
            txConsume.start();
            System.out.println("txConsume.start .........");
        } catch (MQClientException e) {
            e.printStackTrace();
        }

    }
    static class Listener implements MessageListenerConcurrently{

        /**
         * It is not recommend to throw exception,rather than returning ConsumeConcurrentlyStatus.RECONSUME_LATER if consumption failure
         *
         * @param msgs    msgs.size() >= 1<br> DefaultMQPushConsumer.consumeMessageBatchMaxSize=1,you can modify here
         * @param context
         * @return The consume status
         */
        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
            try{
                //消费消息
                for(int i=0;i<msgs.size();i++) {
                    System.out.println(msgs.get(i));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }catch (Exception e){
             e.printStackTrace();
             return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
        }
    }
}
