package com.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName: OrderConsumer
 * @Description: 订单消费者
 * @version: v1.0.0
 * @author: wjw
 * @date: 2018/7/21 21:23
 */
public class OrderConsumer {
    public final static String producerGroup ="orderconsumer";
    public final static String namesrvAddr="192.168.199.100:9876";
    public static void main(String[] args){
        //创建消费者对象
        DefaultMQPushConsumer orderConsumer = new DefaultMQPushConsumer(producerGroup);
        //设置地址
        orderConsumer.setNamesrvAddr(namesrvAddr);
        //设置消费策略

        //CONSUME_FROM_LAST_OFFSET 默认策略，从该队列最尾开始消费，即跳过历史消息
        //CONSUME_FROM_FIRST_OFFSET 从队列最开始开始消费，即历史消息（还储存在broker的）全部消费一遍
        //CONSUME_FROM_TIMESTAMP 从某个时间点开始消费，和setConsumeTimestamp()配合使用，默认是半个小时以前
        orderConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        //设置订阅地址
        try {
            orderConsumer.subscribe("orderTopic","*");
            //注册监听器
            orderConsumer.registerMessageListener(new MessageListenerOrderly() {
                Random random = new Random();
                public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                    context.setAutoCommit(true);
                    System.out.println(Thread.currentThread().getName() + " Receive New Messages: " );
                    for (MessageExt msg: msgs) {
                        System.out.println(msg + ", content:" + new String(msg.getBody()));
                    }
                    try {
                        //模拟业务逻辑处理中...
                        TimeUnit.SECONDS.sleep(random.nextInt(10));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return ConsumeOrderlyStatus.SUCCESS;
                }
            });
            orderConsumer.start();
            System.out.println("orderConsumer.start ...........");
        } catch (MQClientException e) {
            e.printStackTrace();
        }




    }
}
