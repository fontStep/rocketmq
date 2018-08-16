package com.producer;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * @ClassName: Producer
 * @Description: 生产者
 * @version: v1.0.0
 * @author: wjw
 * @date: 2018/7/7 17:58
 */
public class Producer {
    public final static String producerGroup ="producer1";
    public final static String namesrvAddr="192.168.199.100:9876";
    public static void main(String[] args) throws MQClientException, InterruptedException {

        //声明并初始化一个producer
        //需要一个producer group名字作为构造方法的参数，这里为producer1
        DefaultMQProducer producer = new DefaultMQProducer(producerGroup);

        //设置NameServer地址,此处应改为实际NameServer地址，多个地址之间用；分隔
        //NameServer的地址必须有，但是也可以通过环境变量的方式设置，不一定非得写死在代码里
        producer.setNamesrvAddr(namesrvAddr);

        //调用start()方法启动一个producer实例
        producer.start();

        for (int i=1;i<=10;i++){
            //创建消息
            try {
                Message message = new Message("TopicTest","TagA", ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
                SendResult sendResult =   producer.send(message);
                System.out.println(sendResult);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        producer.shutdown();
    }


}

