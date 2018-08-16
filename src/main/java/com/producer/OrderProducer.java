package com.producer;

import com.pojo.OrderDemo;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.lang.String;

/**
 * @ClassName: OrderProducer
 * @Description: 有序消息
 * @version: v1.0.0
 * @author: wjw
 * @date: 2018/7/21 12:46
 */
public class OrderProducer {
    public final static String producerGroup ="orderproducer";
    public final static String namesrvAddr="192.168.199.100:9876";
    public static void main(String[] args)
    {
      //创建生产者
        DefaultMQProducer orderProducer = new DefaultMQProducer(producerGroup);
        //设置nameServer服务地址
        orderProducer.setNamesrvAddr(namesrvAddr);
        //启动生产者服务
        try {
            orderProducer.start();
            // 自定义一个tag数组
            String[] tags = new String[]{"TagA", "TagB", "TagC"};
            // 订单列表
            List<OrderDemo> orderList =  new OrderProducer().buildOrders();

            Date date = new Date();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String dateStr = sdf.format(date);
            for (int i = 0; i < orderList.size(); i++) {
                // 加个时间后缀
                String body = dateStr + " Hello RocketMQ " + orderList.get(i);
                Message msg = new Message("orderTopic", tags[i % tags.length], "KEY" + i, body.getBytes(RemotingHelper.DEFAULT_CHARSET));

                SendResult sendResult = orderProducer.send(msg, new MessageQueueSelector() {
                    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                        Long id = (Long) arg;
                        long index = id % mqs.size();
                        return mqs.get((int) index);
                    }
                }, orderList.get(i).getId());
                System.out.println(sendResult+"msg:"+new java.lang.String(msg.getBody()));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            orderProducer.shutdown();
        }


    }

  
    /**
    * @Description: 生成订单数据 创建->付款->推送 ->完成
    * @param: 
    * @return
    * @version: v1.0.0
    * @exception
    * @author: wjw
    * @date: 2018/7/21 21:11 
    */
    private List<OrderDemo> buildOrders() {
        List<OrderDemo> orderList = new ArrayList<OrderDemo>();

        OrderDemo orderDemo = new OrderDemo();
        orderDemo.setId(15103111039L);
        orderDemo.setDesc("创建");
        orderList.add(orderDemo);

        orderDemo = new OrderDemo();
        orderDemo.setId(15103111065L);
        orderDemo.setDesc("创建");
        orderList.add(orderDemo);

        orderDemo = new OrderDemo();
        orderDemo.setId(15103111039L);
        orderDemo.setDesc("付款");
        orderList.add(orderDemo);

        orderDemo = new OrderDemo();
        orderDemo.setId(15103117235L);
        orderDemo.setDesc("创建");
        orderList.add(orderDemo);

        orderDemo = new OrderDemo();
        orderDemo.setId(15103111065L);
        orderDemo.setDesc("付款");
        orderList.add(orderDemo);

        orderDemo = new OrderDemo();
        orderDemo.setId(15103117235L);
        orderDemo.setDesc("付款");
        orderList.add(orderDemo);

        orderDemo = new OrderDemo();
        orderDemo.setId(15103111065L);
        orderDemo.setDesc("完成");
        orderList.add(orderDemo);

        orderDemo = new OrderDemo();
        orderDemo.setId(15103111039L);
        orderDemo.setDesc("推送");
        orderList.add(orderDemo);

        orderDemo = new OrderDemo();
        orderDemo.setId(15103117235L);
        orderDemo.setDesc("完成");
        orderList.add(orderDemo);

        orderDemo = new OrderDemo();
        orderDemo.setId(15103111039L);
        orderDemo.setDesc("完成");
        orderList.add(orderDemo);

        return orderList;
    }
}
