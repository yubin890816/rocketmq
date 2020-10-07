package com.yubin.rocketmq;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * 消费者顺序消费消息
 *
 * @author YUBIN
 * @create 2020-10-07
 */
public class Consumer2 {

    public static void main(String[] args) throws Exception {
        // 创建消费者对象,并指定消费者组
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("testConsumerGroup002");
        // 设置消费者的nameserver
        consumer.setNamesrvAddr("192.168.254.4:9876");
        // 订阅指定topic的消息
        consumer.subscribe("test002", "*");
        // 注册消息监听器
        /**
         * MessageListenerConcurrently: 并发消费/开多线程
         * MessageListenerOrderly: 顺序消费, 对一个Queue开启一个线程,多个Queue开多个线程
         */
        // 最大开启消费线程数
        //consumer.setConsumeThreadMax(1);
        // 最小线程数
        //consumer.setConsumeThreadMin(1);
        consumer.registerMessageListener(new MessageListenerOrderly() {
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                for (MessageExt messageExt : msgs) {
                    System.out.println(new String(messageExt.getBody()) +
                            " , Thread:" + Thread.currentThread().getName() + " ,QueueId: " + messageExt.getQueueId());
                }
                // 默认情况下 这条消息只会被一个Consumer消费(点对点)
                // message 状态修改(通知broker消息消费状态)
                // ack确认机制
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        // 启动消费者
        consumer.start();
        System.out.println("consumer start...");
    }
}
