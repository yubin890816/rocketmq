package com.yubin.rocketmq;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;

/**
 * 消息消费者
 *
 * @author YUBIN
 * @create 2020-09-26
 */
public class Consumer {

    public static void main(String[] args) throws Exception {
        test1();
    }

    /**
     * 简单的消息消费者
     * @throws MQClientException
     */
    private static void test1() throws MQClientException {
        // RocketMQ的消费者获取消息有两种模式: 客户端主动向服务端拉取消息, 服务端向客户端推送消息(这里采用推送的方式)
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("testConsumerGroup");
        // 设置nameserver地址
        consumer.setNamesrvAddr("192.168.254.4:9876");
        // 订阅消息 每个consumer只能关注一种消息类型(topic)
        // 第一个参数: topic 关注的消息类型
        // 第二个参数: 过滤器, * 表示不过滤
        //consumer.subscribe("test-topic", "*");
        consumer.subscribe("test-topic", "insert");
        // 注册消息监听器
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                for (MessageExt messageExt : list) {
                    System.out.println(new String(messageExt.getBody()));
                }
                // 默认情况下 这条消息只会被一个Consumer消费(点对点)
                // message 状态修改(通知broker消息消费状态)
                // ack确认机制
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        // 设置消费模式 MessageModel.CLUSTERING 集群模式(默认的模式)  MessageModel.BROADCASTING 广播模式
        consumer.setMessageModel(MessageModel.BROADCASTING);

        // 启动消费者
        consumer.start();
        System.out.println("consumer02 start...");
    }
}
