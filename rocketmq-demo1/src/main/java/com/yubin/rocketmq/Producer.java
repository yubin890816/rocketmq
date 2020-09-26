package com.yubin.rocketmq;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.ArrayList;
import java.util.List;

/**
 * 消息生产者
 *
 * @author YUBIN
 * @create 2020-09-26
 */
public class Producer {

    public static void main(String[] args) throws Exception {
        //test1();
        //test2();
        //test3();
        //test4();
        test5();
    }

    // 同步发送消息
    private static void test1() throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        // 创建消息生产者（参数:生产者组）
        DefaultMQProducer producer = new DefaultMQProducer("test-group");
        // 设置nameserver地址
        producer.setNamesrvAddr("192.168.254.4:9876");
        producer.start();
        // topic:消息将要发送到的地址(如果topic不存在,会自动帮你创建)
        // body:消息中的具体数据
        Message msg = new Message("test-topic", "第一条消息".getBytes());
        // 同步消息发送(等待broker发送确认收到的消息)
        SendResult sendResult = producer.send(msg);
        // broker返回的接收结果(即生产者发送消息的结果)
        System.out.println("sendResult:" + sendResult);
        producer.shutdown();
        System.out.println("生产者退出");
    }

    // 批量发送消息
    private static void test2() throws Exception {
        // 创建消息生产者,指定生产者组
        DefaultMQProducer producer = new DefaultMQProducer("test-group");
        // 设置生产者的nameserver
        producer.setNamesrvAddr("192.168.254.4:9876");
        // 开启生产者
        producer.start();
        // 定义要发送的消息列表
        List<Message> list = new ArrayList<Message>();
        // 如果topic不存在会自动创建
        Message msg1 = new Message("test-topic", "第1条消息".getBytes());
        Message msg2 = new Message("test-topic", "第2条消息".getBytes());
        Message msg3 = new Message("test-topic", "第3条消息".getBytes());
        list.add(msg1);
        list.add(msg2);
        list.add(msg3);
        // 同步批量发送消息(官方建议批量发送消息不要超过1M)
        SendResult sendResult = producer.send(list);
        System.out.println(sendResult);

        // 关闭生产者
        producer.shutdown();
        System.out.println("生产者消息发送完毕,退出服务");
    }

    // 异步可靠消息
    private static void test3() throws Exception {
        // 创建消息生产者,指定生产者组
        DefaultMQProducer producer = new DefaultMQProducer("test-group");
        // 设置生产者的nameserver
        producer.setNamesrvAddr("192.168.254.4:9876");
        // 开启生产者
        producer.start();
        // 定义要发送的消息
        // 如果topic不存在会自动创建
        Message msg = new Message("test-topic", "第1条消息".getBytes());
        // 设置消息发送失败重试次数
        //producer.setRetryTimesWhenSendFailed(3);
        // 发送异步可靠消息
        producer.send(msg, new SendCallback() {
            public void onSuccess(SendResult sendResult) {
                System.out.println("消息发送成功,sendResult:" + sendResult);
            }

            public void onException(Throwable throwable) {
                // 如果发生异常, case异常,尝试重投
                System.out.println("消息发送失败,Exception:" + ExceptionUtils.getStackTrace(throwable));
            }
        });

        // 关闭生产者(当异步的时候,这里不能关闭,会造成Producer已经关闭,但是消息还没有发送出去)
        //producer.shutdown();
        System.out.println("生产者消息发送完毕,退出服务");
    }

    // 单向消息 只发送消息，不等待服务器响应，只发送请求不等待应答。此方式发送消息的过程耗时非常短，一般在微秒级别。
    private static void test4() throws Exception {
        // 创建消息生产者,指定生产者组
        DefaultMQProducer producer = new DefaultMQProducer("test-group");
        // 设置生产者的nameserver
        producer.setNamesrvAddr("192.168.254.4:9876");
        // 开启生产者
        producer.start();
        // 定义要发送的消息
        // 如果topic不存在会自动创建
        Message msg = new Message("test-topic", "第1条消息".getBytes());
        // 发送单向消息
        producer.sendOneway(msg);

        // 关闭生产者
        producer.shutdown();
        System.out.println("生产者消息发送完毕,退出服务");
    }

    // tag 消息过滤
    private static void test5() throws Exception {
        // 创建消息生产者,指定生产者组
        DefaultMQProducer producer = new DefaultMQProducer("test-group");
        // 设置生产者的nameserver
        producer.setNamesrvAddr("192.168.254.4:9876");
        // 开启生产者
        producer.start();
        // 定义要发送的消息
        // 如果topic不存在会自动创建
        // tag 是用来过滤消息的(同一个业务,不同的操作如新增订单、更新订单、删除订单等可以使用同一个topic,不同的tag来标记)
        Message msg = new Message("test-topic","insert","业务主键", "第1条消息".getBytes());
        // 发送单向消息
        producer.sendOneway(msg);

        // 关闭生产者
        producer.shutdown();
        System.out.println("生产者消息发送完毕,退出服务");
    }


}
