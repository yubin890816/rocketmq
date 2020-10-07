package com.yubin.rocketmq;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.List;

/**
 * 顺序发送消息
 *
 * @author YUBIN
 * @create 2020-10-07
 */
public class Producer2 {

    public static void main(String[] args) throws Exception {
        // 创建消息生产者对象,并指定生产者组
        DefaultMQProducer producer = new DefaultMQProducer("testProducerGroup002");
        // 设置生产者的nameserver
        producer.setNamesrvAddr("192.168.254.4:9876");
        // 开启生产者
        producer.start();
        for (int i = 0; i < 20; i++) {
            Message message = new Message("test002", ("hello-" + i).getBytes());
            /**
             * send(msg, selector, arg)
             * 参数1: 要发送的消息
             * 参数2: queue 选择器, 向Topic中的哪个Queue去写消息(默认一个Topic有4个Queue)
             *      MessageQueueSelector默认提供了3个实现类
             *          MessageQueueSelector：根据参数3的hash值选择具体的Queue
             *          SelectMessageQueueByRandom: 随机选择一个Queue
             *          SelectMessageQueueByMachineRoom: 根据机房选择Queue(该类是一个空方法,需要自己去实现)
             * 参数3: 自定义参数
             */
            //producer.send(message, new SelectMessageQueueByHash(), 1);
            //producer.send(message, new SelectMessageQueueByRandom(), 1);
            //producer.send(message, new SelectMessageQueueByMachineRoom(), 1);
            SendResult sendResult = producer.send(message, new MessageQueueSelector() {
                /**
                 * 手动选择一个Queue
                 * @param mqs 当前Topic里面包含的所有Queue
                 * @param msg 具体要发的那条消息
                 * @param arg 对应到send()里面的arg
                 * @return
                 */
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    return mqs.get((Integer) arg);
                }
            }, 1);
            System.out.println("sendResult:" + sendResult);
        }
        producer.shutdown();
        System.out.println("producer已经停机");
    }
}
