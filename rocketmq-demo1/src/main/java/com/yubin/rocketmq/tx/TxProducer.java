package com.yubin.rocketmq.tx;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * 事务消息
 *
 * @author YUBIN
 * @create 2020-10-07
 */
public class TxProducer {

    public static void main(String[] args) throws MQClientException {
        // 创建事务消息生产者对象
        TransactionMQProducer producer = new TransactionMQProducer("txProducerGroup01");
        // 设置生产者的nameserver
        producer.setNamesrvAddr("192.168.254.4:9876");
        // 设置事务消息的回调函数
        producer.setTransactionListener(new TransactionListener() {
            // 执行本地事务
            public LocalTransactionState executeLocalTransaction(Message message, Object obj) {
                System.out.println("==================executeLocalTransaction==================");
                System.out.println("msg:" + new String(message.getBody()));
                System.out.println("msg txId:" + message.getTransactionId());
                System.out.println("obj:" + obj);

                /**
                 * 事务相关业务方法
                 * a()
                 * b()
                 * c()
                 */

                // 表示事务执行成功
                // return LocalTransactionState.COMMIT_MESSAGE;
                // 回滚消息(回滚的是Half Message)
                //return LocalTransactionState.ROLLBACK_MESSAGE;
                // 等会儿,目前还不知道事务状态
                 return LocalTransactionState.UNKNOW;
            }

            /**
             * Broker端回调,检查事务
             *
             * @param messageExt (发送时的消息)
             * @return
             */
            public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {

                System.out.println("==================checkLocalTransaction==================");
                System.out.println("messageExt:" + new String(messageExt.getBody()));
                System.out.println("messageExt txId:" + messageExt.getTransactionId());

                // 表示事务执行成功
                // return LocalTransactionState.COMMIT_MESSAGE;
                // 回滚消息(回滚的是Half Message)
                //return LocalTransactionState.ROLLBACK_MESSAGE;
                // 等会儿,目前还不知道事务状态
                 return LocalTransactionState.UNKNOW;
            }
        });
        // 开启生产者
        producer.start();
        // 发送事务消息
        TransactionSendResult sendResult = producer.sendMessageInTransaction(new Message("testTx01", "测试事务消息".getBytes()), null);

        System.out.println("sendResult:" + sendResult);

        // 关闭生产者
        //producer.shutdown();
        //System.out.println("生产者消息发送完毕,退出服务");
    }
}
