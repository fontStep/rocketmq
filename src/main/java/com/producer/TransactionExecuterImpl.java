package com.producer;

import org.apache.rocketmq.client.producer.LocalTransactionExecuter;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.common.message.Message;

/**
 * @ClassName: TransactionExecuterImpl
 * @Description: 本地事务处理机制
 * @version: v1.0.0
 * @author: wjw
 * @date: 2018/7/21 21:56
 */
public class TransactionExecuterImpl  implements LocalTransactionExecuter {

    public LocalTransactionState executeLocalTransactionBranch(Message msg, Object arg) {

        return LocalTransactionState.COMMIT_MESSAGE;
    }
}
