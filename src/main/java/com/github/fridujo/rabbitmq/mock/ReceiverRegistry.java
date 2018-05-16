package com.github.fridujo.rabbitmq.mock;

interface ReceiverRegistry {

    Receiver getReceiver(ReceiverPointer receiverPointer);
}
