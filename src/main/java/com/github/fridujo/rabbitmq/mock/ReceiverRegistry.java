package com.github.fridujo.rabbitmq.mock;

import java.util.Optional;

interface ReceiverRegistry {

    Optional<Receiver> getReceiver(ReceiverPointer receiverPointer);
}
