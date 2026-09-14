package com.github.fridujo.rabbitmq.mock;

import java.util.Optional;

public interface ReceiverRegistry {

    Optional<Receiver> getReceiver(ReceiverPointer receiverPointer);

    boolean removeReceiver(ReceiverPointer receiverPointer);

    default boolean removeReceiver(Receiver receiver) {
        return removeReceiver(receiver.pointer());
    }
}
