package com.github.fridujo.rabbitmq.mock;

import java.util.Optional;

public interface ReceiverRegistry {

    Optional<Receiver> getReceiver(ReceiverPointer receiverPointer);
}
