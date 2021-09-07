package com.github.fridujo.rabbitmq.mock.tool;

import com.github.fridujo.rabbitmq.mock.ReceiverPointer;

import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

public class ParameterMarshaller {

    public static BiFunction<String, Map<String, Object>, Optional<String>> getParameterAsString =
        (k, m) -> Optional.ofNullable(m.get(k))
            .filter(aeObject -> aeObject instanceof String)
            .map(String.class::cast);

    public static BiFunction<String, Map<String, Object>, Optional<Integer>> getParameterAsPositiveInteger =
        (k, m) -> Optional.ofNullable(m.get(k))
            .filter(aeObject -> aeObject instanceof Number)
            .map(Number.class::cast)
            .map(num -> num.intValue())
            .filter(i -> i > 0);

    public static BiFunction<String, Map<String, Object>, Optional<Short>> getParameterAsPositiveShort =
        (k, m) -> Optional.ofNullable(m.get(k))
            .filter(aeObject -> aeObject instanceof Number)
            .map(Number.class::cast)
            .map(num -> num.intValue())
            .filter(i -> i > 0)
            .filter(i -> i < 256)
            .map(Integer::shortValue);

    public static BiFunction<String, Map<String, Object>, Optional<Long>> getParameterAsPositiveLong =
        (k, m) -> Optional.ofNullable(m.get(k))
            .filter(aeObject -> aeObject instanceof Number)
            .map(Number.class::cast)
            .map(number -> number.longValue());

    public static BiFunction<String, Map<String, Object>, Optional<ReceiverPointer>> getParameterAsExchangePointer =
        (k, m) -> Optional.ofNullable(m.get(k))
            .filter(aeObject -> aeObject instanceof String)
            .map(String.class::cast)
            .map(aeName -> new ReceiverPointer(ReceiverPointer.Type.EXCHANGE, aeName));

}

