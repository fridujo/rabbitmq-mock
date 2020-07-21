package com.github.fridujo.rabbitmq.mock.exchange;

import java.util.Map;

import com.github.fridujo.rabbitmq.mock.AmqArguments;
import com.github.fridujo.rabbitmq.mock.ReceiverRegistry;

public class MockTopicExchange extends MultipleReceiverExchange {

    public static final String TYPE = "topic";

    public MockTopicExchange(String name, AmqArguments arguments, ReceiverRegistry receiverRegistry) {
        super(name, TYPE, arguments, receiverRegistry);
    }

    /**
     * <a href="https://www.rabbitmq.com/tutorials/tutorial-five-python.html">
     * https://www.rabbitmq.com/tutorials/tutorial-five-python.html</a>
     *
     * <p>sharp / hash character substitutes to <b>zero</b> or more words</p>
     * <p>An easy thought representation of routing keys is a list of words, words being separated by dots,
     * and topic exchange binding keys an description of this kind of list.</p>
     * <p>Considering the key <b>some.#.key.*</b>, all these keys can match:</p>
     * <ul>
     *     <li><b>some.key.redeyes</b> where # matches for no words and * for 1 word (redeyes)</li>
     *     <li><b>some.pink.key.blueeyes</b> where # matches for 1 words (pink) and * for 1 word (blueeyes)</li>
     *     <li><b>some.pink.rabbit.key.random</b> where # matches for 2 words (pink, rabbit) and * for 1 word (random)</li>
     * </ul>
     */
    protected boolean match(BindConfiguration bindConfiguration, String routingKey, Map<String, Object> headers) {
        String bindingRegex = bindConfiguration.bindingKey
            .replace("*", "([^\\.]+)")
            .replace(".#", "(\\.(.*))?")
            .replace("#", "(.+)");
        return routingKey.matches(bindingRegex);
    }
}
