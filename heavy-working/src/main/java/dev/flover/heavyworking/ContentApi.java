package dev.flover.heavyworking;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.async.DeferredResult;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

@Slf4j
@RestController
public class ContentApi {

    private final Connection connection;

    public ContentApi(Connection connection) {
        this.connection = connection;
    }

    /**
     * maybe upload to very big size content
     */
    @PostMapping(path = "/api/v1/content")
    @ResponseBody
    public DeferredResult<Map<String, ?>> uploadContent(@RequestBody Map<String, String> body) throws IOException {
        String content = body.get("content");

        Channel channel = connection.createChannel();

        var responseQueueName = Constants.QUEUE_RESPONSE_PREFIX + ManagementFactory.getRuntimeMXBean().getName();
        // 오래 걸리는 작업이 끝나길 기다리는 Queue에 Direct Bind key
        var resBindKey = UUID.randomUUID().toString();

        // 오래 걸리는 작업이 끝나면 Message를 받을 큐 선언 및 binding
        // 현재 channel만 접근 및 channel이 끊기면 큐 자동 삭제
        channel.queueDeclare(responseQueueName, false, true, true, null);
        channel.queueBind(responseQueueName, Constants.EXCHANGE_REPLIES, resBindKey);

        DeferredResult<Map<String, ?>> result = new DeferredResult<>();
        Runnable closeCallback = () -> {
            try {
                channel.close();
            } catch (Exception e) {
                log.error("channel close error", e);
            }
        };
        result.onError(throwable -> closeCallback.run());
        result.onCompletion(closeCallback);

        // 오래걸리는 작업이 완료 되면 완료 Clallback을 받는다.
        channel.basicConsume(responseQueueName, (consumerTag, message) -> {
            var repliedMessage = new String(message.getBody());
            result.setResult(Map.of("result", repliedMessage));
        }, (consumerTag, sig) -> {
        });


        var prop = new AMQP.BasicProperties.Builder()
                .correlationId("this correlationId")
                .contentType("text/plain")
                .timestamp(new Date())
                .replyTo(responseQueueName)
                .deliveryMode(1)
                .headers(Map.of("response-bind-key", resBindKey))
                .build();

        channel.basicPublish(Constants.EXCHANGE_REQUEST, Constants.DETECT_BIND_KEY, prop, content.getBytes());

        return result;
    }
}
