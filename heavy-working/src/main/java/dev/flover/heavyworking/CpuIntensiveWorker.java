package dev.flover.heavyworking;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.time.ZoneId.systemDefault;

@Slf4j
@Component
public class CpuIntensiveWorker {

    private final Connection connection;

    public CpuIntensiveWorker(Connection connection) {
        this.connection = connection;
    }

    private Channel channel;

    synchronized public void openChannel() throws IOException {
        if (channel == null) {
            this.channel = connection.createChannel();
        }
    }

    public void work() throws IOException {
        String appId = ManagementFactory.getRuntimeMXBean().getName();
        var queueName = Constants.QUEUE_RPC_WORKER_PREFIX + appId;

        // 임시 큐 선언
        channel.queueDeclare(queueName, false, true, true, null);
        channel.queueBind(queueName, Constants.EXCHANGE_REQUEST, Constants.DETECT_BIND_KEY);


        channel.basicConsume(queueName, true, (consumerTag, message) -> {
            log.info("processing very long time working");
            var contentType = message.getProperties().getContentType();
            var content = new String(message.getBody());
            var bindKey = message.getProperties().getHeaders().get("response-bind-key").toString();

            var receivedTime = message.getProperties().getTimestamp();
            var receivedAt = LocalDateTime.ofInstant(Instant.ofEpochMilli(receivedTime.getTime()), systemDefault());
            var now = LocalDateTime.now();
            var sec = Duration.between(now, receivedAt).abs().getSeconds();

            log.info("received rpc request published {} seconds ago.", sec);
            log.info("received content: [{}]", content);

            var replyMessage = "processing ok! content[" + content + "]";
            try {
                TimeUnit.SECONDS.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                channel.basicCancel(consumerTag);
                return;
            }

            var replyTo = message.getProperties().getReplyTo();
            var prop = new AMQP.BasicProperties.Builder()
                    .appId("Pub Sub 2 Consumer")
                    .contentType(contentType)
                    .replyTo(replyTo)
                    .build();

            channel.basicPublish(Constants.EXCHANGE_REPLIES,
                    bindKey,
                    prop,
                    replyMessage.getBytes());
        }, (consumerTag, sig) -> {});
    }

    public void closeChannel() throws IOException, TimeoutException {
        this.channel.close();
    }
}
