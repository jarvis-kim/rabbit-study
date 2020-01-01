package dev.flover.heavyworking;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Connection;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

@SpringBootApplication
public class HeavyWorkingApplication {

    public static void main(String[] args) {
        SpringApplication.run(HeavyWorkingApplication.class, args);
    }

    private final Connection connection;
    private final CpuIntensiveWorker worker;

    public HeavyWorkingApplication(Connection connection, CpuIntensiveWorker worker) {
        this.connection = connection;
        this.worker = worker;
    }

    @EventListener(ApplicationStartedEvent.class)
    public void applicationStartEvent() throws IOException, TimeoutException {
        try (var channel = connection.createChannel()) {
            channel.exchangeDeclare(Constants.EXCHANGE_REQUEST, BuiltinExchangeType.DIRECT);
            channel.exchangeDeclare(Constants.EXCHANGE_REPLIES, BuiltinExchangeType.DIRECT);
        }

        worker.openChannel();
        worker.work();
    }

    @EventListener(ContextClosedEvent.class)
    public void applicationStopEvent() {
        if (worker != null) {
            try {
                worker.closeChannel();
            } catch (IOException e) {
            } catch (TimeoutException e) {
            }
        }

        try {
            connection.close();
        } catch (IOException e) {
        }
    }
}
