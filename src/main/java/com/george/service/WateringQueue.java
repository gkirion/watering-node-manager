package com.george.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.george.model.Command;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

@Service
public class WateringQueue {

    private static final Logger LOGGER = LoggerFactory.getLogger(WateringQueue.class);

    @Value("${rabbitmq-host:localhost}")
    private String rabbitMQHost;

    private static final String WATERING_QUEUE_NAME = "watering-queue";

    private static final String COMMAND_QUEUE_NAME = "command-queue";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private Channel channel;

    private Optional<WateringCommandReceiver> wateringCommandReceiver = Optional.empty();

    @PostConstruct
    private void init() throws IOException, TimeoutException {
        LOGGER.info("host ip: {}", InetAddress.getLocalHost());

        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(rabbitMQHost);
        Connection connection = connectionFactory.newConnection();
        channel = connection.createChannel();

        LOGGER.info("declaring queue {}", WATERING_QUEUE_NAME);
        AMQP.Queue.DeclareOk declareOk = channel.queueDeclare(WATERING_QUEUE_NAME, false, false, false, null);
        LOGGER.info("declared queue {}", declareOk);

        LOGGER.info("declaring queue {}", COMMAND_QUEUE_NAME);
        declareOk = channel.queueDeclare(COMMAND_QUEUE_NAME, false, false, false, null);
        LOGGER.info("declared queue {}", declareOk);

        channel.basicConsume(declareOk.getQueue(), true, (consumerTag, delivery) -> {

            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            LOGGER.info("consumerTag: {}", consumerTag);
            LOGGER.info("message: {}", message);
            Command command = OBJECT_MAPPER.readValue(message, Command.class);
            if (wateringCommandReceiver.isPresent()) {
                wateringCommandReceiver.get().receiveCommand(command);
            }

        }, consumerTag -> { LOGGER.info("consumer shutdown"); });

    }

    public void setWateringCommandReceiver(WateringCommandReceiver wateringCommandReceiver) {
        this.wateringCommandReceiver = Optional.of(wateringCommandReceiver);
    }

    public void sendWateringStatus(Integer currentStopIndex) throws IOException {
        channel.basicPublish("", WATERING_QUEUE_NAME, null, OBJECT_MAPPER.writeValueAsBytes(currentStopIndex));
    }


}
