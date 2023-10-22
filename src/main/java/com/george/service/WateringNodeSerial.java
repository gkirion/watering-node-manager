package com.george.service;

import com.fazecast.jSerialComm.SerialPort;
import com.george.exception.WateringNodeException;
import com.george.model.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
public class WateringNodeSerial implements WateringCommandReceiver {

    private static final Logger LOGGER = LoggerFactory.getLogger(WateringNodeSerial.class);

    private BufferedReader bufferedReader;

    private BufferedWriter bufferedWriter;

    private String port;

    private int maxNumberOfAttempts;

    private WateringQueue wateringQueue;

    public WateringNodeSerial(@Value("${serial.port}") String port, @Value("${serial.maxNumberOfAttempts:3}") int maxNumberOfAttempts, WateringQueue wateringQueue) throws WateringNodeException {
        LOGGER.info("port: {}", port);
        SerialPort serialPort = SerialPort.getCommPort(port);
        LOGGER.info("connecting to: {}", serialPort);
        serialPort.setComPortParameters(9600, 8, SerialPort.ONE_STOP_BIT, SerialPort.NO_PARITY);
        serialPort.setComPortTimeouts(SerialPort.TIMEOUT_READ_BLOCKING, 200, 0);
        boolean connected;
        int attemptNumber = 0;

        do {

            attemptNumber++;
            LOGGER.info("connection attempt: {}, max number of attempts: {}", attemptNumber, maxNumberOfAttempts);
            connected = serialPort.openPort();

        } while (!connected && attemptNumber < maxNumberOfAttempts);

        if (!connected) {
            throw new RuntimeException("could not connect to " + serialPort);
        }
        LOGGER.info("connected to: {}", serialPort);

        InputStreamReader inputStreamReader = new InputStreamReader(serialPort.getInputStream());
        OutputStreamWriter outputStreamWriter = new OutputStreamWriter(serialPort.getOutputStream());
        bufferedReader = new BufferedReader(inputStreamReader);
        bufferedWriter = new BufferedWriter((outputStreamWriter));

        this.wateringQueue = wateringQueue;
        wateringQueue.setWateringCommandReceiver(this);

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> {
            try {
                readSerial();
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
        });
    }

    private void setWateringStatus(Command command) throws WateringNodeException {

        try {
            bufferedWriter.write(command + System.lineSeparator());
            bufferedWriter.flush();
        } catch (IOException e) {
            throw new WateringNodeException(e);
        }

    }

    private void readSerial() throws Exception {
        String input;
        while (true) {
            if (bufferedReader.ready()) {
                input = bufferedReader.readLine();
                LOGGER.info("{}", input);

                try {
                    Integer currentStopIndex = Integer.parseInt(input);
                    LOGGER.info("current stop index: {}", currentStopIndex);
                    wateringQueue.sendWateringStatus(currentStopIndex);

                } catch (NumberFormatException e) {
                    LOGGER.warn("exception: {}", e.getMessage());
                    e.printStackTrace();
                }

            } else {
                Thread.sleep(10);
            }
        }
    }

    @Override
    public void receiveCommand(Command command) {
        try {
            setWateringStatus(command);
        } catch (WateringNodeException e) {
            e.printStackTrace();
        }
    }

}
