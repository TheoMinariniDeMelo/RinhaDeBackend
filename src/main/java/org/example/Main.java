package org.example;

import com.rabbitmq.client.*;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.RoutingHandler;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;

public class Main {
    public static void main(String[] args) {
        RoutingHandler routingHandlerPeople = new RoutingHandler().get("/people", new HttpHandlerManagerPeoplesRegister());
        Undertow server = Undertow.builder()
                .addHttpListener(8080, "localhost")
                .setHandler(routingHandlerPeople)
                .build();
        server.start();
    }

    public String[] getJson(String json) {
        return null;
    }

    public static java.sql.Connection driverManager() throws SQLException {
        return DriverManager.getConnection(
                "postgresql:http://localhost:5432/Rinha",
                "root",
                "root"
        );

    }

    public static class HttpHandlerManagerPeoplesRegister implements HttpHandler {
        @Override
        public void handleRequest(HttpServerExchange exchange) throws Exception {
            ConnectionFactory connectionFactory = new ConnectionFactory();
            connectionFactory.setHost("localhost");
            connectionFactory.setPort(5672);
            connectionFactory.setUsername("root");
            connectionFactory.setPassword("root");
            try (Connection connection = connectionFactory.newConnection()) {
                Channel channel = connection.createChannel();
                String name = "rinha";
                String mensagem = "Esta é a minha mensagem";

                // Publica a mensagem na fila 'rinha'
                channel.basicPublish("", name, null, mensagem.getBytes());
                channel.queueDeclare(name, false, false, false, null);
                channel.basicConsume(name, defaultConsumer(), consumerTag -> {
                });

            } catch (Exception ioException) {
                System.out.println("------erro------");
            }
        }

        public static DeliverCallback defaultConsumer() {
            return (consumerTag, message) -> {
                String body = Arrays.toString(message.getBody());
                try {
                    java.sql.Connection connection = driverManager();
                    connection.beginRequest();
                    PreparedStatement stm = connection.prepareStatement(
                            """ 
                                    CREATE TABLE (
                                           id SERIAL PRIMARY_KEY
                                    );
                                    """.trim()
                    );
                    connection.commit();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
                System.out.println("hehe");
            };
        }
    }
}