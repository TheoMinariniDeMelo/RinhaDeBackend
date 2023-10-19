package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.RoutingHandler;

import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class Main {
    public static List<Object> jsonParsed;

    public static void main(String[] args) {
        RoutingHandler routingHandlerPeople = new RoutingHandler()
                .post("/people", new HttpHandlerManagerPeoplesRegister());
        Undertow server = Undertow.builder()
                .addHttpListener(8080, "localhost")
                .setHandler(routingHandlerPeople)
                .build();
        server.start();
    }

    public static void setJsonParser(String json) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode node = objectMapper.readTree(json);
        List<Object> nodes = new ArrayList<>();
        nodes.add(node.get("nome").toString());
        nodes.add(node.get("apelido").toString());
        nodes.add(node.get("nascimento").toString());
        if (node.get("stack").isArray()) {
            nodes.add(objectMapper.treeToValue(node.get("stack"), String[].class));
        }
        jsonParsed = nodes;
    }

    public List<String> verifiedJson() {
        final List<String> errors = new ArrayList<>();
        jsonParsed.forEach(node -> {
            if (node.getClass().isArray()) {
                Arrays.stream(((String[]) node)).forEach(item -> {
                    if (Pattern.matches(".*\\|.*", item)) errors.add("Error in: " + item);
                });
                return;
            }
            if (Pattern.matches(".*\\|.*", node.toString())) errors.add("Error in: " + node);
        });
        return errors;
    }

    public static class HttpHandlerManagerPeoplesRegister implements HttpHandler {
        @Override
        public void handleRequest(HttpServerExchange exchange) throws Exception {
            exchange.getRequestReceiver().receiveFullString(
                    (exchange1, message) -> {
                        try {
                            setJsonParser(message);
                        } catch (JsonProcessingException e) {
                            throw new RuntimeException(e);
                        }
                        ;
                    }
            );
            ConnectionFactory connectionFactory = new ConnectionFactory();
            connectionFactory.setHost("localhost");
            connectionFactory.setPort(5672);
            connectionFactory.setUsername("root");
            connectionFactory.setPassword("root");
            try (Connection connection = connectionFactory.newConnection()) {

                Channel channel = connection.createChannel();
                String name = "rinha";
                String mensagem = jsonParsed.stream().map((x) ->
                        {
                            if (x.getClass().isArray()) {
                                StringBuilder values = new StringBuilder();
                                for (int i = 0; i < ((String[]) x).length; i++) values.append(x).append(" | ");
                                return values;
                            }
                            return x.toString();
                        }
                ).toString();

                channel.basicPublish("", name, null, mensagem.getBytes());
                channel.queueDeclare(name, false, false, false, null);
                channel.basicConsume(name, defaultConsumer(), consumerTag -> {

                });

            } catch (Exception ioException) {
                throw new Exception("Not possible connect in Message broker");
            }
        }
    }

    @SuppressWarnings("SQLException")
    public static DeliverCallback defaultConsumer() {
        return (consumerTag, message) -> {
            String[] body = Arrays.toString(message.getBody()).split("[|]");
            try {
                java.sql.Connection connection = DriverManager.getConnection("postgresql://localhost:5432/Rinha", "postgres", "postgres");
                final PreparedStatement stm;
                stm = connection.prepareStatement("""
                        INSERT INTO Users (id,nome,apelido, nascimento, stack) VALUES(?,?,?,?)
                        """);
                stm.setString(0, body[0]);
                stm.setString(1, body[1]);
                stm.setString(2, body[2]);
                stm.setDate(3, Date.valueOf(body[2]));
                stm.setDate(4, Date.valueOf(body[3]));
                stm.execute();
                stm.close();
            } catch (SQLException e) {
                throw new RuntimeException("Not Possible persistence User");
            }
        };
    }
}