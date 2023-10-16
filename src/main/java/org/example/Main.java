package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.RoutingHandler;
import org.neo4j.driver.*;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        RoutingHandler routingHandlerPeople = new RoutingHandler()
                .get("/people", new HttpHandlerManagerPeoplesRegister());
        Undertow server = Undertow.builder()
                .addHttpListener(8080, "localhost")
                .setHandler(routingHandlerPeople)
                .build();
        server.start();
    }

    public List<Object> getJson(String json) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode node = objectMapper.readTree(json);
        List<Object> nodes = new ArrayList<>();
        nodes.add(node.get("name").toString());
        nodes.add(node.get("apelido").toString());
        nodes.add(node.get("nascimento").toString());
        if (node.get("stack").isArray()) {
            // Converte o JsonNode em um array de inteiros
            nodes.add(objectMapper.treeToValue(node.get("stack"), String[].class));
        }
        return nodes;
    }

    public static Driver driverManager() throws SQLException {
        return GraphDatabase.driver("localhost:7673",
                AuthTokens.basic("neo4j", "neo4j"));
    }

    public static class run1 implements Runnable {

        @Override
        public void run() {
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
    }

    public static class HttpHandlerManagerPeoplesRegister implements HttpHandler {
        @Override
        public void handleRequest(HttpServerExchange exchange) throws Exception {
            new run1().run();
        }


    }

    public static class runPersist implements Runnable {

        @Override
        public void run() {
            Driver driver = null;
            try {
                driver = driverManager();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            Session session = driver.session();
            session.beginTransaction();
            session.run("CREATE (n{name:''})");
            session.close();
        }
    }

    public static DeliverCallback defaultConsumer() {
        return (consumerTag, message) -> {
            String body = Arrays.toString(message.getBody());
            new runPersist().run();
            System.out.println("hehe");
        };
    }
}