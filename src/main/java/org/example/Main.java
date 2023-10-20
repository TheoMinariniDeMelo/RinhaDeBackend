package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;
import com.rabbitmq.client.Connection;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.RoutingHandler;

import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.regex.Pattern;

public class Main {
    public static List<Object> jsonParsed;

    public static void main(String[] args) {
        RoutingHandler routingHandlerPeople = new RoutingHandler()
                .post("/people", new HttpHandlerManagerPeoplesRegister());
        RoutingHandler routingHandlerGetPeople = new RoutingHandler()
                .get("/people/get", new httpHandlerGetUser());
        Undertow server = Undertow.builder()
                .addHttpListener(8080, "localhost")
                .setHandler(routingHandlerPeople)
                .setHandler(routingHandlerGetPeople)
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

    public static List<String> verifiedJson() {
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

    public static java.sql.Connection connection() throws SQLException {
        return DriverManager.getConnection("postgresql://localhost:5432/Rinha", "rinha", "rinha");
    }

    public static boolean usernameVerifiedAlreadyExists(String nome) throws SQLException {
        try (java.sql.Connection connection = connection()) {
            String sql = "SELECT EXISTS(SELECT 1 FROM Users WHERE nome = ?)";
            try (PreparedStatement stm = connection.prepareStatement(sql)) {
                stm.setString(1, nome);
                try (ResultSet rs = stm.executeQuery()) {

                    if (rs.next()) return rs.getBoolean(1);
                }

            }
        }

        return false;

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
            final var erros = verifiedJson();

            if (!erros.isEmpty()) exchange.getResponseSender().send(Arrays.toString(erros.toArray()));

            ConnectionFactory connectionFactory = new ConnectionFactory();
            connectionFactory.setHost("localhost");
            connectionFactory.setPort(5672);
            connectionFactory.setUsername("root");
            connectionFactory.setPassword("root");
            try (Connection connection = connectionFactory.newConnection()) {

                Channel channel = connection.createChannel();
                String queue = "rinha";
                String mensagem = jsonParsed.stream().map((x) ->
                        {
                            if (x.getClass().isArray()) {
                                StringBuilder values = new StringBuilder();
                                for (int i = 0; i < ((String[]) x).length; i++) values.append(x).append(" | ");
                                return values;
                            }
                            return x + " | ";
                        }
                ).toString();
                UUID id = UUID.randomUUID();
                mensagem = id + " | " + mensagem;
                channel.basicPublish("", queue, null, mensagem.getBytes());
                channel.queueDeclare(queue, false, false, false, null);
                channel.basicConsume(queue, defaultConsumer(), consumerTag -> {
                });
                exchange.getResponseSender().send(String.valueOf(id));

            } catch (Exception ioException) {
                throw new Exception("Not possible connect in Message broker");
            }

        }
    }

    public static class httpHandlerGetUser implements HttpHandler {
        private static class PeopleDTO {
            UUID uuid;
            String nome;
            String apelido;
            Date nascimento;
        }

        @Override
        public void handleRequest(HttpServerExchange exchange) throws Exception {

            Map<String, Deque<String>> params = exchange.getQueryParameters();

            try (java.sql.Connection connection = connection()) {
                connection.beginRequest();
                List<PeopleDTO> peoples = new ArrayList<>();
                PreparedStatement statement = connection.prepareStatement("SELECT * FROM Users WHERE agregado LIKE %?%");
                statement.setString(0, params.get("t").getFirst());
                ResultSet resultSet = statement.executeQuery();
                while (resultSet.next()) {
                    final var people = new PeopleDTO();
                    people.uuid = UUID.fromString(resultSet.getString(1));
                    people.nome = resultSet.getString(2);
                    people.apelido = resultSet.getString(3);
                    people.nascimento = resultSet.getDate(4);
                    peoples.add(people);
                    ;
                }
                statement.close();
                exchange.getResponseSender().send(
                        new ObjectMapper().writeValueAsString(peoples)
                );
            } catch (SQLException e) {
                throw new RuntimeException();
            }
            ;
        }
    }

    public static DeliverCallback defaultConsumer() {
        return (consumerTag, message) -> {
            String[] body = Arrays.toString(message.getBody()).split("[|]");

            Array stack = Array.class.cast(Arrays.stream(body).skip(4).toArray());

            try (java.sql.Connection connection = connection()) {
                final PreparedStatement stm = connection.prepareStatement(
                        """
                                INSERT INTO Users (id,nome,apelido, nascimento, stack, agregado) VALUES(?,?,?,?,?,?)
                                """);

                stm.setString(1, body[0]);
                stm.setString(2, body[1]);
                stm.setString(3, body[2]);
                stm.setDate(4, Date.valueOf(body[3]));
                stm.setArray(5, stack);
                stm.setString(6, Arrays.toString(body));
                stm.close();
            } catch (SQLException e) {
                throw new RuntimeException("Not Possible persistence User");
            }
        };
    }
}