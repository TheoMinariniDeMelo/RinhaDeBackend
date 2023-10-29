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

import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Main {

    public static List<Object> jsonParsed;

    public static void main(String[] args) {
        System.out.println("hehehe");
        RoutingHandler routingHandlerPeople = new RoutingHandler();
        routingHandlerPeople.add("POST", "/people", new HttpHandlerManagerPeoplesRegister());
        routingHandlerPeople.add("GET", "/people", new httpHandlerGetUser());

        Undertow server = Undertow.builder()
                .addHttpListener(8080, "0.0.0.0")
                .setHandler(routingHandlerPeople)
                .build();
        server.start();
    }

    public static void setJsonParser(String json) throws JsonProcessingException {
        System.out.printf("chegou aqui");

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode node = objectMapper.readTree(json);
        List<Object> nodes = new ArrayList<>();
        nodes.add(node.get("nome").textValue());
        nodes.add(node.get("apelido").textValue());
        nodes.add(node.get("nascimento").textValue());
        if (node.has("stack") && node.get("stack").isArray()) {
            JsonNode stackNode = node.get("stack");
            String[] stackArray = objectMapper.treeToValue(stackNode, String[].class);
            nodes.add(stackArray);
        }
        jsonParsed = nodes;
        System.gc();
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
        return DriverManager.getConnection("jdbc:postgresql://db:5432/Rinha", "rinha", "rinha");
    }

    public static boolean usernameVerifiedAlreadyExists(String nome) throws SQLException {
        try (java.sql.Connection connection = connection()) {
            String sql = "SELECT EXISTS(SELECT 1 FROM users WHERE nome = ?)";
            try (PreparedStatement stm = connection.prepareStatement(sql)) {
                stm.setString(1, nome);
                try (ResultSet rs = stm.executeQuery()) {
                    if (rs.next()) return rs.getBoolean(1);
                }
            }
        }
        System.gc();
        return false;
    }

    public static class HttpHandlerManagerPeoplesRegister implements HttpHandler {
        @Override
        public void handleRequest(HttpServerExchange exchange) {
            exchange.getRequestReceiver().receiveFullString(
                    (exchange1, message) -> {
                        try {
                            setJsonParser(message);
                        } catch (JsonProcessingException ignored) {
                        }
                        ;
                    }
            );
            final var erros = verifiedJson();

            if (!erros.isEmpty()) exchange.getResponseSender().send(Arrays.toString(erros.toArray()));

            ConnectionFactory connectionFactory = new ConnectionFactory();
            connectionFactory.setHost("rabbitMQ");
            connectionFactory.setPort(5672);
            connectionFactory.setUsername("root");
            connectionFactory.setPassword("root");
            try (Connection connection = connectionFactory.newConnection()) {

                Channel channel = connection.createChannel();
                String queue = "rinha";
                String mensagem = jsonParsed.stream().map((x) ->
                        {
                            if (x.getClass().isArray()) {
                                String[] value = (String[]) x;
                                StringBuilder values = new StringBuilder();
                                for (int i = 0; i < value.length; i++) {
                                    if (i == value.length - 1) {
                                        values.append(value[i]);
                                    } else {
                                        values.append(value[i]).append(" | ");
                                    }
                                }
                                return values;
                            }
                            return x + " | ";
                        }
                ).collect(Collectors.joining());
                UUID id = UUID.randomUUID();
                mensagem = id + " | " + mensagem;
                channel.basicPublish("", queue, null, mensagem.getBytes());
                channel.queueDeclare(queue, false, false, false, null);
                channel.basicConsume(queue, defaultConsumer(), consumerTag -> {
                });
                exchange.getResponseSender().send(String.valueOf(id));

            } catch (Exception ignored) {
                throw new RuntimeException();
            }
            System.gc();

        }
    }

    public static class httpHandlerGetUser implements HttpHandler {
        private static class PeopleDTO {
            String uuid;
            String nome;
            String apelido;
            Date nascimento;

            public String getUuid() {
                return uuid;
            }

            public String getNome() {
                return nome;
            }

            public String getApelido() {
                return apelido;
            }

            public Date getNascimento() {
                return nascimento;
            }

        }

        @Override
        public void handleRequest(HttpServerExchange exchange) throws Exception {

            Deque<String> params = exchange.getQueryParameters().get("t");

            try (java.sql.Connection connection = connection()) {
                connection.beginRequest();
                List<PeopleDTO> peoples = new ArrayList<>();
                String searchString = "%" + params.getFirst() + "%";
                PreparedStatement statement = connection.prepareStatement("SELECT * FROM users WHERE agregado LIKE ?");
                statement.setString(1, searchString);

                ResultSet resultSet = statement.executeQuery();
                while (resultSet.next()) {
                    final var people = new PeopleDTO();
                    people.uuid = resultSet.getString(1);
                    people.nome = resultSet.getString(2);
                    people.apelido = resultSet.getString(3);
                    people.nascimento = resultSet.getDate(4);
                    peoples.add(people);
                    ;
                }

                statement.close();
                exchange.getResponseSender().send(
                        peoples.isEmpty() ? null : new ObjectMapper().writeValueAsString(peoples)
                );
            } catch (SQLException e) {
                throw new RuntimeException();
            }
        }
    }

    public static DeliverCallback defaultConsumer() {
        return (consumerTag, message) -> {
            String[] body = new String(message.getBody(), StandardCharsets.UTF_8).split("\\|");
            try (java.sql.Connection connection = connection()) {
                System.out.println("heheheheeh chegou aqui");

                connection.setAutoCommit(false);
                PreparedStatement stm = connection.prepareStatement(
                        " INSERT INTO users (id, nome, apelido, nascimento, stack, agregado) VALUES(?, ?, ?, ?, ?, ?)"
                );

                stm.setString(1, body[0]);
                stm.setString(2, body[1]);
                stm.setString(3, body[2]);
                stm.setDate(4, Date.valueOf(body[3].replaceAll(" ", "")));

                Array stack = connection.createArrayOf("text", Arrays.copyOfRange(body, 4, body.length));
                stm.setArray(5, stack);

                stm.setString(6, String.join(" | ", body));
                stm.executeUpdate();
                connection.commit();
                stm.close();
            } catch (SQLException ignored) {
                throw new RuntimeException();
            }
        };
    }
}