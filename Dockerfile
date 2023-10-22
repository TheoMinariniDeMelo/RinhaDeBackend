FROM amazoncorretto:17

# Diretório de trabalho dentro do contêiner
WORKDIR /opt/app

# Copiar o arquivo JAR do projeto para o contêiner
COPY /build/libs/Stackoverflow-1.0-SNAPSHOT-all.jar /opt/app/application.jar
EXPOSE 8080
# Comando para executar o aplicativo Java
CMD ["java", "-jar","application.jar"]