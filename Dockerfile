# ===================================================================
# Etapa 1: Compilación y Construcción (Build Stage)
# ===================================================================
FROM maven:3.9.6-eclipse-temurin-21-alpine AS builder
WORKDIR /app

# Copiar el POM y descargar dependencias (Aprovechamiento de caché de capas)
COPY pom.xml .
RUN mvn dependency:go-offline -B

# Copiar el código fuente y compilar el JAR omitiendo pruebas unitarias
COPY src ./src
RUN mvn clean package -DskipTests

# ===================================================================
# Etapa 2: Imagen de Ejecución Ligera (Runtime Stage)
# ===================================================================
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

# Crear un usuario no-root por seguridad
RUN addgroup -S appgroup && adduser -S appuser -G appgroup
USER appuser

# Copiar solo el artefacto JAR generado desde la etapa de compilación
COPY --from=builder /app/target/*.jar app.jar

# Exponer el puerto predeterminado del microservicio
EXPOSE 8084

# Variable de entorno por defecto
ENV PORT=8084

# Comando de inicio
ENTRYPOINT ["java", "-jar", "app.jar"]
