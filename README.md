# Pipeline de Streaming en Tiempo Real con Apache Beam y Apache Kafka

**Integrantes del Proyecto:**
* Pilar Ruiz Diaz
* Mathias Chaparro
* Tanya Godoy

---

## 1. Descripción del Proyecto

Este proyecto implementa una solución end-to-end de procesamiento de datos en streaming en tiempo real. La arquitectura ingiere eventos sintéticos en formato JSON hacia un tópico de entrada en **Apache Kafka**, realiza el procesamiento distribuido (parseo, temporización por ventanas fijas y deduplicación por clave de evento) con **Apache Beam**, y publica los eventos procesados de forma idempotente en un tópico de salida.

---

## 2. Arquitectura del Sistema

```text
[Productor Sintético]
       │
       ▼ (Eventos JSON)
[Kafka: eventos-entrada] ──(puerto 9092)──► [Apache Beam Pipeline]
       │                                            │
       ├─► Ventanas Fijas (10s)                     │
       ├─► Agrupamiento por event_id                │
       └─► Deduplicación (DeduplicateDoFn)          │
                                                    ▼
[Kafbat UI (puerto 8080)] ◄───────────── [Kafka: eventos-salida]

```
### Componentes principales:
- **Apache ZooKeeper**: Gestor de coordinación del clúster de Kafka en Docker.
- **Apache Kafka**: Plataforma de eventos distribuida ejecutándose en `localhost:9092`.
- **Kafbat UI**: Interfaz gráfica web de monitoreo para inspeccionar tópicos y mensajes (`http://localhost:8080`).
- **Apache Beam (Python SDK)**: Motor de procesamiento en streaming ejecutado con `DirectRunner`.
- **Confluent Kafka Python**: Cliente nativo para la comunicación de lectura y escritura con Kafka.

---

## 3. Estructura del Repositorio

```text
streaming-kafka-beam/
├── docker-compose.yml     # Configuración de servicios ZooKeeper, Kafka y Kafbat UI
├── requirements.txt       # Dependencias de Python
├── pipeline.py            # Código fuente principal del pipeline de streaming
├── test_pipeline.py       # Suite de pruebas unitarias automatizadas con pytest
├── eventos_prueba.txt     # Lote de eventos sintéticos JSON para pruebas de ingesta
└── README.md              # Documentación e instrucciones de ejecución
```

## 4. Requisitos Previos

* **Anaconda** o **Miniconda** (Python 3.10 instalado).
* **Docker Desktop** activo en el sistema.

---

## 5. Guía de Despliegue y Ejecución

### Paso 1: Levantar la Infraestructura (Kafka, ZooKeeper y Kafbat UI)
Desde la terminal en la raíz del proyecto, ejecutá Docker Compose para iniciar todos los contenedores:

```bash
docker-compose up -d
```
Verificar que ambos contenedores estén en estado running o Up.
Tener en cuenta limpiar los datos de sesiones y volumenes anteriores.

```bash
docker compose down -v
```

### Paso 2: Configurar el Entorno Virtual en Anaconda Prompt
Navegar a la raíz del proyecto e iniciá el entorno:

:: Crear entorno en Conda con Python 3.10
```bash
conda create -n beam-env python=3.10 -y
```
:: Activar el entorno
```bash
conda activate beam-env
```
:: Instalar dependencias
```bash
pip install -r requirements.txt
pip install "setuptools<70.0.0" confluent-kafka
```

### Paso 3: Ejecutar las Pruebas Unitarias
Antes de iniciar la ingesta, ejecutá la suite de pruebas unitarias con Pytest:

```bash
pytest test_pipeline.py

```
Debe retornar el mensaje de éxito 3 passed

### Paso 4: Ejecutar el Pipeline de Streaming
Para iniciar la lectura continua, procesamiento en ventanas y reescritura idempotente:

```bash
# Paso 4a: Crear los tópicos en Kafka (Opcional pero recomendado para evitar advertencias de tópico no encontrado)
docker exec -it kafka kafka-topics --create --topic eventos-entrada --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec -it kafka kafka-topics --create --topic eventos-salida --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# Paso 4b: Ejecutar el Pipeline de Streaming
python pipeline.py
```
La consola el mensaje indicando que el pipeline está activo y escuchando eventos en eventos-entrada.

## 6. Prueba End-to-End con Eventos Sintéticos

Para probar la ingesta y deduplicación en tiempo real, abrir una segunda terminal y canalizar directamente los eventos guardados en el archivo de pruebas hacia el productor de Kafka:

```bash
docker exec -i kafka kafka-console-producer --topic eventos-entrada --bootstrap-server localhost:9092 < eventos_prueba.txt
```
## 7. Monitoreo en el Dashboard (Kafbat UI)

Para verificar la recepción de los mensajes deduplicados en la interfaz gráfica:

Ingresar al navegador:
```bash
http://localhost:8080
```
1-En el menú lateral izquierdo, clic en Topics.

2-Seleccionar el tópico de salida eventos-salida.

3- Clic en la pestaña Messages para visualizar los eventos procesados por el pipeline.

## 8. Estrategia de Pruebas e Idempotencia
Deduplicación por Clave (DeduplicateDoFn): Se procesan los grupos de eventos por event_id garantizando que solo la primera ocurrencia sea emitida y descartando duplicados generados por reintentos de red o fallos en el origen.

Escritura Idempotente (KafkaWriteDoFn): Cada registro enviado al tópico eventos-salida incluye la clave de partición basada en event_id, garantizando consistencia en el almacenamiento.

Validación Automatizada (test_pipeline.py): Suite completa con Pytest que valida unitariamente la deduplicación, el parseo de marcas temporales y las funciones de agregación combinada (CombineFn).
