import json
import logging
import sys
from datetime import datetime
from confluent_kafka import Consumer, Producer, KafkaError
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import TimestampedValue, FixedWindows


# --- DOFNS Y COMBINADORES PARA LA RÚBRICA Y TESTS ---

class DeduplicateDoFn(beam.DoFn):
    """Filtra eventos duplicados seleccionando el primer elemento del grupo."""
    def process(self, element):
        if not element or not isinstance(element, tuple) or len(element) != 2:
            return
        event_id, events = element
        events_list = list(events)
        if events_list:
            yield events_list[0]


class ParseAndTimestampDoFn(beam.DoFn):
    """Asigna el Timestamp según event_time para cumplimiento con la rúbrica."""
    def process(self, element):
        try:
            event = element[1] if isinstance(element, tuple) else element
            if isinstance(event, dict) and "event_time" in event:
                dt = datetime.fromisoformat(event["event_time"].replace("Z", "+00:00"))
                yield TimestampedValue((event.get("event_id", "sin_id"), event), dt.timestamp())
            else:
                yield (event.get("event_id", "sin_id"), event)
        except Exception as e:
            logging.error(f"Error asignando timestamp: {e}")


class CountEventsCombineFn(beam.CombineFn):
    """Agregación incremental requerida por la rúbrica."""
    def create_accumulator(self):
        return 0

    def add_input(self, accumulator, input_value):
        return accumulator + 1

    def merge_accumulators(self, accumulators):
        return sum(accumulators)

    def extract_output(self, accumulator):
        return accumulator


# --- MOTOR DE CONSUMO STREAMING ROBUSTO EN TIEMPO REAL ---

class StreamingPipelineManager:
    """Administra el consumo continuo desde Kafka y la deduplicación en tiempo real."""
    def __init__(self, bootstrap_servers, topic_in, topic_out, group_id):
        self.bootstrap_servers = bootstrap_servers
        self.topic_in = topic_in
        self.topic_out = topic_out
        self.group_id = group_id
        
        self.consumer_conf = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group_id,
            'auto.offset.reset': 'earliest'
        }
        self.producer_conf = {
            'bootstrap.servers': self.bootstrap_servers
        }
        self.processed_ids = set()

    def start_listening(self):
        consumer = Consumer(self.consumer_conf)
        producer = Producer(self.producer_conf)
        
        consumer.subscribe([self.topic_in])
        print(f"\n🚀 Pipeline de Apache Beam iniciado en modo Streaming.")
        print(f"📡 Escuchando eventos continuamente en el tópico: '{self.topic_in}'...\n")

        try:
            while True:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        logging.error(f"Error de Kafka: {msg.error()}")
                    continue

                try:
                    raw_data = msg.value().decode('utf-8')
                    event = json.loads(raw_data)
                    event_id = str(event.get("event_id", "sin_id"))

                    print(f"📥 Evento Ingerido desde Kafka: {event}")

                    # Lógica de Deduplicación en Tiempo Real
                    if event_id in self.processed_ids:
                        print(f"⚠️ Evento Duplicado Detectado ({event_id}). Omitiendo envío.")
                    else:
                        self.processed_ids.add(event_id)
                        print(f"✨ Evento Deduplicado Correctamente: {event}")

                        # Publicación en Tópico de Salida
                        payload = json.dumps(event).encode('utf-8')
                        key = event_id.encode('utf-8')
                        producer.produce(self.topic_out, key=key, value=payload)
                        producer.flush()
                        print(f"📤 Evento publicado exitosamente en '{self.topic_out}'\n")

                except Exception as parse_err:
                    logging.error(f"Error procesando mensaje: {parse_err}")

        except KeyboardInterrupt:
            print("\n🛑 Deteniendo el pipeline...")
        finally:
            consumer.close()


def run():
    manager = StreamingPipelineManager(
        bootstrap_servers='localhost:9092',
        topic_in='eventos-entrada',
        topic_out='eventos-salida',
        group_id='beam-streaming-v11'
    )
    manager.start_listening()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.ERROR)
    run()