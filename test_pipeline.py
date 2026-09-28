import pytest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import TimestampedValue, FixedWindows

# Importar las clases del pipeline
from pipeline import DeduplicateDoFn, ParseAndTimestampDoFn, CountEventsCombineFn


def test_deduplication_logic():
    """Valida que la lógica de deduplicación reduzca elementos repetidos a uno solo."""
    input_data = [
        ("EV100", [{"event_id": "EV100", "user": "A"}, {"event_id": "EV100", "user": "A"}]),
        ("EV200", [{"event_id": "EV200", "user": "B"}])
    ]

    expected_output = [
        {"event_id": "EV100", "user": "A"},
        {"event_id": "EV200", "user": "B"}
    ]

    with TestPipeline() as p:
        pcoll = p | beam.Create(input_data)
        result = pcoll | beam.ParDo(DeduplicateDoFn())
        assert_that(result, equal_to(expected_output))


def test_unique_events_pass_through():
    """Valida que eventos únicos sin duplicados pasen intactos."""
    input_data = [
        ("EV300", [{"event_id": "EV300", "action": "click"}])
    ]

    expected_output = [
        {"event_id": "EV300", "action": "click"}
    ]

    with TestPipeline() as p:
        pcoll = p | beam.Create(input_data)
        result = pcoll | beam.ParDo(DeduplicateDoFn())
        assert_that(result, equal_to(expected_output))


def test_windowing_and_incremental_aggregation():
    """Valida la agregación incremental en ventanas temporales fijas asignando timestamps explícitamente."""
    # Tuplas de entrada: (clave, valor, timestamp_unix)
    raw_events = [
        ("A", {"event_id": "1"}, 1000),  # Ventana 1
        ("A", {"event_id": "2"}, 1020),  # Ventana 1
        ("A", {"event_id": "3"}, 1070),  # Ventana 2
    ]

    with TestPipeline() as p:
        result = (
            p
            | "CreateEvents" >> beam.Create(raw_events)
            # Extrae la tupla (k, v) y le asigna el timestamp para evitar TypeCheckError
            | "AddTimestamps" >> beam.Map(lambda elem: TimestampedValue((elem[0], elem[1]), elem[2]))
            | "FixedWindow" >> beam.WindowInto(FixedWindows(60))
            | "Combine" >> beam.CombinePerKey(CountEventsCombineFn())
        )

        expected = [("A", 2), ("A", 1)]
        assert_that(result, equal_to(expected))