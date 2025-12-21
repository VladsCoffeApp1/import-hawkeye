"""Tests for data transformation."""

import pandas as pd
import pytest

from app.detector import DataType
from app.transform import clean_dataframe


class TestGPSTransform:
    def test_drops_unwanted_columns(self):
        df = pd.DataFrame({
            'Document ID': ['doc1'],
            'Emitter Id': ['emit1'],
            'Event Time': ['2025-01-01T10:00:00Z'],
            '_id': ['id1'],
            '_index': ['idx1'],
            'Snet Username': ['user1'],
        })
        result = clean_dataframe(df, DataType.GPS)
        assert '_id' not in result.columns
        assert '_index' not in result.columns
        assert 'Snet Username' not in result.columns

    def test_renames_columns_correctly(self):
        df = pd.DataFrame({
            'Document ID': ['doc1'],
            'Display Name': ['name1'],
            'Emitter Id': ['emit1'],
            'Event Time': ['2025-01-01T10:00:00Z'],
            'Lat': [55.0],
            'Lon': [37.0],
        })
        result = clean_dataframe(df, DataType.GPS)
        assert 'document_id' in result.columns
        assert 'latitude' in result.columns
        assert 'longitude' in result.columns

    def test_adds_event_date_partition(self):
        df = pd.DataFrame({
            'Document ID': ['doc1'],
            'Emitter Id': ['emit1'],
            'Event Time': ['2025-01-01T10:00:00Z'],
        })
        result = clean_dataframe(df, DataType.GPS)
        assert 'event_date' in result.columns


class TestPortEventsTransform:
    def test_only_drops_snet_username(self):
        df = pd.DataFrame({
            'Document ID': ['doc1'],
            'Display Name': ['Port Event 1'],
            'City': ['Rotterdam'],
            'Ata': ['2025-01-01T08:00:00Z'],
            'Event Time': ['2025-01-01T10:00:00Z'],
            'Imo': ['1234567'],
            'Snet Username': ['user1'],
        })
        result = clean_dataframe(df, DataType.SHADOWFLEET_PORT_EVENTS)
        assert 'Snet Username' not in result.columns
        assert 'document_id' in result.columns

    def test_imo_stays_string(self):
        df = pd.DataFrame({
            'Document ID': ['doc1'],
            'Ata': ['2025-01-01T08:00:00Z'],
            'Event Time': ['2025-01-01T10:00:00Z'],
            'Imo': ['1234567'],
        })
        result = clean_dataframe(df, DataType.SHADOWFLEET_PORT_EVENTS)
        assert result['imo'].dtype == object


class TestVesselHistoryTransform:
    def test_only_drops_required_columns(self):
        df = pd.DataFrame({
            'Document ID': ['doc1'],
            'Display Name': ['Vessel 1'],
            'Built Year': [2010],
            'Event Time': ['2025-01-01T10:00:00Z'],
            'Imo': ['1234567'],
            'Snet Username': ['user1'],
            'Class 2 Code': ['X'],
            'Registered Owner': ['Owner A'],
        })
        result = clean_dataframe(df, DataType.SHADOWFLEET_VESSEL_HISTORY)
        assert 'Snet Username' not in result.columns
        assert 'Class 2 Code' not in result.columns
        assert 'Registered Owner' not in result.columns
        assert 'document_id' in result.columns

    def test_imo_stays_string(self):
        df = pd.DataFrame({
            'Document ID': ['doc1'],
            'Built Year': [2010],
            'Event Time': ['2025-01-01T10:00:00Z'],
            'Imo': ['1234567'],
        })
        result = clean_dataframe(df, DataType.SHADOWFLEET_VESSEL_HISTORY)
        assert result['imo'].dtype == object
