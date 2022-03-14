"""Provides main extraction functionality"""
from .cohort import extract_cohort
from .admission import extract_admission_events

__all__ = [
    'extract_cohort',
    'extract_admission_events'
]
