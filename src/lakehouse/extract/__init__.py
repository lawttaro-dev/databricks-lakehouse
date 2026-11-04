"""Extractors: pull raw data from external sources into landing Volumes.

Runs outside Databricks (GitHub Actions or a laptop), because Free Edition restricts
outbound internet access. See docs/03-decisions.md (ADR-005, ADR-008, ADR-009).
"""
