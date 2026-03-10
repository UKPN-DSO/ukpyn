"""Helper functions for powerflow orchestrator operations.

This module provides utilities for coordinating between LTDS topology data
and powerflow time series data.
"""

from typing import Any

from ..models import RecordListResponse


def extract_lv_nodes_and_voltages(
    table_2a_response: RecordListResponse,
    table_2b_response: RecordListResponse,
    debug: bool = False,
) -> list[dict[str, Any]]:
    """
    Extract LV node and voltage information from LTDS Table 2A/2B responses.

    Args:
        table_2a_response: Response from LTDS Table 2A (2-winding transformers)
        table_2b_response: Response from LTDS Table 2B (3-winding transformers)
        debug: If True, print debug information

    Returns:
        List of dicts with 'lv_node' and 'voltage_lv' fields

    Example:
        >>> nodes = extract_lv_nodes_and_voltages(table_2a, table_2b)
        >>> [{'lv_node': 'node_123', 'voltage_lv': 11.0}, ...]
    """
    nodes_info = []

    if debug:
        print(f"\n[DEBUG] Extracting LV nodes from LTDS responses:")
        print(f"  Table 2A records: {len(table_2a_response.records)}")
        print(f"  Table 2B records: {len(table_2b_response.records)}")

    # Process Table 2A (2-winding transformers)
    for idx, record in enumerate(table_2a_response.records):
        fields = record.fields
        lv_node = fields.get("lv_node")
        voltage_lv = fields.get("voltage_lv")
        lv_substation = fields.get("lv_substation")
        hv_substation = fields.get("hv_substation")

        if debug:
            print(f"\n  [Table 2A #{idx+1}]")
            print(f"    HV Substation: {hv_substation}")
            print(f"    LV Substation: {lv_substation}")
            print(f"    LV Node: {lv_node}")
            print(f"    Voltage LV: {voltage_lv}")

        if lv_node and voltage_lv is not None:
            nodes_info.append({
                "lv_node": lv_node, 
                "voltage_lv": voltage_lv,
                "lv_substation": lv_substation,
                "hv_substation": hv_substation
            })

    # Process Table 2B (3-winding transformers - has two LV sides)
    for idx, record in enumerate(table_2b_response.records):
        fields = record.fields
        hv_substation = fields.get("hv_substation")
        
        if debug:
            print(f"\n  [Table 2B #{idx+1}]")
            print(f"    HV Substation: {hv_substation}")
        
        # LV side 1
        lv_node_1 = fields.get("lv_node_1")
        voltage_lv_1 = fields.get("voltage_lv_1")
        lv_substation_1 = fields.get("lv_substation_1")
        
        if debug:
            print(f"    LV Substation 1: {lv_substation_1}")
            print(f"    LV Node 1: {lv_node_1}")
            print(f"    Voltage LV 1: {voltage_lv_1}")
            
        if lv_node_1 and voltage_lv_1 is not None:
            nodes_info.append({
                "lv_node": lv_node_1, 
                "voltage_lv": voltage_lv_1,
                "lv_substation": lv_substation_1,
                "hv_substation": hv_substation
            })

        # LV side 2
        lv_node_2 = fields.get("lv_node_2")
        voltage_lv_2 = fields.get("voltage_lv_2")
        lv_substation_2 = fields.get("lv_substation_2")
        
        if debug:
            print(f"    LV Substation 2: {lv_substation_2}")
            print(f"    LV Node 2: {lv_node_2}")
            print(f"    Voltage LV 2: {voltage_lv_2}")
            
        if lv_node_2 and voltage_lv_2 is not None:
            nodes_info.append({
                "lv_node": lv_node_2, 
                "voltage_lv": voltage_lv_2,
                "lv_substation": lv_substation_2,
                "hv_substation": hv_substation
            })

    if debug:
        print(f"\n[DEBUG] Extracted {len(nodes_info)} LV nodes total")
        for i, node in enumerate(nodes_info):
            print(f"  Node {i+1}: {node['lv_node']} @ {node['voltage_lv']}kV (HV: {node['hv_substation']}, LV: {node['lv_substation']})")

    return nodes_info


def parse_voltage(voltage_str: Any) -> float | None:
    """
    Parse voltage value from string or numeric format.

    Args:
        voltage_str: Voltage value (could be "11kV", "33", 11.0, etc.)

    Returns:
        Numeric voltage value in kV, or None if cannot parse

    Example:
        >>> parse_voltage("11kV")
        11.0
        >>> parse_voltage("33")
        33.0
        >>> parse_voltage(11.0)
        11.0
    """
    if voltage_str is None:
        return None

    # Already numeric
    if isinstance(voltage_str, (int, float)):
        return float(voltage_str)

    # String parsing
    if isinstance(voltage_str, str):
        # Remove common suffixes
        clean = voltage_str.lower().replace("kv", "").strip()
        try:
            return float(clean)
        except ValueError:
            return None

    return None


def determine_transformer_type(voltage_lv: float) -> str:
    """
    Determine if a transformer is 'primary' or 'grid' based on LV voltage.

    Grid transformers: 132kV/33kV (voltage_lv >= 22 kV)
    Primary transformers: 33kV/11kV (voltage_lv < 22 kV)

    Args:
        voltage_lv: LV side voltage in kV

    Returns:
        'grid' or 'primary'

    Example:
        >>> determine_transformer_type(33.0)
        'grid'
        >>> determine_transformer_type(11.0)
        'primary'
    """
    return "grid" if voltage_lv >= 22 else "primary"


def extract_transformer_ids(response: RecordListResponse, debug: bool = False) -> list[str]:
    """
    Extract unique transformer IDs (tx_id) from a powerflow response.

    Args:
        response: Response from powerflow monthly data query
        debug: If True, print debug information

    Returns:
        List of unique transformer IDs

    Example:
        >>> tx_ids = extract_transformer_ids(monthly_response)
        >>> ['berechurch_primary_11kv_t1', 'berechurch_primary_11kv_t2']
    """
    tx_ids = set()

    if debug:
        print(f"\n[DEBUG] Extracting transformer IDs from {len(response.records)} records")

    for idx, record in enumerate(response.records):
        tx_id = record.fields.get("tx_id")
        if tx_id:
            tx_ids.add(tx_id)
            if debug and idx < 10:  # Show first 10 for brevity
                print(f"  Record {idx+1}: tx_id={tx_id}")

    if debug:
        print(f"\n[DEBUG] Found {len(tx_ids)} unique transformer IDs:")
        for tx_id in sorted(tx_ids):
            print(f"  - {tx_id}")

    return sorted(tx_ids)
