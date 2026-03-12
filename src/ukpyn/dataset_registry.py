"""Canonical dataset ID registry mapping friendly names to OpenDataSoft IDs.

Each entry maps a user-friendly name to the actual ODP dataset identifier.
Inline comments describe what each dataset contains.

Registry verified against ODP catalog: 2026-02-04
Total ODP datasets: 133
"""

# =============================================================================
# LTDS (Long Term Development Statement) Datasets
# Published twice yearly with network planning data
# =============================================================================
LTDS_DATASETS: dict[str, str] = {
    # Table 1 - Circuit Data (lines, cables, circuit parameters)
    "table_1": "ltds-table-1-circuit-data",
    "circuit_data": "ltds-table-1-circuit-data",
    # Table 2a - 2-winding transformer specs (HV-LV)
    "table_2a": "ltds-table-2a-transformer-2w",
    "transformer_2w": "ltds-table-2a-transformer-2w",
    # Table 2b - 3-winding transformer specs (HV-LV1-LV2)
    "table_2b": "ltds-table-2b-transformer-data-3w",
    "transformer_3w": "ltds-table-2b-transformer-data-3w",
    # Table 3a - Observed peak demand at primary substations
    "table_3a": "ltds-table-3a-load-data-observed",
    "observed_demand": "ltds-table-3a-load-data-observed",
    "observed_peak_demand": "ltds-table-3a-load-data-observed",  # Legacy alias
    # Table 3a Transposed - Same data, pivoted format
    "table_3a_transposed": "ltds-table-3a-load-data-observed-transposed",
    # Table 3b - True (firm) demand forecasts
    "table_3b": "ltds-table-3b-load-data-true",
    "forecast_demand": "ltds-table-3b-load-data-true",
    # Table 4a - Three-phase fault levels at substations
    "table_4a": "ltds-table-4a-3ph-fault-level",
    "fault_level_3ph": "ltds-table-4a-3ph-fault-level",
    # Table 4b - Earth fault levels at substations
    "table_4b": "ltds-table-4b-earth-fault-level",
    "fault_level_earth": "ltds-table-4b-earth-fault-level",
    # Table 5 - Distributed generation by primary substation
    "table_5": "ltds-table-5-generation",
    "generation": "ltds-table-5-generation",
    # Table 6 - New connection interest queue
    "table_6": "ltds-table-6-interest-connections",
    "connection_interest": "ltds-table-6-interest-connections",
    # Table 7 - Operational restrictions on network
    "table_7": "ltds-table-7-operational-restrictions",
    "restrictions": "ltds-table-7-operational-restrictions",
    # Table 8 - Fault data (>95th percentile events)
    "table_8": "ltds-table-8-gt-95-perc-fault-data",
    "fault_data": "ltds-table-8-gt-95-perc-fault-data",
    # Infrastructure projects - 5-year development plans
    "projects": "ukpn-ltds-infrastructure-projects",
    "infrastructure_projects": "ukpn-ltds-infrastructure-projects",
    # CIM - IEC Common Information Model network representation
    "cim": "ukpn-ltds-cim",
}

# =============================================================================
# DFES (Distribution Future Energy Scenarios) Datasets
# Network capacity scenarios and headroom analysis
# =============================================================================
DFES_DATASETS: dict[str, str] = {
    # Network headroom report - capacity availability by scenario
    "headroom": "dfes-network-headroom-report",
    "headroom_report": "dfes-network-headroom-report",
    # Peak demand scenarios by local authority
    "peak_demand_scenarios": "ukpn-dfes-peak-demand-scenarios",
    # DFES data aggregated by local authority
    "by_local_authority": "ukpn-dfes-by-local-authorities",
}

# =============================================================================
# DNOA (Distribution Network Options Assessment) Datasets
# Network reinforcement and flexibility options
# =============================================================================
DNOA_DATASETS: dict[str, str] = {
    # Main DNOA methodology and results
    "dnoa": "ukpn-dnoa",
    # Low voltage DNOA assessments
    "lv_dnoa": "ukpn-low-voltage-dnoa",
}

# =============================================================================
# Network Statistics Datasets
# Aggregated network performance and metrics
# =============================================================================
NETWORK_STATS_DATASETS: dict[str, str] = {
    # Annual network statistics summary
    "statistics": "ukpn-network-statistics",
    # Network losses data
    "losses": "ukpn-network-losses",
    # Power quality metrics
    "power_quality": "ukpn-power-quality",
    # IIS (Interruptions Incentive Scheme) performance
    "iis": "ukpn-iis",
}

# =============================================================================
# Powerflow Time Series Datasets
# Circuit and transformer operational measurements
# Note: Some half-hourly data is split by licence area (EPN/SPN)
# =============================================================================
POWERFLOW_DATASETS: dict[str, str] = {
    # 132kV Circuit Data - all areas combined (not split)
    "132kv_monthly": "ukpn-132kv-circuit-operational-data-monthly",
    "132kv_half_hourly": "ukpn-132kv-circuit-operational-data-half-hourly",
    # 33kV Circuit Data - monthly combined, half-hourly SPLIT by licence area
    "33kv_monthly": "ukpn-33kv-circuit-operational-data-monthly",
    "33kv_half_hourly_epn": "ukpn-33kv-circuit-operational-data-half-hourly-epn",
    "33kv_half_hourly_spn": "ukpn-33kv-circuit-operational-data-half-hourly-spn",
    # Grid Transformer Data - 132/33kV, all areas combined (not split)
    "grid_monthly": "ukpn-grid-transformer-operational-data-monthly",
    "grid_half_hourly": "ukpn-grid-transformer-operational-data-half-hourly",
    # Primary Transformer Data - 33/11kV, monthly combined, half-hourly SPLIT
    "primary_monthly": "ukpn-primary-transformer-power-flow-historic-monthly",
    "primary_half_hourly_epn": "ukpn-primary-transformer-power-flow-historic-half-hourly-epn",
    "primary_half_hourly_spn": "ukpn-primary-transformer-power-flow-historic-half-hourly-spn",
}

# =============================================================================
# Flexibility Markets Datasets
# Flexibility service dispatches and tenders
# =============================================================================
FLEXIBILITY_DATASETS: dict[str, str] = {
    # Flexibility dispatch events - when services were called
    "dispatches": "ukpn-flexibility-dispatches",
    "dispatch_events": "ukpn-flexibility-dispatches",  # Alias
    # Flexibility tender data - procurement information
    "tenders": "ukpn-flexibility-tender-data",

}

# =============================================================================
# Curtailment Datasets
# Generation/demand curtailment events
# =============================================================================
CURTAILMENT_DATASETS: dict[str, str] = {
    # Site-specific curtailment events
    "events": "ukpn-curtailment-events-site-specific",
    "site_specific": "ukpn-curtailment-events-site-specific",
    # Curtailment events (included for convenience in flexibility orchestrator)
    "curtailment": "ukpn-curtailment-events-site-specific",
    "curtailment_events": "ukpn-curtailment-events-site-specific",  # Alias
}

# =============================================================================
# DER (Distributed Energy Resources) Datasets
# Embedded generation/storage and large demand connections
# =============================================================================
DER_DATASETS: dict[str, str] = {
    # Embedded Capacity Register - all DER >= 1MW
    "ecr": "ukpn-embedded-capacity-register",
    "embedded_capacity": "ukpn-embedded-capacity-register",
    # ECR for smaller installations < 1MW
    "ecr_small": "ukpn-embedded-capacity-register-1-under-1mw",
    "embedded_capacity_small": "ukpn-embedded-capacity-register-1-under-1mw",
    # Large Demand List - major demand customers
    "large_demand": "ukpn-large-demand-list",
    # Large embedded power stations
    "large_power_stations": "ukpn-large-embedded-power-stations",
}

# Backward-compatible alias
RESOURCES_DATASETS: dict[str, str] = DER_DATASETS

# =============================================================================
# Geospatial / Infrastructure Datasets
# Network asset locations and coverage areas
# =============================================================================
GEO_DATASETS: dict[str, str] = {
    # Substation distribution areas by postcode
    "primary_areas": "ukpn_primary_postcode_area",
    "secondary_areas": "ukpn_secondary_postcode_area",
    "grid_areas": "ukpn-grid-postcode-area",
    # Secondary substation sites
    "secondary_sites": "ukpn-secondary-sites",
    # Grid and primary substation locations
    "grid_primary_sites": "grid-and-primary-sites",
    "grid_supply_points": "ukpn-grid-supply-points",
    # Overhead lines by voltage
    "hv_overhead_lines": "ukpn-hv-overhead-lines-shapefile",
    "lv_overhead_lines": "ukpn-lv-overhead-lines-shapefile",
    "33kv_overhead_lines": "ukpn-33kv-overhead-lines",
    "132kv_overhead_lines": "ukpn-132kv-overhead-lines",
    # Poles and towers
    "hv_poles": "ukpn-hv-poles",
    "lv_poles": "ukpn-lv-poles",
    "33kv_poles": "ukpn-33kv-poles-towers",
    "132kv_poles": "ukpn-132kv-poles-towers",
    # Licence area boundaries
    "licence_boundaries": "ukpn-licence-boundaries",
    "epn_boundaries": "ukpn-epn-area-operational-boundaries",
    "spn_boundaries": "ukpn-spn-area-operational-boundaries",
    "lpn_boundaries": "ukpn-lpn-area-operational-boundaries",
    # IDNO areas
    "idno_areas": "ukpn-idno-areas",
}

# =============================================================================
# Transformer & Equipment Datasets
# Asset specifications and utilisation
# =============================================================================
EQUIPMENT_DATASETS: dict[str, str] = {
    # Transformer specifications
    "grid_transformers": "ukpn-grid-transformers",
    "primary_transformers": "ukpn-primary-transformers",
    "secondary_transformers": "ukpn-secondary-site-transformers",
    "super_grid_transformers": "ukpn-super-grid-transformer",
    # Secondary site utilisation
    "secondary_utilisation": "ukpn-secondary-site-utilisation",
}

# =============================================================================
# Connections & Queue Datasets
# Connection applications and queue status
# =============================================================================
CONNECTIONS_DATASETS: dict[str, str] = {
    # Overall connection queue insights
    "queue_insights": "ukpn-overall-queue-insights",
    # GSP-level insights and narratives
    "gsp_insights": "ukpn-gsp-insights",
    "gsp_narrative": "ukpn-gsp-narrative",
    "gsp_project_status": "ukpn-gsp-project-status",
    # Wide area planning status
    "wide_planning": "ukpn-wide-planning-status",
    # Modification applications
    "modifications": "ukpn-modification-application",
    # Connections reform by local authority
    "connections_reform": "ukpn-connections-reform-local-authority",
}

# =============================================================================
# Constraints & Operations Datasets
# Real-time network constraints and operations
# =============================================================================
OPERATIONS_DATASETS: dict[str, str] = {
    # Real-time constraint meter readings
    "constraint_readings": "ukpn-constraints-real-time-meter-readings",
    # Constraint breach history
    "constraint_history": "ukpn-constraint-breaches-history",
    # EHV network outages
    "ehv_outages": "ukpn-ehv-network-outages",
    # Live faults
    "live_faults": "ukpn-live-faults",
    # Rota load disconnection schedule
    "rota_disconnect": "ukpn-rota-load-disconnection",
}

# =============================================================================
# Sensitivity Factors Datasets
# Network sensitivity for import/export
# =============================================================================
SENSITIVITY_DATASETS: dict[str, str] = {
    # Import sensitivity factors
    "sensitivity_import": "ukpn-sensitivity-factors-import",
    # Export sensitivity factors
    "sensitivity_export": "ukpn-sensitivity-factors-export",
}

# =============================================================================
# Smart Meter & Low Carbon Tech Datasets
# Smart meter data and LCT deployment
# =============================================================================
SMART_LCT_DATASETS: dict[str, str] = {
    # Smart meter consumption at substation level
    "smart_meter_substation": "ukpn-smart-meter-consumption-substation",
    # Smart meter consumption at LV feeder level
    "smart_meter_feeder": "ukpn-smart-meter-consumption-lv-feeder",
    # Smart meter installation volumes
    "smart_meter_volumes": "ukpn-smart-meter-installation-volumes",
    # Low carbon technology deployment by LSOA
    "lct_lsoa": "ukpn-low-carbon-technologies-lsoa",
    # LCT at secondary substation level
    "lct_secondary": "ukpn-low-carbon-technologies-secondary",
    # General LCT dataset
    "low_carbon_tech": "low-carbon-technologies",
}

# =============================================================================
# Standard Profiles Datasets
# Load and generation profiles
# =============================================================================
PROFILES_DATASETS: dict[str, str] = {
    # Standard electricity demand profiles
    "demand_profiles": "ukpn-standard-profiles-electricity-demand",
    # Technology-specific generation profiles
    "generation_profiles": "ukpn-standard-technology-profiles-generation",
    # Data centre demand profiles
    "data_centre_profiles": "ukpn-data-centre-demand-profiles",
}

# =============================================================================
# Appendix G (ANM/Curtailment Settings) Datasets
# Active Network Management configuration
# =============================================================================
APPENDIX_G_DATASETS: dict[str, str] = {
    # Main Appendix G data
    "appendix_g": "ukpn-appendix-g",
    # Aggregated view
    "appendix_g_aggregated": "ukpn-appendix-g-aggregated",
    # Project progression
    "appendix_g_progression": "ukpn-appendix-g-project-progression",
    # Site-specific conditions
    "appendix_g_conditions": "ukpn-appendix-g-site-specific-conditions",
    # Summary
    "appendix_g_summary": "ukpn-appendix-g-summary",
}

# =============================================================================
# Reference & Metadata Datasets
# Supporting reference data
# =============================================================================
REFERENCE_DATASETS: dict[str, str] = {
    # Local authority boundaries
    "local_authorities": "ukpn-local-authorities",
    # Business glossary
    "glossary": "ukpn-business-glossary",
    # Data roadmap tracker
    "data_roadmap": "ukpn-external-facing-tracker",
    # Data best practice maturity
    "data_maturity": "ukpn-data-best-practice-live-maturity",
    # EV chargepoint register
    "chargepoints": "ozev-ukpn-national-chargepoint-register",
}

# =============================================================================

# BEGIN AUTO-GENERATED UNMANAGED DATASETS
# =============================================================================
# Unmanaged ODP Datasets
# Auto-generated from ODP metadata (domain-dataset0)
# Last sync: 2026-03-12
# =============================================================================
UNMANAGED_DATASETS: dict[str, str] = {
}
# END AUTO-GENERATED UNMANAGED DATASETS

# NETWORK_DATASETS (Legacy - backward compatibility)
# Combines stats, profiles, and some powerflow for existing network orchestrator
# =============================================================================
NETWORK_DATASETS: dict[str, str] = {
    # Circuit time series (subset for backward compat)
    "132kv_monthly": "ukpn-132kv-circuit-operational-data-monthly",
    "132kv_half_hourly": "ukpn-132kv-circuit-operational-data-half-hourly",
    "33kv_monthly": "ukpn-33kv-circuit-operational-data-monthly",
    # Statistics
    "statistics": "ukpn-network-statistics",
    "network_statistics": "ukpn-network-statistics",
    # Profiles
    "demand_profiles": "ukpn-standard-profiles-electricity-demand",
}

# =============================================================================
# Combined registry for lookup
# =============================================================================
ALL_DATASETS: dict[str, str] = {
    **LTDS_DATASETS,
    **DFES_DATASETS,
    **DNOA_DATASETS,
    **NETWORK_STATS_DATASETS,
    **NETWORK_DATASETS,
    **POWERFLOW_DATASETS,
    **FLEXIBILITY_DATASETS,
    **CURTAILMENT_DATASETS,
    **DER_DATASETS,
    **GEO_DATASETS,
    **EQUIPMENT_DATASETS,
    **CONNECTIONS_DATASETS,
    **OPERATIONS_DATASETS,
    **SENSITIVITY_DATASETS,
    **SMART_LCT_DATASETS,
    **PROFILES_DATASETS,
    **APPENDIX_G_DATASETS,
    **REFERENCE_DATASETS,
}
