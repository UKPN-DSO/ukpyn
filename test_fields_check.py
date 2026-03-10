"""Quick diagnostic script to check if fields are populated across all orchestrators."""

from ukpyn.orchestrators import (
    ltds,
    flexibility,
    gis,
    ders,
    powerflow,
    curtailment,
    dfes,
    dnoa,
    network,
)

# Map orchestrators to their available datasets
orchestrators = {
    "ltds": ltds,
    "flexibility": flexibility,
    "gis": gis,
    "ders": ders,
    "powerflow": powerflow,
    "curtailment": curtailment,
    "dfes": dfes,
    "dnoa": dnoa,
    "network": network,
}

print("=" * 80)
print("ORCHESTRATOR FIELDS POPULATION CHECK")
print("=" * 80)
print()

total_tests = 0
passed_tests = 0
failed_tests = 0

for orch_name, orch_module in orchestrators.items():
    print(f"\n{'='*80}")
    print(f"Testing: {orch_name.upper()}")
    print(f"{'='*80}")
    
    # Get available datasets
    if hasattr(orch_module, "available_datasets"):
        datasets = orch_module.available_datasets
        print(f"Available datasets: {len(datasets)}")
        
        # Test first 3 datasets from each orchestrator (or all if fewer)
        test_datasets = datasets[:3] if len(datasets) > 3 else datasets
        
        for dataset in test_datasets:
            total_tests += 1
            try:
                # Try to get 1 record
                result = orch_module.get(dataset, limit=1)
                
                if not result.records:
                    print(f"  ⚠️  {dataset}: No records returned")
                    continue
                
                record = result.records[0]
                
                # Check if fields is populated
                if record.fields is None:
                    print(f"  ❌ {dataset}: fields = None")
                    failed_tests += 1
                elif len(record.fields) == 0:
                    print(f"  ⚠️  {dataset}: fields = {{}} (empty)")
                    failed_tests += 1
                else:
                    field_count = len(record.fields)
                    field_names = list(record.fields.keys())[:5]  # Show first 5 fields
                    print(f"  ✅ {dataset}: {field_count} fields - {field_names}...")
                    passed_tests += 1
                    
            except Exception as e:
                error_msg = str(e)[:60]
                print(f"  ❌ {dataset}: ERROR - {error_msg}")
                failed_tests += 1
    else:
        print(f"  ℹ️  No available_datasets attribute")

print()
print("=" * 80)
print("SUMMARY")
print("=" * 80)
print(f"Total tests: {total_tests}")
print(f"✅ Passed: {passed_tests}")
print(f"❌ Failed: {failed_tests}")
print(f"Success rate: {(passed_tests/total_tests*100) if total_tests > 0 else 0:.1f}%")
print()

if failed_tests == 0:
    print("🎉 All tests passed! Fields are being populated correctly.")
else:
    print(f"⚠️  {failed_tests} test(s) failed. Review the output above.")
