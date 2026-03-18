"""Quick test: fetch field schemas for DNOA datasets from the live ODP API."""

import asyncio

from ukpyn.client import UKPNClient


async def main():
    async with UKPNClient() as client:
        for ds_id in ["ukpn-dnoa", "ukpn-low-voltage-dnoa"]:
            dataset = await client.get_dataset(ds_id)
            fields = dataset.field_ids
            print(f"{ds_id}: {fields}")


if __name__ == "__main__":
    asyncio.run(main())
