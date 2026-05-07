# ukpyn Demo Video — Commentary Script

Keep this open in a second window while recording. Each section matches a notebook section.

---

## Before Recording

- Pre-run all notebook cells once to warm cache
- Have browser tabs ready:
  1. ODP portal: https://ukpowernetworks.opendatasoft.com/
  2. PyPI: https://pypi.org/project/ukpyn/
  3. Docs: https://ukpn-dso.github.io/ukpyn/
  4. geojson.io (empty, ready to paste)
- Font size: bump VS Code to 16pt+ so it's readable on video
- Close unnecessary panels/tabs

---

## 1. The Problem (~30s)

**[Browser → ODP portal]**

> "UK Power Networks publishes over 130 datasets on their Open Data Portal — demand forecasts, substation locations, embedded generation, connection queues. It's a gold mine for energy strategy work. But using it directly means remembering dataset IDs, building API URLs, handling pagination, and dealing with data quality issues like NaN values and inconsistent geometry formats."

**[Browser → PyPI page]**

> "ukpyn wraps all of that into a clean Python library. One-line install from PyPI."

**[Switch to VS Code / notebook]**

---

## 2. First Query (~30s)

**[Run cell 2]**

> "Three lines of code. Import the LTDS orchestrator, call get_table_3a for South Eastern Power Networks, and we get structured data back immediately. Every field is named, typed, and ready to use."

> "No dataset IDs to memorise, no URL construction, no pagination logic."

---

## 3. Orchestrators (~30s)

**[Run cell 3]**

> "ukpyn organises 133 datasets into five orchestrators — one for each domain. LTDS for network planning data. DFES for future energy scenarios. DER for the generation register. GIS for geospatial assets. DNOA for flex-versus-build decisions."

> "Each orchestrator has convenience methods for the most common queries, plus a generic `get()` that takes any dataset alias."

---

## 4. Filter to Mole Valley (~45s)

**[Run cell 4 — ECR]**

> "Let's say Mole Valley Council wants to know what generation is already connected in their area. One `where` clause — `local_authority = 'Mole Valley'` — and we get back every site in the Embedded Capacity Register for that authority."

**[Run cell 5 — DFES]**

> "And what does the future look like? The DFES scenarios dataset gives us heat pump uptake, EV numbers, and solar deployment — broken out by year and pathway — for Mole Valley specifically. This is exactly how local authority energy strategies are structured."

---

## 5. Map Moment (~60s)

**[Run cell 6 — substations]**

> "Every record has a normalised `.geometry` property. Standard GeoJSON — coordinates you can drop straight onto a map."

**[Run cell 7 — GeoJSON export]**

> "You can also export entire datasets as GeoJSON. Here we're exporting the UKPN licence area boundaries with 2D coordinates — the `dimensions` parameter strips Z values so it works in any web mapping tool."

**[Copy the JSON output line, switch to browser → geojson.io, paste]**

> "Paste that into geojson.io and you get the three UKPN licence areas on a map instantly. Every geospatial dataset in ukpyn can be exported like this — substations, overhead lines, cable routes — ready for QGIS, Leaflet, Mapbox, whatever your team uses."

---

## 6. Data Quality (~30s)

**[Run cell 8]**

> "The raw ODP data has some gotchas that trip people up. Fields with NaN values that break JSON serialisation. Geometry fields wrapped in GeoJSON Feature objects. Mixed 2D and 3D coordinates across different endpoints. ukpyn handles all of this automatically — every record comes back clean, consistent, and ready to use."

---

## 7. Scale (~30s)

**[Run cell 9]**

> "Here's the full picture. 133 datasets across 11 domains — network planning, future scenarios, geospatial assets, generation registers, flexibility markets, powerflow time series, smart meter data, and more. All accessible through the same consistent API."

---

## 8. Wrap-up (~15s)

**[Browser → docs page]**

> "Full documentation, tutorials, and code examples at the docs site. Install with `pip install ukpyn` — it's open source, and we'd love contributions."

**[End recording]**
