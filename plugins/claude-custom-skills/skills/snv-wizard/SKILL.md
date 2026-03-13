---
name: snv-wizard
description: Query local SNV geospatial files (.shp or .gpkg from DNIT) to locate road kilometers from coordinates, extract BR segments between km markers, or enrich CSV/XLS/XLSX tables with WKT geometries derived from kmi/kmf, BR, and UF columns. Use when Claude must work against a user-provided SNV dataset or filtered subset, including QGIS-oriented outputs from spreadsheets or tables.
---

# SNV Wizard

Use the bundled script in `scripts/snv_km.py` for deterministic SNV processing.

## Do this first

Confirm these inputs before running anything:

- An SNV dataset path. Accept `.shp` and `.gpkg`, including filtered subsets by `ul`, `uf`, or any other attribute, as long as the requested BR/km range exists in the file.
- The task mode: coordinate lookup, segment extraction, or table enrichment.
- The target CRS assumptions. Default to `EPSG:4674` for the SNV source. The metric UTM EPSG is **auto-detected** from the dataset centroid via spatial join with the FUSOS_UTM shapefile at `/home/liev/.claude/fusos-utm-brasil/FUSOS_UTM.shp` — do NOT pass `--epsg-utm` unless the user explicitly requests a specific zone.

## Dependencies

Install the geospatial stack before using the script:

```bash
python -m pip install geopandas shapely pyproj pyogrio
```

For table enrichment:

```bash
python -m pip install openpyxl xlrd
```

`pyogrio` is the I/O backend used with GeoPandas in this environment. `openpyxl` is used to preserve `.xlsx` workbooks while adding the geometry columns. `xlrd` is used only to read legacy `.xls`, which is then written back as `.xlsx` or `.csv`.

## Coordinate to km

Run:

```bash
python "<skill-dir>/scripts/snv_km.py" "<caminho-para-snv.shp-ou-gpkg>" --lat -21.8074 --lon -46.4701 --raio 250
```

Read the JSON output and report:

- `br_uf`
- `km`
- `dist_lat_m` when relevant
- `vl_codigo` and `versao_snv` when useful for validation

## Segment extraction

Run:

```bash
python "<skill-dir>/scripts/snv_km.py" "<caminho-para-snv.shp-ou-gpkg>" --modo segmento --br 146 --uf MG --km-ini 423 --km-fim 486 --formato wkt
```

Use `--formato gpkg` or `--formato shp` with `--output` when the user wants a geospatial file instead of inline JSON/WKT.

## Table enrichment with WKT

Use this when the user provides a `.csv`, `.xlsx`, or `.xls` containing km ranges and wants a copy with a geometry column ready for QGIS import.

For Excel inputs, process only the `Export_ICM` worksheet. The script also accepts the normalized form `Export-ICM` when the workbook uses a hyphen instead of an underscore.

Run:

```bash
python "<skill-dir>/scripts/snv_km.py" "<caminho-para-snv.shp-ou-gpkg>" --modo tabela --input "C:\dados\entrada.xlsx" --output "C:\dados\entrada_snv_wkt.xlsx"
```

The script auto-detects common aliases for:

- BR: `br`, `rodovia`, `rod`
- UF: `uf`, `estado`
- km inicial: `kmi`, `km inicial`, `km_ini`
- km final: `kmf`, `km final`, `km_fim`

When using table mode:

- For `.xlsx`, preserve the workbook and append the output columns only on `Export_ICM`.
- For `.xls`, read the selected worksheet and write the result as `.xlsx` or `.csv`.
- Accept blank `uf` values and fall back to `MG` unless the user specifies another default.
- If some rows fail, keep the row and add the failure text to `snv_error`.

Read [references/tabular-mode.md](references/tabular-mode.md) only when table mode is needed.

## ICM workbook export

Use `scripts/export_icm.py` when the user already has an ICM workbook with `geometry_wkt` and wants direct `csv`, `xlsx`, or `gpkg` export without consolidating the two directions.

Run:

```bash
python "<skill-dir>/scripts/export_icm.py" "C:\dados\icm_geom.xlsx" --formats gpkg,csv,xlsx --output-base "C:\dados\icm_geom"
```

When exporting to `gpkg`, the script writes:

- one line layer named after the output file;
- one point layer named `marcos-km` with the internal integer kilometer markers inferred from the covered segment.

## Validation

For coordinate lookups, optionally validate against DNITGeo:

`https://servicos.dnit.gov.br/sgplan/apigeo/rotas/localizarkm?lng=<lon>&lat=<lat>&r=<raio>&data=<YYYY-MM-DD>`

Use the local file as the primary source of truth. DNITGeo validation is secondary because the user may be working on an older SNV version or a local subset.
