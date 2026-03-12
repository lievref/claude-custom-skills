# Table Mode

Use this reference only when enriching tabular inputs with WKT geometry.

## Accepted inputs

- `.csv`
- `.xlsx`
- `.xls` for reading only, when `xlrd` is installed

## Worksheet rule

For spreadsheet inputs, work only with the worksheet named `Export_ICM`.

If the workbook uses the variant `Export-ICM`, treat it as the same worksheet name.

If neither variant exists, stop and report that the expected worksheet is missing.

## Required business fields

The script auto-detects these logical fields from common aliases:

- `br`: `br`, `rodovia`, `rod`, `highway`
- `uf`: `uf`, `estado`, `sg_uf`
- `kmi`: `kmi`, `km inicial`, `km_ini`, `km_inicial`
- `kmf`: `kmf`, `km final`, `km_fim`, `km_final`

Headers are matched case-insensitively and with punctuation removed, so values like `Km inicial`, `KM_INICIAL`, and `km-inicial` are treated as the same alias.

## Output behavior

- Append `geometry_wkt` to the existing table.
- Append `snv_error` only when at least one row cannot be processed.
- Preserve existing `.xlsx` workbooks and modify only the target worksheet.
- If the input is `.xls` and no explicit output is provided, the script writes `<nome>_snv_wkt.xlsx`.
- Use the output directly in QGIS by importing the table and selecting the `geometry_wkt` column as WKT geometry.

## Recommended command

```bash
python scripts/snv_km.py "<snv.shp-ou-gpkg>" --modo tabela --input "<entrada.xlsx>" --output "<saida.xlsx>"
```
