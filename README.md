# claude-custom-skills

Custom skills plugin for [Claude Code](https://claude.ai/claude-code).

## Skills

### snv-wizard

Query local SNV geospatial files (`.shp` or `.gpkg` from DNIT) to:

- Locate road kilometers from GPS coordinates
- Extract BR route segments between km markers as WKT/SHP/GPKG
- Enrich CSV/XLS/XLSX tables with WKT geometries for QGIS import
- Export ICM workbooks to `gpkg` with a `marcos-km` point layer

## Installation

```bash
claude plugin install https://github.com/lievref/claude-custom-skills
```
