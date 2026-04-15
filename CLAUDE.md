# Proyecto Nulidad — Scraper ONPE Elecciones 2026

## Objetivo
Scraper de resultados electorales ONPE para Lima Provincia (43 distritos).
Extrae votos mesa-por-mesa, descarga PDFs de actas, OCR hora de instalación.

## Stack
- **Python 3.12+** (uv)
- **httpx** — HTTP async
- **pandas** — dataset
- **Tesseract/PaddleOCR** — OCR local para hora instalación

## Comandos
```bash
uv run python src/scraper.py          # Ejecutar scraper completo
uv run python src/scraper.py --test   # Solo 1 distrito (Ancón)
uv run python src/ocr_hora.py         # OCR sobre actas instalación
uv run pytest tests/ -v               # Tests
```

## API ONPE — Endpoints Mapeados

Base: `https://resultadoelectoral.onpe.gob.pe/presentacion-backend`

| Endpoint | Uso |
|----------|-----|
| `ubigeos/departamentos?idEleccion=10&idAmbitoGeografico=1` | Regiones |
| `ubigeos/provincias?...&idUbigeoDepartamento=140000` | Provincias Lima |
| `ubigeos/distritos?...&idUbigeoProvincia=140100` | 43 distritos Lima Prov |
| `actas?pagina={p}&tamanio=50&idAmbitoGeografico=1&idUbigeo={ubigeo}` | Actas paginadas |
| `actas/{id}` | Detalle: votos, archivos, metadata |
| `actas/file?id={archivoId}` | URL firmada S3 para PDF |

## IDs clave
- idEleccion=10 → Presidencial
- idAmbitoGeografico=1 → Perú
- Lima región=140000, Lima provincia=140100
- Acta ID = mesa + ubigeo + eleccion + tipo (ej: 3699914010210)

## Campos por acta
- `nvotos` → votos por partido
- `archivos[].tipo`: 1=ESCRUTINIO, 3=INSTALACIÓN, 4=SUFRAGIO
- Estado: "Contabilizada" (con datos) | "Pendiente" (sin datos/PDFs)

## Top 5 candidatos
| Key | Nombre | Partido |
|-----|--------|---------|
| Voto_Keiko | KEIKO SOFIA FUJIMORI HIGUCHI | FUERZA POPULAR |
| Voto_Rafael | RAFAEL BERNARDO LÓPEZ ALIAGA CAZORLA | RENOVACIÓN POPULAR |
| Voto_Nieto | JORGE NIETO MONTESINOS | PARTIDO DEL BUEN GOBIERNO |
| Voto_Belmont | RICARDO PABLO BELMONT CASSINELLI | PARTIDO CÍVICO OBRAS |
| Voto_Roberto | ROBERTO HELBERT SANCHEZ PALOMINO | JUNTOS POR EL PERÚ |

## Estructura de salida
```
data/
├── {DISTRITO}/
│   ├── {mesa}_ESCRUTINIO.pdf
│   ├── {mesa}_INSTALACION.pdf
│   └── {mesa}_SUFRAGIO.pdf
├── dataset_lima_provincia.csv
├── actas_sin_pdf.csv          ← actas pendientes (preocupante)
└── horas_instalacion.csv      ← output OCR
```

## Convenciones
- RTK para TODOS los comandos bash
- Modo cavernicola: 0 relleno, 0 cortesía, max 5 palabras por punto
- Python: type hints, httpx async, logging no print
- Archivos < 400 líneas
- Immutable data patterns
- Rate limit: 1 req/seg a ONPE (respetar servidor público)
