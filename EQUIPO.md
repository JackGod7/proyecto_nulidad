# Guía para el Equipo — Auditoría Electoral ONPE 2026

## Setup (5 minutos)

### 1. Requisitos
- Python 3.12+
- Git
- 10GB disco libre mínimo

### 2. Clonar e instalar
```bash
git clone <URL_REPO>
cd proyecto_nulidad
pip install uv          # Si no tienes uv
uv sync                 # Instala dependencias
uv run playwright install chromium   # Instala browser
```

### 3. API Key de Gemini
Pedir a Tony el archivo `.env` con la key. Colocarlo en la raíz del proyecto.
**NO compartir la key por chat/email.** Solo en persona o por canal seguro.

## Ejecución

### Paso 1: Scraping de datos (30 min por distrito grande)
```bash
uv run python -c "
from src.scraping.browser_scraper import main
import asyncio
asyncio.run(main(fase=1, workers=3, filtro_distritos=['TU_DISTRITO_1', 'TU_DISTRITO_2']))
"
```

### Paso 2: Descarga de PDFs (~1 hora por distrito grande)
```bash
uv run python -c "
from src.scraping.browser_scraper import main
import asyncio
asyncio.run(main(fase=2, workers=2, filtro_distritos=['TU_DISTRITO_1']))
"
```

### Paso 3: Extracción AI con Gemini (cuando esté implementado)
```bash
uv run python -m src.extraction.gemini_extractor --distrito "TU_DISTRITO"
```

### Paso 4: Enviar resultados
Enviar el archivo `data/forensic.db` al coordinador (Tony).

## Asignación de Distritos

### Persona 1 (Tony) — Lima Centro
- MIRAFLORES ✅ (completado)
- SAN JUAN DE MIRAFLORES ✅ (completado)
- SAN ISIDRO
- SAN BORJA
- LA MOLINA
- SANTIAGO DE SURCO
- SURQUILLO
- BARRANCO
- JESÚS MARÍA
- MAGDALENA DEL MAR
- PUEBLO LIBRE
- LINCE
- SAN MIGUEL
- LA VICTORIA
- LIMA

### Persona 2 — Lima Norte + Este
- SAN JUAN DE LURIGANCHO (el más grande: 2,744 actas)
- COMAS
- LOS OLIVOS
- SAN MARTÍN DE PORRES
- INDEPENDENCIA
- CARABAYLLO
- PUENTE PIEDRA
- ATE
- SANTA ANITA
- EL AGUSTINO
- LURIGANCHO
- RÍMAC
- BREÑA
- SAN LUIS

### Persona 3 — Lima Sur + Periféricos
- VILLA EL SALVADOR
- VILLA MARÍA DEL TRIUNFO
- CHORRILLOS
- SAN JUAN DE MIRAFLORES (ya hecho por Persona 1)
- LURÍN
- PACHACÁMAC
- CIENEGUILLA
- PUCUSANA
- PUNTA HERMOSA
- PUNTA NEGRA
- SAN BARTOLO
- SANTA MARÍA DEL MAR
- SANTA ROSA
- ANCÓN
- CHACLACAYO

## Datos que se capturan por acta

- **38 partidos políticos** (votos de TODOS, no solo top 5)
- Estado del acta (Contabilizada, Pendiente, Para envío al JEE, Impugnada)
- Hora de instalación, escrutinio, sufragio (del PDF)
- SHA-256 de cada PDF descargado
- JSON crudo completo de la API
- Cadena de custodia (quién, cuándo, desde qué máquina)

## Verificar progreso
```bash
uv run python -c "
import sqlite3
conn = sqlite3.connect('data/forensic.db')
conn.row_factory = sqlite3.Row
for r in conn.execute('SELECT nombre, estado, procesadas, presidenciales FROM distritos ORDER BY nombre').fetchall():
    print(f'{r[\"estado\"]:20s} {r[\"nombre\"]:30s} {r[\"procesadas\"] or 0}/{r[\"presidenciales\"] or 0}')
conn.close()
"
```

## Consolidar resultados
El coordinador ejecuta el script de consolidación para unir las DBs de cada persona:
```bash
uv run python -m src.utils.consolidar --dbs persona1.db persona2.db persona3.db
```
