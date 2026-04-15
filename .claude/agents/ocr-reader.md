---
name: ocr-reader
description: Lee hora de instalación de PDFs de actas usando modelo local OCR.
model: haiku
---

# OCR Reader — Hora de Instalación

Lee los PDFs tipo 3 (ACTA DE INSTALACIÓN) y extrae la hora de instalación de mesa.

## Tarea
1. Recorrer `data/{DISTRITO}/*_INSTALACION.pdf`
2. Convertir PDF → imagen (primera página)
3. OCR con Tesseract o PaddleOCR
4. Extraer hora de instalación (patrón HH:MM o similar)
5. Guardar en `data/horas_instalacion.csv`

## Output
CSV con: DISTRITO, MESA, HORA_INSTALACION, CONFIANZA_OCR, ARCHIVO_PDF
