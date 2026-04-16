"""Test calibración Gemini vs GPT para actas electorales."""
from dotenv import load_dotenv
import os
import json

load_dotenv()

PROMPT = (
    "Eres un auditor electoral experto en actas peruanas ONPE.\n\n"
    "ESTRUCTURA DEL ACTA DE ESCRUTINIO:\n"
    "- La tabla tiene UNA SOLA columna de datos numéricos a la DERECHA: TOTAL DE VOTOS\n"
    "- A la IZQUIERDA de cada partido hay un número pequeño que es su POSICIÓN EN LA CÉDULA DE VOTACIÓN (sorteo ONPE). "
    "Este número NO es un voto. IGNÓRALO COMPLETAMENTE.\n"
    "- Los votos están escritos A MANO en la columna derecha dentro de recuadros.\n"
    "- Ejemplo: si ves '32 PARTIDO APRISTA PERUANO .... 2', el 32 es la posición y 2 son los votos.\n"
    "- Ejemplo: si ves '33 RENOVACIÓN POPULAR .... 70', el 33 es la posición y 70 son los votos.\n\n"
    "REGLA CRÍTICA: El número que aparece ANTES del nombre del partido NUNCA es un voto. "
    "SIEMPRE es la posición en la cédula. Los votos están en la columna de la DERECHA, dentro de casillas/recuadros.\n\n"
    "Extrae SOLO los votos (columna derecha) en este JSON:\n"
    '{"mesa": "numero", "distrito": "nombre", "total_electores_habiles": numero, '
    '"hora_inicio_escrutinio": "HH:MM p.m.", "hora_fin_escrutinio": "HH:MM p.m.", '
    '"votos": {"NOMBRE_PARTIDO": votos_int}, '
    '"total_ciudadanos_votaron": numero, "votos_blanco": numero, "votos_nulos": numero, '
    '"votos_impugnados": numero}\n'
    "Solo JSON, nada más."
)

REALES = {
    "RENOVACIÓN POPULAR": 70,
    "PARTIDO DEL BUEN GOBIERNO": 29,
    "FUERZA POPULAR": 19,
    "AHORA NACIÓN - AN": 11,
    "PRIMERO LA GENTE": 10,
    "PARTIDO PAÍS PARA TODOS": 9,
    "PARTIDO CÍVICO OBRAS": 8,
    "PARTIDO SICREO": 3,
    "PARTIDO APRISTA PERUANO": 2,
    "JUNTOS POR EL PERÚ": 2,
    "PARTIDO FRENTE DE LA ESPERANZA 2021": 2,
}

test_pdf = "data/MIRAFLORES/045012_ESCRUTINIO.pdf"

with open(test_pdf, "rb") as f:
    pdf_bytes = f.read()


def score(votos: dict, reales: dict) -> tuple[int, int]:
    ok = 0
    total = 0
    for partido, real in reales.items():
        for k, v in votos.items():
            if partido.upper()[:20] in k.upper():
                total += 1
                if v == real:
                    ok += 1
                else:
                    print(f"    MISS: {partido}: real={real} vs modelo={v}")
                break
    return ok, total


# === GEMINI ===
print("=== GEMINI 2.5 Flash ===")
try:
    from google import genai
    from google.genai import types

    gclient = genai.Client(api_key=os.environ["GEMINI_API_KEY"])
    gresp = gclient.models.generate_content(
        model="gemini-2.5-flash",
        contents=[
            types.Part.from_bytes(data=pdf_bytes, mime_type="application/pdf"),
            PROMPT,
        ],
    )
    gtext = gresp.text.strip().removeprefix("```json").removesuffix("```").strip()
    gdata = json.loads(gtext)
    gok, gtotal = score(gdata.get("votos", {}), REALES)
    print(f"  Score: {gok}/{gtotal}")
    print(f"  Blanco: {gdata.get('votos_blanco')} (real=4)")
    print(f"  Nulos: {gdata.get('votos_nulos')} (real=5)")
except Exception as e:
    print(f"  Error: {e}")

# === GEMINI PRO ===
print("\n=== GEMINI 2.5 Pro ===")
try:
    gresp2 = gclient.models.generate_content(
        model="gemini-2.5-pro",
        contents=[
            types.Part.from_bytes(data=pdf_bytes, mime_type="application/pdf"),
            PROMPT,
        ],
    )
    gtext2 = gresp2.text.strip().removeprefix("```json").removesuffix("```").strip()
    gdata2 = json.loads(gtext2)
    gok2, gtotal2 = score(gdata2.get("votos", {}), REALES)
    print(f"  Score: {gok2}/{gtotal2}")
    print(f"  Blanco: {gdata2.get('votos_blanco')} (real=4)")
    print(f"  Nulos: {gdata2.get('votos_nulos')} (real=5)")
except Exception as e:
    print(f"  Error: {e}")

# === GPT ===
print("\n=== GPT-4.1 Nano ===")
from openai import OpenAI

oclient = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
ofile = oclient.files.create(file=open(test_pdf, "rb"), purpose="assistants")
oresp = oclient.responses.create(
    model="gpt-4.1-nano",
    input=[{"role": "user", "content": [
        {"type": "input_file", "file_id": ofile.id},
        {"type": "input_text", "text": PROMPT},
    ]}],
)
otext = oresp.output_text.strip().removeprefix("```json").removesuffix("```").strip()
odata = json.loads(otext)
ook, ototal = score(odata.get("votos", {}), REALES)
print(f"  Score: {ook}/{ototal}")
print(f"  Blanco: {odata.get('votos_blanco')} (real=4)")
print(f"  Nulos: {odata.get('votos_nulos')} (real=5)")
