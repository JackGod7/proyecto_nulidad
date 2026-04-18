# Configs por operador

Cada archivo es un template `machine_config.json` listo para usar.

## Distribución (Lima Sur, gap descendente)

| Operador | Distritos | Actas pend. | Branches git |
|----------|-----------|-------------|--------------|
| Jack     | VMT, Chorrillos | ~1200 | `distrito/vmt`, `distrito/chorrillos` |
| Lynn     | SJM, VES | ~1100 | `distrito/sjm`, `distrito/ves` |
| Jim      | Pachacamac, Lurin | ~900 | `distrito/pachacamac`, `distrito/lurin` |
| Hector   | 5 balnearios | ~400 | `distrito/punta-hermosa`, `distrito/punta-negra`, `distrito/san-bartolo`, `distrito/santa-maria-del-mar`, `distrito/pucusana` |

## Aplicar config (una vez)

```bash
bash scripts/configurar_maquina.sh <operador>
```

Donde `<operador>` es uno de: `jack`, `lynn`, `jim`, `hector`.

El script copia `configs/<operador>.json` a `machine_config.json` en la raíz.
