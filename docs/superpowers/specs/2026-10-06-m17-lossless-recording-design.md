# M17 — Grabación sin pérdidas, compacta y con retención eterna

**Fecha:** 2026-10-06/07 · **Estado:** construido; 3 barreras (spec, calidad, adversarial) + re-verificación + revisión
final + 4 rondas de arreglos

## 1. Problema (medido sobre el lake real, 09-26 → 10-06)

| Hallazgo | Dato |
|---|---|
| Volumen | 250,8 GB en 11 días (~24-30 GB/día) |
| Reparto | kucoin 79,5 % · gemini 13,3 % · mexc 3,8 % → **96,6 %** son 3 venues ccxt |
| Causa | ccxt entrega el libro **entero** en cada mensaje y se graba entero (kucoin 16.437 filas × ~5/s) |
| Disco | D: (932 GB, compartido con otros proyectos) 262 GB libres → **lleno en ~11 días** si no hay reinicio |
| Retención | la poda (7 días) **solo corre al arrancar**: entre reinicios el lake crece sin límite |
| **Ya ocurrió** | el recorder llevaba 14 días sin reiniciarse → **el lake llenó D:** del 28-09 15:46 al 29-09 16:24 UTC: 47.297 escrituras fallidas («no space left on device»), los 14 venues, ~1 de cada 5 lotes perdido. Ese tramo sigue en el lake con huecos; lo vació la poda al reiniciar. Ninguna ventana de investigación lo usó (eran del 08/09-06) |
| Fidelidad del replay | los venues nativos (kraken, binance, okx, bybit, bitstamp) **nunca** mandan snapshot salvo al conectar → el replay de una ventana a mitad de sesión reconstruye sus libros **solo con los deltas de la ventana** (libros parciales; faltan los niveles que no cambian) |
| Pérdidas silenciosas | el recorder descarta updates con la cola llena (`dropped`) sin log ni métrica; los fallos de escritura solo dejaban trazas |
| Log | `recorder_supervisor.log` = 97 MB y creciendo (sin rotación) |
| Reloj | el `ts` es monótono por venue **salvo** binance (semilla REST con hora del PC, deltas con hora del exchange `E`; el PC iba ~323 ms por detrás de binance el 10-06 y la sincronización horaria de Windows no está activa) y los saltos del reloj del PC |

## 2. Diseño

El path en vivo solo cambia en dos puntos: el agregador **añade** un checkpoint que entrega al recorder sin aplicarlo, y
los deltas de binance llevan la hora de **recepción** (como la semilla y los otros 13 venues).

1. **`BookUpdate.checkpoint: bool = False`** — campo nuevo con valor por defecto (retrocompatible).
2. **Checkpoints del agregador** — cada `checkpoint_every_s` (60 s), para cada venue **activo**, entrega a `on_update`
   una foto completa de su libro vivo: `is_snapshot=True, checkpoint=True, ts=book.last_ts`. Nunca se aplica.
   El calendario sigue el reloj inyectado y se **rearma si ese reloj salta hacia atrás**; `checkpoint_every_s` debe ser
   finito y > 0 (se valida al construir el `Engine`).
3. **`BookEncoder` en el hilo escritor del recorder** — espejo por venue con la misma normalización que `BookState`:
   - checkpoint → **foto completa**; snapshot normal → **diferencia** frente al espejo; delta → normalizado;
     lo que quedaría vacío → **heartbeat** (1 fila no-op) para que el replay refresque `last_ts`.
   - **Orden:** el `ts` escrito nunca retrocede dentro de un venue ni baja del **inicio de la sesión** (`set_floor`);
     los empates conservan el orden de llegada por `seq_no`. Si el reloj del PC retrocede más de 2 s se avisa (WARNING,
     una vez por episodio; CRITICAL y aviso en el dashboard si pasa de un minuto) y el `ts` queda fijo hasta que el reloj
     lo alcanza.
   - **Base:** la primera escritura de un venue y la primera tras **más de `staleness_s` (15 s) de silencio**
     (reconexión, atasco) son el **libro completo** del espejo.
   - **Pérdida:** tras `rebase()` — fallo de escritura, update que no se pudo codificar, lote fallido, fin de pausa —
     la siguiente escritura del venue es su libro completo **más una marca de pérdida** (un nivel de precio no finito que
     `BookState` descarta). Un libro **vacío** tras una pérdida se escribe como la marca sola (vacío explícito): el lake
     podría conservar niveles que la escritura perdida habría borrado.
   - **Invariante**: aplicar lo escrito reproduce el `BookState` vivo — niveles siempre; `last_ts` (el máximo del venue)
     siempre que el libro tenga mid.
4. **Replay** (`iter_lake` + `replay_snapshots`):
   - **warm-up** desde `t0 − warmup_s` (120 s): aplica todo, no emite antes de `t0`, ancla la cadencia en el primer
     update ≥ `t0`. Garantiza al menos un checkpoint (o una base tras silencio) de cada venue activo.
   - lee **solo las particiones de hora** que pueden contener la ventana (exacto: las filas se particionan por su `ts`).
   - un Parquet **ilegible** (truncado, vacío o con el footer dañado) se salta con aviso en lugar de romper el replay.
   - **sesiones del recorder:** cada arranque añade su inicio a `<lake>/_runs.jsonl` (nunca anterior a la fila más nueva
     del lake ni al último inicio marcado, aunque el reloj del PC haya retrocedido o un run muriera sin escribir; CRITICAL si
     eso lo adelanta más de un minuto) y el recorder no sella nada por debajo. El fichero tolera líneas rotas o basura. El replay **resetea todos
     los libros en cada inicio marcado**: ni conserva libros de un proceso muerto ni borra los que el nuevo ya escribió.
   - `exclude=` deja fuera venues; los tres scripts de investigación usan este replay en lugar de copias propias.
5. **Retención eterna** — **nada se borra automáticamente** (`book_retention_days=None`; borrar por edad es opcional).
   **Suelo de disco** (`book_min_free_gb=100`, GiB como los muestra Windows): si D: baja de ese libre, el recorder
   **se pausa**: sigue **codificando** (su espejo = el libro vivo) pero no escribe; al reanudar, la siguiente escritura de
   cada venue es su libro completo con marca de pérdida, así que el lake vuelve a ser exacto **en el acto**. Pausa y
   reanudación se registran en CRITICAL (llegan también al log del supervisor) y se ven en el dashboard; el conserje
   registra ERROR en **cada pasada** mientras falta espacio y WARNING al recuperarlo. Lo comprueba al arrancar (antes de
   grabar) y **cada 10 min**; ninguna excepción puede matarlo.
   *Descartado:* borrar lo más antiguo al tocar el suelo — un proceso ajeno desbocado en D: se llevaría toda la historia.
6. **Observabilidad** — el recorder registra `dropped` (el primero al momento, luego como mucho uno por minuto), pausas,
   **fallos de escritura** (contados; CRITICAL al empezar y cuando llevan un intervalo completo sin fallar — los fallos
   intermitentes no hacen aletear el aviso —; ERROR con traza como mucho uno por minuto) y un resumen por minuto que solo
   cuenta lo realmente escrito. El dashboard muestra «recording paused · disk floor», «recording write errors (N)» o
   «recording dropped N» (estos dos, solo si ocurrieron en los últimos 10 min) o «recording clock N s behind». El
   proceso registra en `logs/pavilos.log` rotativo (20 MB + 5 copias): el fichero vivo se renombra a
   `pavilos.<UTC con microsegundos>.log` (nombre nuevo: nada se sobrescribe ni se encadena) y se borran las copias más
   viejas que sobran; si otro proceso tiene abierto el fichero vivo no se toca nada y se reintenta en un minuto, y una
   copia vieja abierta simplemente se borra más tarde; si `logs/` no se puede usar, cae a stderr WARNING+ en vez de no
   arrancar. Al log del supervisor solo llega CRITICAL y los crashes; uvicorn escribe por nuestros handlers.
7. **Ficheros atómicos** — cada Parquet se escribe como `*.parquet.tmp` y se renombra; un fallo normal (disco lleno)
   borra el `.tmp`, y un proceso matado a mitad deja solo un `.tmp` ignorable, nunca un `.parquet` truncado.
8. **Compactación segura** — solo horas terminadas hace ≥ 15 min; la regla 1 ya no borra a ciegas: los crudos que
   `compacted.parquet` **contiene** se borran (una consulta para todos), los que **no** contiene se **fusionan**
   (verificar → promover → borrar); una hora con un fichero **ilegible** se deja intacta y se informa; un COPY o un
   `os.replace` fallidos no dejan temporales; **una ejecución a la vez** (`_compaction.lock`; si una ejecución muere a la
   fuerza, el lock se borra a mano). DuckDB (compactación, replay, `check_lake`, `book_query`, `analyze`) usa memoria
   acotada y vuelca en la carpeta temporal del sistema, no en `./.tmp` del repo.

## 3. Verificación

- Test de propiedad (encoder ≡ `BookState` vivo) con duplicados, no-finitos, libros vacíos, checkpoints, **saltos de
  reloj hacia atrás y silencios**; ida y vuelta real recorder → Parquet → replay (incluida una caída de venue y
  reinicios marcados); tests de mutación sobre cada pieza.
- 3 barreras independientes + re-verificación de sus hallazgos tras los arreglos.
- **En vivo tras desplegar**: `python -m scripts.check_lake <dir> <t0> <t1>` — en cada checkpoint, el libro reconstruido
  justo antes debe ser **idéntico** (esperado: 0 discrepancias, 0 reordenados). Un libro con marca de pérdida se compara
  siempre y su diferencia se cuenta aparte (`loss_mismatches`: esperada, ya avisada por el recorder); los libros completos
  tras un silencio y los inicios de `_runs.jsonl` son bases. Limitación: drops de cola de más de 15 s que acaban justo en un
  checkpoint se confunden con un silencio (el contador de drops del recorder sí los reporta).

## 4. Efecto esperado

Escritor: ~4-6 % de un núcleo frente a ~31-38 % (lote real de 5 s, barrera 2); retraso p99 del event loop 46 ms frente a
175 ms; replay de 10 min: 0,06 s frente a 5,2 s en frío. Volumen: checkpoints ~0,47 GB/día; diferencias **a medir tras
desplegar** (estimación sintética 2-7 GB/día; la referencia antes de desplegar era ~10 GB/día a las 22:50 y ~25 GB/día
de media). Con el suelo en 100 GiB (107 GB) el margen es de **semanas a meses** según lo que se mida — de ahí la
prioridad de M17b.

## 5. Fuera de alcance (siguiente: M17b)

Convertir el lake existente (diferencias en las particiones ccxt + inyectar checkpoints en los nativos desde su snapshot
de conexión): libera ~240 GB y hace exactas las ventanas antiguas. Capa eterna derivada (mejor bid/ask, velas) y cinta de
trades. Limpieza de `.tmp/` del repo (17 GB de volcados de DuckDB del 09-06; pendiente de autorización del operador).
Conocido y aceptado: un fichero con el contenido corrupto por dentro (no truncado, footer legible) aún hace fallar el
replay de su hora — solo con corrupción de disco; el encoder repite la normalización de `BookState` (el test de propiedad
la fija).
