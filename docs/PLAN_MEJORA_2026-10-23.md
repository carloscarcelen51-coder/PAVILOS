# PAVILOS — Plan de mejora: medición veraz y calibración

Fecha: 2026-10-23. Parte del análisis de los trades del 21 y 23-10 (23.695 trades del 8-06 al 23-10) y de los
pendientes de [`PENDIENTES.md`](PENDIENTES.md). El orden importa: **primero medir bien, después calibrar, y calibrar
fuera de línea**. Nada de los datos se borra: lo que no es comparable se **marca**.

## 1. Diagnóstico

| Medida (sin la puesta en marcha del 8–9 jun) | Valor |
|---|---|
| Ganancia bruta por trade | **+0,72 pb** (IC 95 %: +0,66 a +0,78) |
| Comisiones | **10,00 pb** (5 de entrada + 5 de salida) |
| Neto por trade | **−9,3 pb**, igual en todos los meses, horas, días, duraciones y distancias de stop |
| Win rate | 2,6 % (para no perder harían falta ~60 % con la ganancia y pérdida medias actuales) |
| Trades que se mueven ≥ 10 pb a favor | 2,7 % |
| Duración mediana | 9,7 s; el 15,8 % dura menos de 1 s |
| Stop inicial (deducido del tamaño) | mediana 54,5 pb |
| Largos / cortos | 99,6 % / 0,4 % |

**Por qué pierde** (mecanismos con los parámetros actuales):
1. **El objetivo es menor que el coste.** Sale cuando hay una resistencia confiable (≥ 0,7) a ≤ 8 pb
   (`opposing_distance_bps`), y la entrada solo exige que no esté ya a ≤ 8 pb del precio de armado. Para cubrir 10 pb
   la resistencia tendría que estar a ≥ ~20 pb.
2. **Salidas inmediatas.** La entrada se llena 2 pb por encima del precio de armado (`entry_offset_bps`), así que una
   resistencia a 8–10 pb queda dentro del margen de salida en el tick siguiente.
3. **La confianza de las zonas se satura** con 30 s de vida, 3 exchanges y fuerza 15: casi siempre hay una resistencia
   «fiable» cerca.
4. **Riesgo y beneficio al revés:** stop a ~54 pb (borde inferior de zonas de hasta 50 pb) para ganar ~1 pb.
5. **ATR de 10 s** (50 ticks a 5 Hz): el suelo de 3×ATR sale ~1 pb y no protege.
6. **Sesgo largo**, con el principal sospechoso en la conversión USDT = 1,0 (USDT a −12 pb el 23-10): los libros en
   USDT quedaban por encima de los de dólares y creaban soportes artificiales pegados al precio.
7. **Frecuencia:** 169 trades al día con una ventaja menor que el coste.

**Qué distorsionaba la medición:**
- **Cuenta repuesta en cada arranque.** Hasta el 23-10, cada arranque reponía la cuenta a 10.000 € y perdía la
  posición abierta (unas 2 en total, estimado). Por eso el P&L registrado suma una cuenta por sesión: 31 sesiones con
  trades.
- **Tamaños no comparables.** El tamaño era el 1 % del capital de la sesión, que se iba gastando: 4.471 trades se
  abrieron con menos de 1.000 € de nocional.
- **Días con el programa apagado.** Los ratios diarios los contaban como días a 0 €.
- **Puesta en marcha.** Hasta el reinicio del 9-06 13:20Z se estaban añadiendo exchanges: 758 trades.
- **Precio de la simulación.** El P&L se simula sobre el precio medio spot combinado, no sobre el contrato de futuros
  (PF_XBTUSD) que se supone que se opera.

## 2. Tandas

### A — Conexiones con fecha límite ✅ hecho 23-10
- **OKX al puerto 443:** OKX cierra el 8443 el 31-10. Verificado en vivo: snapshot de 400 niveles y actualizaciones.
- **htx solo spot:** sus derivados en `api.hbdm.vn`, interceptado por el proveedor a ratos, tumbaban su
  `load_markets`. Verificado en vivo: 2.175 mercados spot en 2 s y libro BTC/USDT correcto.

### B — Datos veraces del histórico ✅ hecho 23-10
- **`/trades` cuenta por defecto cada trade a 10.000 €** (el resultado en pb no cambia), y con un botón se ven los
  importes registrados. La puesta en marcha (8–9 jun) se excluye por defecto, también con un botón.
- **Los ratios diarios** (Sharpe, Sortino, volatilidad) cuentan solo los días con el programa en marcha.
- **Sesiones visibles:**
  - líneas verticales en la curva para cada arranque;
  - la pestaña «Sesión» del desglose, con el P&L registrado y el nocional al principio y al final de cada sesión.
- **Resumen de la cuenta veraz:** neto por trade en pb, resultado a 10.000 € por trade y registrado, con el número de
  sesiones.
- **«Cómo leer estas cifras»** en la propia página.
- **`scripts/audit_trades.py`** es solo lectura:
  - reconstruye las sesiones y da el informe en pb;
  - con `--csv`, exporta los trades anotados (sesión, capital previo, stop deducido, tope de apalancamiento, puesta en
    marcha, P&L a 10.000 €).
- **Los arranques de junio, anteriores al supervisor,** se deducen del log con el tope de apalancamiento y quedan
  documentados en `src/pavilos/web/history.py`.

### C — Medición veraz a partir de ahora ✅ hecho 23-10
- **Tamaño fijo de 10.000 €** por trade (`fixed_notional`), sin depender del capital: los resultados son comparables y
  no hay trades «residuales».
- **Cuenta paper continua.** `paper_state.json` guarda el capital y la posición abierta en cada entrada y salida. Al
  arrancar se recupera, y una posición que quedó abierta se cierra al primer precio con el motivo **`restart`**:
  queda registrada y no se pierde.
- **Contexto de entrada en cada trade:**
  - stop y espacio hasta el objetivo;
  - confianza, persistencia, exchanges y fuerza de la zona;
  - distancias, ATR, número de zonas y capital.
  - **Para qué:** permite calibrar a partir del propio log. En `/trades` aparecen como columnas («Stop pb»,
    «Espacio pb», «Conf. zona») y rangos de filtro.
- **USDT y USDC a su precio real:** Kraken cada 30 s, en lugar de 1,0. Cada lectura se guarda en
  `D:\pavilos_book_data\_pegs.jsonl` para poder corregir el replay del lake (que guarda los libros sin convertir).

**Criterios de aceptación tras el despliegue:**
- el log muestra lecturas `peg:`;
- se crea `paper_state.json` en el primer trade;
- los trades nuevos llevan `context` y son de 10.000 €.

### D — Ejecución realista sobre Kraken Futures ⏳ pendiente (decisión del operador)
- **Grabar PF_XBTUSD:** libro, trades, ticker y funding. Raíz y proceso separados del lake spot, según DER-16 P1;
  unos 1–2 GB al día.
- **Simular los llenados del broker sobre el precio de futuros,** con su diferencial (spread) y el funding real.
- **Criterio:** el P&L paper se mide sobre el instrumento que se operaría; se comparan las dos mediciones (spot y
  futuros) durante unas semanas.
- **Coste:** un conector nuevo y su grabación (≈ 1 día de trabajo) y otra pieza más en marcha en el PC.

### E — Calibración fuera de línea ⏳ pendiente (decisión del operador: coste de CPU)

**1. Parámetros nuevos**, todos con un valor por defecto que mantiene el comportamiento actual:

| Parámetro | Hoy | Probar |
|---|---|---|
| Espacio mínimo hasta el objetivo, medido desde el llenado (nuevo) | — | 15 / 20 / 30 / 40 pb |
| Salida (`opposing_distance_bps`, tipo de salida) | resistencia a 8 pb | 0 / 2 / 4 pb; trailing stop; «cabalgada»; take-profit en múltiplos del riesgo |
| `entry_offset_bps` | 2 | 0 / 1 / 5 |
| `entry_threshold` / `min_persistence_s` / `min_venues` | 0,6 / 10 s / 2 | 0,8–0,9 / 30–120 s / 4–6 |
| Ancla del stop | borde inferior de la zona | borde superior |
| ATR | 50 ticks | ATR(14) de velas de 1 min |
| `entry_mode` | momentum | reversion (`tp_mult` 1–3) |
| Ejecución | a mercado (5 pb) | orden límite en la entrada (2 pb) |

**2. Replay del lake con la conversión USDT corregida:**
- desde el 23-10, con `_pegs.jsonl`;
- antes, con una serie histórica de USDT/USD descargada aparte;
- sin corregir no sirve: el sesgo de 12 pb domina las señales de ±10 pb.

**3. Barrido y validación:**
- barrido de parámetros con walk-forward (fuera de muestra) sobre los datos sin pérdidas desde el 7-10 (más de dos
  semanas);
- es pesado: se ejecuta en serie, con prioridad baja y sin recorrer el lake en paralelo, porque el análisis del 17-10
  ya degradó el grabador;
- primero se estima el tiempo con una ventana pequeña.

**4. Criterio para tocar el vivo:** neto > 0 fuera de muestra con cientos de trades y en varias ventanas. Objetivo
realista: hace falta un bruto de 12–15 pb por trade (hoy 0,7), así que se buscan entradas mucho más selectivas y
recorridos largos, no ajustes finos.

### F — Calidad de los datos del lake ⏳ pendiente (ver `PENDIENTES.md`, puntos 4 y 6–19)
- **ccxt:** pierde el 44–95 % de las actualizaciones por coalescencia [R2], recarga los mercados en cada reconexión [R4]
  y bitfinex reconecta en bucle.
- **Reloj del PC** 250 ms detrás de UTC [R7].
- **Rendimiento:** el CRC de Kraken, el escritor, los checkpoints y la compactación horaria.

### G — Expansión ⏳ después de D, E y F
- Futuros P1–P4, condicionantes del precio como filtros, pares nuevos (SOL, ETH, DOGE, XRP): ver PENDIENTES 20–23.

## 3. Decisiones pendientes del operador
1. **Tanda D:** ¿grabar Kraken Futures y medir el paper sobre él?
2. **Tanda E:** ¿lanzar el barrido de calibración? Primero estimo el tiempo de CPU con una ventana pequeña.
3. **`.tmp/` del repo:** 17 GB de volcados viejos de DuckDB. ¿Se borra?
4. **€ o USD** en las dos páginas: los importes son dólares.
5. **Segunda copia de seguridad** fuera del USB G:.
