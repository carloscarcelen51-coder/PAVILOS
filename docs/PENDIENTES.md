# PAVILOS — Pendientes

Actualizado: 2026-10-23. Fuente de cada punto entre corchetes: los códigos (F, DER, R, RA) son hallazgos del análisis
del 2026-10-17, con evidencia completa en [`docs/analisis/2026-10-17-informes-parciales.md`](analisis/2026-10-17-informes-parciales.md).
**Esos informes aún no han pasado la verificación adversarial**: confirmar cada punto antes de cambiar código.
**El orden de trabajo y las decisiones pendientes están en [`PLAN_MEJORA_2026-10-23.md`](PLAN_MEJORA_2026-10-23.md).**

## 🔴 Urgente (con fecha)

1. ✅ **OKX al puerto 443** (hecho el 23-10, verificado en vivo antes del cierre del 8443 el 31-10). [DER-01]
2. **Espacio en D:** 106 GiB libres el 23-10. Desde el 23-10 la grabación se pausa por debajo de **20 GiB** (antes
   100; decisión del operador) y no borra nada. Otros
   proyectos consumen D: a ráfagas. **El operador lo resuelve (21-10).** Propuesta técnica: lake frío (pre-M17) en L:,
   caliente en un SSD con suelo propio. [RA-01, RA-05]

## 🟠 Calidad de los datos BTC (antes de añadir nada)

3. **USDT ≠ USD.** Hecho el 23-10: el agregador recibe USDT/USD y USDC/USD reales de Kraken cada 30 s (antes 1,0;
   USDT estaba a −12,1 pb) y cada lectura se guarda en `D:\pavilos_book_data\_pegs.jsonl`. **Falta** corregir el
   replay: desde el 23-10 con `_pegs.jsonl` y, antes, con una serie histórica de USDT/USD. Sin eso, los backtests sobre
   el lake siguen sesgados unos 12 pb. Falta también un respaldo de la fuente (Coinbase o Bitstamp). [F01, DER-03]
4. **ccxt pierde actualizaciones por coalescencia.** Mensajes fusionados (se pierden en el lake): kucoin 95 % (273 msg/s
   llegan y quedan 14,3/s), cryptocom 91 %, bitfinex 84 %, gemini 79 %, gate 66 %, bitget 55 %, mexc 44 %. Además ese
   camino gasta ~40 % del tiempo de CPU. Arreglo: emitir un delta por mensaje desde ccxt (hook en `OrderBookSide`) o
   pasar a conectores nativos. [R2 BLOCKER, RA-02]
5. ✅ **htx solo carga spot** (hecho el 23-10, verificado en vivo). Sus derivados en `api.hbdm.vn`, interceptado a ratos
   por el proveedor, le hacían fallar `load_markets`. Hay que vigilar sus reconexiones en los próximos días.
   [R3, DER-11, RA-07, F12]
6. **Cada reconexión ccxt recarga todos los mercados** dentro del bucle: gate 5,5 s de CPU por recarga, bitget 3,6 s,
   kucoin 2,5 s; ~190 MB. Reutilizar la instancia y los mercados y cargar solo spot. [R4]
7. **bitfinex:** 3.291 reconexiones del 07-10 al 17-10 (ChecksumError, KeyError y TypeError de ccxt). Actualizar ccxt
   o pasar a conector nativo. [auditoría ccxt pendiente]
8. **La carga externa del PC degrada el recorder.** Durante el análisis hubo paradas del bucle de 0,5-12 s: memoria al
   96-97 %, Defender usando 2,6-5,6 núcleos y agentes en paralelo. Propuesto, a decidir por el operador: prioridad
   ABOVE_NORMAL para el recorder, exclusión de Defender para el lake y métrica de retraso del bucle. [R1, RA-02]
9. **Reloj del PC ~250 ms detrás de UTC**, con W32Time en marcha pero sin sincronizar. Activar NTP (decisión del
   operador; requiere admin) y guardar siempre la hora del exchange. [R7, F14, DER-10]

## 🟡 Rendimiento y almacenamiento (sin perder datos)

10. Kraken: el libro de cadenas para el CRC cuesta 1,1 ms por mensaje; con índice ordenado baja a 21,6 µs y el CRC sale
    idéntico. [R5]
11. Escritor: el ciclo real es de 8,55 s (configurado 5 s) por construir las filas como dicts; pasar a columnas. [R6a]
12. Los checkpoints son el 55 % de los bytes (el 90 % en coinbase). Proponen checkpoints escalonados sin pérdida. [R6b]
13. Ficheros: ~148.000 al día. Compactación horaria automática y vaciado de 30-60 s para los streams pequeños.
    [RA-04, RA-08, DER-09]
14. El tick a 5 Hz recorre 87.128 niveles (22-31 ms por tick): cachear el mejor bid/ask, usar numpy y bajar a 1 Hz lo
    que es solo del dashboard. Además `summarize()` recorre los ~23.000 trades a 5 Hz: hacerlo incremental. [R8, R9]
15. **M17b:** lake caliente/frío y conversión del lake pre-M17, que libera ~240 GB. [RA-04]
16. **Varios procesos antes de añadir pares:** el proceso actual no aguanta BTC+ETH+SOL (probado). [RA-03, RA-06, R12]
17. La retención eterna supera la capacidad a partir del escenario S2: hace falta un HDD de 16-20 TB para frío más una
    segunda copia. Decisión de compra del operador. [RA-09]
18. ✅ **Vaciar el escritor al apagar Windows** (hecho el 23-10). No sirve `CTRL_SHUTDOWN_EVENT`: el proceso carga
    user32, así que Windows avisa con `WM_ENDSESSION` a una ventana oculta. **Falta** generar `_gaps.json`
    automáticamente. [RA-11]
19. `.tmp/` del repo: 17 GB de volcados viejos de DuckDB. Borrar solo con el OK del operador.

## 🔵 Expansión (después de lo anterior)

20. **Futuros y derivados** [DER-16]. Todos los venues son accesibles desde España (17-10).
    - **P0:** puerto de OKX, serie USDT/USD y backfill diario gratuito (data.binance.vision, analytics de Kraken Futures).
    - **P1:** Binance USDM (depth en conexión propia; aggTrade, markPrice y forceOrder por `/market`) y Kraken Futures
      (PF_XBTUSD y PF_SOLUSD: libro, trades y ticker). Kraken Futures es el venue de ejecución y hoy no se graba: el
      broker simula con el mid spot y funding 0.
    - **P2:** Bybit (orderbook.1000) y Deribit (DVOL).
    - **P3:** OKX e Hyperliquid.
    - **P4:** Bitget y Gate, solo si hace falta.
    - **Montaje:** raíz y proceso separados del lake spot y fuera de D:. Unos 4,8 GB/día en total.
    - **Valor medido:** el basis futuros-spot predice el spot con IC 0,58-0,62 a 5 s y una diferencia entre quintiles de
      3,0-3,6 pb a 30-60 s. El CVD de futuros es casi independiente de la presión spot (correlación 0,05-0,18). Está por
      debajo de las comisiones (10 pb): sirve de piloto o filtro, no es una ventaja por sí solo.
21. **Condicionantes del precio** [F02-F11]. Ninguno supera por sí solo los 10 pb a 5-60 min.
    - **Usos:** como filtro de régimen y volatilidad (DVOL, |ΔOI|, liquidaciones, volumen relativo); bloquear entradas
      de T−5 a T+30 min en CPI, NFP y FOMC; tratar el fin de semana aparte.
    - **Qué grabar en vivo:** liquidaciones (3 venues) y trades de Kraken Futures y de los perps principales.
    - **Qué descartar:** primas de Coinbase y coreana, ETF, on-chain, ballenas y noticias.
22. **Pares nuevos:** candidatos SOL, ETH, DOGE y XRP, más un experimento con SUI, AVAX, LINK o PEPE. Falta la medición
    real: el área `research-pairs` no llegó a terminar.
23. **Re-test de la cabalgada** (17 operaciones en junio): con el peg corregido, con futuros (basis + CVD) y con más días.

## ⚪ Menores (decisión del operador)

24. ✅ **Enlace a `/trades` desde el panel en vivo** (hecho el 21-10, a petición del operador): las dos páginas llevan
    en la cabecera las pestañas «Panel en vivo» y «Trades históricos». Es el único cambio en `index.html`.
25. **Moneda de los importes.** El panel en vivo pinta el P&L en €, pero el broker modela un perpetuo lineal en USD
    (PF_XBTUSD) y los precios son BTC/USD: los importes son dólares. `/trades` usa € por coherencia con el panel.
    Para corregirlo hay que cambiar las dos páginas a la vez.

## ⏸️ Análisis del 2026-10-17 a completar

Terminaron 4 de 12 áreas (runtime, almacenamiento, derivados y condicionantes). Las otras se cortaron por el límite
semanal de uso:
- **Áreas sin hacer:** `audit-native`, `audit-ccxt`, `audit-lake`, `audit-crosscheck`, `research-pairs`,
  `research-empirical-pairs`, `research-empirical-derivs` y `research-execution`.
- **Verificaciones adversariales:** ninguna se hizo, tampoco las de las 4 áreas terminadas.
- **Plan sintetizado y crítica:** no se llegaron a hacer.

Script del flujo: `C:\Users\FSOCIETY\.claude\projects\C--Users-FSOCIETY-Desktop-APPS-PAVILOS\98572d2b-7b5a-44be-8d1e-c0e6adfa3c4c\workflows\scripts\pavilos-audit-y-plan-expansion-wf_c4578dae-a43.js`
(run `wf_c4578dae-a43`). Al relanzarlo: como mucho 3 agentes a la vez, con prioridad baja y sin recorrer el lake en
paralelo. El propio análisis degradó el recorder [R1].

## ✅ Hecho recientemente

- 2026-10-07: M17 desplegado (grabación sin pérdidas y retención eterna).
- 2026-10-17: copia completa a `G:\PAVILOS_BACKUP` (lake: 1.842.566 ficheros, 319,1 GB, 0 errores).
- 2026-10-21: instantánea del repo y puesta al día del lake en `G:\PAVILOS_BACKUP` (ver `LEEME.txt`).
- 2026-10-21: pestaña de trades históricos en http://127.0.0.1:8800/trades. No toca la página principal.
  - **Datos:** todos los trades sin paginación, en una tabla virtual. Se actualiza cada 5 s y exporta a CSV.
  - **Filtros:** periodo, par, lado, resultado, motivo, 8 rangos, hora, días y búsqueda; quedan guardados en la URL.
  - **Métricas y gráficos:** unas 60 métricas, 5 gráficos y desgloses por día, semana, mes, año, motivo, lado, par,
    hora, día de la semana y duración. Las métricas se contrastan en los tests con Python y con `summarize()`.
  - **Formato del log:** desde aquí cada línea lleva `symbol`. Un código anterior a este commit descartaría esas
    líneas al leer, así que no se debe volver atrás sin tenerlo en cuenta. `load()` ya ignora campos desconocidos para
    el futuro.
- 2026-10-23: suelo de disco de D: bajado de 100 a 20 GiB (decisión del operador).
- 2026-10-23: medición veraz (tandas A, B y C del [plan](PLAN_MEJORA_2026-10-23.md)):
  - **Conexiones:** OKX por el 443 y htx solo spot.
  - **Paper:** opera con 10.000 € fijos por trade y la cuenta continúa entre arranques (`paper_state.json`). Una
    posición abierta al apagar se cierra al volver con el motivo `restart`.
  - **Contexto:** cada trade guarda su contexto de entrada.
  - **USDT/USDC:** a su precio real desde Kraken (`_pegs.jsonl`).
  - **`/trades`:** cuenta cada trade a 10.000 € por defecto, excluye la puesta en marcha (8–9 jun), muestra las sesiones
    y la guía «Cómo leer estas cifras», y calcula los ratios solo con los días en marcha.
  - **Auditoría:** `scripts/audit_trades.py` (solo lectura) reconstruye las sesiones y exporta los trades anotados.
- 2026-10-23: mediciones y persistencia.
  - **Mediciones:** las dos páginas cuentan por defecto desde el inicio de la medición actual (23-10 22:44); «Todo»
    muestra el historial completo.
  - **«Nueva medición»** pone el balance a 10.000 € sin borrar nada.
  - **Si el programa cae:**
    - el stop y un pulso cada 30 s quedan guardados;
    - una posición abierta sigue tras un corte de ≤ 5 min;
    - al apagar el PC se guarda la cuenta y se vacía el grabador;
    - los trades se escriben a disco con fsync.
  - **Reinicio ordenado** (`POST /api/admin/restart` con la cabecera `X-Pavilos: 1`): es la forma de desplegar sin
    perder datos.
