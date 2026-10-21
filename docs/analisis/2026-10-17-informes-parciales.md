# Informes del análisis del 2026-10-17 (4 de 12 áreas)

Flujo de agentes «auditoría del pipeline BTC + estudio de pares, futuros y condicionantes» lanzado el 2026-10-17.
Terminaron 4 áreas; las otras 8 y todas las verificaciones adversariales se cortaron por el límite semanal de uso
(ver `docs/PENDIENTES.md`). **Estos informes NO han pasado la verificación adversarial**: tratarlos como hallazgos
a confirmar. Las sondas y datos que citan estaban en `%TEMP%/pavilos_wf/` (temporal).

---

## Recursos y rendimiento del proceso en vivo (recorder PAVILOS: python -m pavilos PID 17592, supervisor 17536) — CPU/GIL, memoria, latencia, caminos calientes, red, sistema y escalado

### Resumen

AVISO PREVIO: los PID del encargo (17652/17600) no existen. El proceso vivo es 17592 (python -m pavilos, arrancado el 16-10 a las 06:46:43 UTC); su supervisor es 17536 y lo lanzó cmd 17472 (run_recorder.bat). No toqué el proceso: solo lecturas, py-spy pasivo --nonblocking (sin pausas ni señales) y sondas propias.
1) CPU y GIL. El proceso está cerca de llenar un núcleo. Media de vida: 86,6 % de un núcleo (hilo del bucle 59 %, hilo escritor 27 %). En la ventana de 15 min: 60,3 % de media (rango 30-86 %). Según py-spy, el GIL estuvo ocupado el 74,2 % del tiempo (bucle 56,4 %, escritor 17,8 %).
2) Primer consumidor: las venues ccxt entregan el LIBRO ENTERO en cada mensaje. Ese camino se lleva ~40 % del tiempo de reloj: conversión 17,1 %, BookState snapshot 8,5 %, encoder 14,7 %. Coste por mensaje: kucoin 11,0 ms (12.643 niveles), mexc 3,3 ms y gemini 3,0 ms, frente a 9-14 µs por la vía delta. Hay además pérdida de calidad: medido en el mismo minuto, kucoin envía 273 mensajes/s y el lake registra 14,3/s (95 % fusionados). Otras venues: cryptocom 91 %, bitfinex 84 %, gemini 79 %, gate 66 %, bitget 55 %, mexc 44 %.
3) HTX no graba desde las ~14:20 UTC (y falla a ratos antes): 1.140 errores desde el 10-07. Causa: ccxt carga también los derivados en api.hbdm.vn, y el CDN de ese dominio presenta un certificado AUTOFIRMADO (CN=core1.netops.test). Pasa igual con los DNS 8.8.8.8 y 1.1.1.1; no es el antivirus (solo hay Defender). Arreglo validado: cargar solo spot → 2.173 mercados y 177 libros recibidos en 20 s.
4) Cada reconexión ccxt vuelve a cargar todos los mercados dentro del bucle. CPU por recarga: gate 5,5 s, bitget 3,6 s, kucoin 2,5 s, mexc 2,2 s, bitfinex 0,64 s. Los mercados ocupan ~190 MB. Reconexiones desde el 10-07: bitfinex 3.299, htx 1.140.
5) Kraken: el libro de cadenas que mantiene el CRC cuesta 1,11 ms por mensaje (~8,5 % del reloj). Con un índice ordenado baja a 21,6 µs y el CRC sale idéntico (2.000 mensajes).
6) Tick a 5 Hz: recorre 87.128 niveles, y solo el 39,6 % cae dentro de ±300 pb (en coinbase, el 8 %). Cuesta 22-31 ms por tick (~10 % del reloj). Una versión numpy idéntica bit a bit cuesta 11,3 ms. summarize() sobre 21.950 trades cuesta 9,7 ms en cada tick.
7) Escritor: el ciclo real es de 8,55 s (configurado 5 s), con ~2,9 s de codificación por ciclo. Los checkpoints de 60 s son el 54,9 % de los bytes del lake (1,27 de 2,31 GB/día; en coinbase, el 89,8 %).
8) Latencia: en horas tranquilas no hay parones de más de 1 s. Durante esta auditoría hubo 33-97 parones >1 s por hora, con un máximo de 12,2 s. El dashboard dio 48 timeouts en 10 min. Causa externa: commit al 96-97 %, disponible 1,8-7,8 GB, WSL con 32 GB, unos 10 procesos de otros agentes (~20 GB) y Defender usando entre 2,6 y 5,6 núcleos. A las 15:34 UTC todas las venues llevaban 2,5-4,7 s sin actualizar.
9) Red: unos 136 kB/s (~1,1 Mbit/s); kucoin es el 51 %. No es el cuello de botella. Deflate se negocia en los conectores nativos; ccxt no lo pide y esos servidores tampoco lo aceptan. Kraken Futures envía 279 mensajes/s. En Binance USD-M, el aggTrade solo llega por la ruta /market.
10) Reloj: va 247,7 ms por detrás (medido contra 3 servidores NTP). W32Time está en marcha pero sin sincronizar (Local CMOS Clock).
11) Escalado: con estas mejoras (P1) el uso del GIL bajaría del ~74 % al ~16-20 %, y entonces cabrían BTC+ETH+SOL spot más perpetuos en un proceso (~45-50 %). Sin P1, cualquier par nuevo en kucoin, gemini o mexc satura el proceso. Por encima del ~40-50 %, conviene repartir en 2-3 procesos con marcas de arranque por proceso.

### Hallazgos

#### [BLOCKER] R1 — La carga EXTERNA del PC (memoria, Defender, agentes concurrentes) está degradando el recorder en vivo ahora mismo

**Evidencia:** Commit 68,2-68,4 de 70,5-70,7 GB (96-97 %); memoria disponible 6,3-7,8 GB (14:30-14:50 UTC) y 1,79 GB a las 15:34; page-ins del sistema ~6.000/s (syscounters.jsonl); vmmemWSL 30,7-32,6 GB privados; unos 10 procesos Python de otros agentes y proyectos (integrity48.py, disp24.py, fast_series.py, study_leadlag.py, evaluate.py, incremental.py, pytest…) con ~20 GB privados y 5-6 núcleos; MsMpEng al 259 % de un núcleo (15:10) y al 564 % (15:34); working set del recorder recortado de 930 MB a 586 MB con 1.495 MB privados. Huecos del flujo fusionado en el lake (lake_stalls.txt): 0 huecos >1 s por hora en las horas tranquilas, frente a 33-41/h entre 11 y 13 UTC y 97/h a las 14 UTC (33 >2 s, 4 >5 s, máximo 12,22 s, 200 s parado). Dashboard a 2 Hz: 48 timeouts de 5 s en 10 min (4,3 %) y 128 s en parón. Durante los parones la CPU del proceso baja al 28 % frente al 61 % normal: está bloqueado o sin CPU, no calculando. A las 15:34 UTC: lag de 3,1-6,5 s, todas las venues con 2,5-4,7 s sin actualizar y 28 errores de conectores entre 15:20 y 15:34 (kucoin 'Connection timeout', kraken 'keepalive ping timeout', mexc, bitfinex). Histórico de cortes por keepalive: 185 desde el 10-07 (82 el 10-15).

**Impacto:** Huecos y reconexiones en el lake, más coalescencia en ccxt, dashboard inservible. Una parte procede de esta misma auditoría (agentes en paralelo leyendo el lake y consumiendo memoria).

**Recomendación:** INMEDIATO (orquestador/operador): reducir el paralelismo y la memoria de los agentes de análisis, lanzarlos con prioridad BelowNormal/Idle y no recorrer decenas de miles de ficheros del lake en paralelo. Robocopy con /IPG o fuera de horas. PLAN: el supervisor lanza el recorder con ABOVE_NORMAL_PRIORITY_CLASS. Opt-out explícito de EcoQoS para el recorder (SetProcessInformation ProcessPowerThrottling, ControlMask=EXECUTION_SPEED, StateMask=0) porque la CPU es híbrida (6P+4E). Working set mínimo (SetProcessWorkingSetSizeEx) o MEMORY_PRIORITY alta para evitar el recorte. Capar la memoria de WSL en .wslconfig (afecta a Brujita; lo decide el operador). Exclusión de Defender para D:\pavilos_book_data (requiere admin).

**Riesgo de pérdida de datos:** ALTO mientras dure la carga (huecos, reconexiones, estados intermedios perdidos). Las medidas propuestas no tocan datos.

**Esfuerzo:** Bajo (prioridades y límites) a medio (working set, WSL)

#### [BLOCKER] R2 — Las venues ccxt procesan el LIBRO ENTERO en cada mensaje: ~40 % del tiempo y 44-95 % de los mensajes perdidos por coalescencia

**Evidencia:** py-spy en vivo (GIL, 150 s): conversión ccxt_connector.py:70-71 17,1 % del reloj; BookState.apply snapshot (book_state.py:33-34) 8,5 %; encoder _snapshot_levels/_diff (encoder.py:59,73-74) 14,7 %. microbench_out2.txt, coste por mensaje (conversión + apply + encode): kucoin 11,0 ms, mexc 3,3 ms, gemini 3,0 ms, bitget 0,82 ms, htx 0,26 ms; la vía delta cuesta 9-14 µs. El GC es ~38 % del coste de conversión (230 libros kucoin vivos en la cola). A las tasas de la hora 13 esto suma ~35 % de un núcleo; solo kucoin, 15-30 % según la actividad. Coalescencia en el mismo minuto (15:13:30-15:14:30 UTC, mensajes registrados frente a recibidos por la sonda): kucoin 14,3/s frente a 273,3/s (95 %), cryptocom 1,48 frente a 16,0 (91 %), bitfinex 2,9 frente a 18,5 (84 %), gemini 4,7 frente a 23,0 (79 %; incluye trades), gate 9,3 frente a 27,0 (66 %), bitget 4,2 frente a 9,5 (55 %), mexc 5,6 frente a 10,0 (44 %). Mecanismo verificado en ccxt client.py: resolve() solo resuelve si hay un futuro pendiente y receive_loop drena varios mensajes seguidos.

**Impacto:** Es el mayor consumo de CPU y GIL, crea más de 200 MB de libros en la cola del escritor y PIERDE los estados intermedios del libro: OFI, quote flicker y dinámica de colas quedan infra-medidos en 7 de 14 venues. Impide añadir pares o futuros.

**Recomendación:** Emitir DELTAS POR MENSAJE desde ccxt con un hook genérico: subclase o parche de OrderBookSide que anota {precio: tamaño} en storeArray y remove_index/limit; se envuelve handle_order_book del exchange para emitir un BookUpdate delta tras cada mensaje (put_nowait). Si hay reset o el objeto del libro es nuevo, se emite el snapshot completo. Validar contra el libro de ccxt con lake_check/replay. Como alternativa, conectores nativos empezando por kucoin (luego gemini, mexc, cryptocom, bitfinex). Provisional: sustituir el genexpr por tuple(map(itemgetter(0,1), side)), que reduce la conversión un 25-87 %.

**Riesgo de pérdida de datos:** Hoy ya se pierden estados intermedios; el cambio los recupera. Hay riesgo de implementación, por eso hay que validar el libro reconstruido.

**Esfuerzo:** Medio (hook genérico) / alto (conectores nativos)

#### [IMPORTANT] R3 — HTX sin grabar: ccxt carga derivados en api.hbdm.vn y su CDN presenta un certificado autofirmado (no es el antivirus)

**Evidencia:** tls_check.txt: api.hbdm.vn y api.hbdm.com resuelven a 43.152.188.49 (CNAME api.hbdm.vn.eo.dnse3.com) con un certificado autofirmado 'CN=core1.netops.test, O=Widgits Pty Ltd, OU=Packetland'. Ocurre igual con los DNS 8.8.8.8 y 1.1.1.1. api.huobi.pro y api.htx.com verifican bien (Amazon y GoDaddy). El único antivirus es Windows Defender, sin inspección SSL. ccxt htx usa por defecto fetchMarkets.types spot+linear+inverse (htx.py:999-1005). Log: 1.140 errores htx desde el 10-07, reintento cada ~30 s con una traza de ~100 líneas, y 255 avisos 'requires to release'/'Unclosed client session'. Cada intento consume ~1,5 s de CPU (1,48 s medidos). htx estuvo desconectado el 91 % de 14:24-14:29 y el 88,5 % de 14:39-14:49; no hay ficheros htx en la hora 15 (htx_availability.txt). Prueba: con types solo spot carga 2.173 mercados y watch_order_book devuelve 177 libros en 20 s (htx_and_markets_test.txt).

**Impacto:** Venue sin datos; además, ~4-5 % de un núcleo perdido en reintentos, ruido en el log y sesiones sin cerrar.

**Recomendación:** Añadir en venues.py _CCXT_OPTIONS['htx'] = {'options': {'fetchMarkets': {'types': {'spot': True, 'linear': False, 'inverse': False}}}}. Combinarlo con R4 (caché de mercados). Tras el despliegue, comprobar ficheros htx cada hora.

**Riesgo de pérdida de datos:** ALTO hoy (htx no se graba). El arreglo no tiene riesgo.

**Esfuerzo:** Bajo (una línea más prueba)

#### [IMPORTANT] R4 — Cada reconexión ccxt crea un exchange nuevo y recarga TODOS los mercados dentro del bucle

**Evidencia:** ccxt_connector.py:58-60 y 224-234 (_make_exchange en cada reintento). CPU medida de load_markets (proceso aparte, rateLimit de 1 s): gate 5,47 s (6.514 mercados, 61,6 MB), bitget 3,61 s (4.386, 45,9 MB), kucoin 2,52 s (1.670, 24,7 MB), mexc 2,19 s (3.058, 30,8 MB), htx solo spot 1,63 s (2.173, 22,7 MB), bitfinex 0,64 s (288, 2,2 MB). Reconexiones desde el 10-07: bitfinex 3.299, htx 1.140, kucoin 192, mexc 96, cryptocom 78, gemini 30, bitget 27, gate 23. 1.476 excepciones 'Client.receive_loop' de bitfinex (TypeError amount None; KeyError book:tBTCUSD).

**Impacto:** Bloqueos de 0,6-5,5 s del bucle en cada reconexión, que pueden encadenar timeouts de keepalive en otras venues; ~190 MB en mercados; ráfagas de REST.

**Recomendación:** Cargar los mercados una vez por venue y reutilizarlos (ex.set_markets(markets, currencies)) en las reconexiones, o conservar la instancia y reabrir solo el WS. Limitar a spot (fetchMarkets types) en gate, bitget, mexc, kucoin y htx. Para bitfinex, actualizar ccxt o pasar a nativo, porque el error es un bug de ccxt.

**Riesgo de pérdida de datos:** Bajo; reduce huecos.

**Esfuerzo:** Bajo-medio

#### [IMPORTANT] R5 — Kraken: el libro de cadenas para el CRC cuesta 1,1 ms por mensaje (recorte con heapq+Decimal y top-10 con heapq)

**Evidencia:** py-spy: kraken_book.py _bound 6,1 % y checksum 4,9 % de las muestras con GIL (~8,5 % del reloj). Microbench: 1.113 µs por mensaje frente a 21,6 µs con un índice ordenado por bisect, con CRC IDÉNTICO en 2.000 mensajes. Kraken envía 71-94 mensajes/s, es decir ~8 % de un núcleo. Además, el BookState de kraken NO se recorta: 1.105 bids y 1.110 asks (105 y 110 más allá de la profundidad 1000, a -324/+499 pb), así que hay niveles fantasma en los checkpoints, fuera de ±300 pb.

**Impacto:** −8 puntos de CPU; los niveles fantasma son un asunto de calidad para estudios de libro profundo.

**Recomendación:** KrakenRawBook con una lista ordenada de precios float más un dict precio→(precio_str, qty_str): recorte O(1) por nivel sobrante y top-10 por slicing; mantener la verificación CRC en cada mensaje. Opcional: json.loads con parse_float=str en lugar de Decimal. Revisar el recorte del BookState de kraken.

**Riesgo de pérdida de datos:** Ninguno (CRC idéntico).

**Esfuerzo:** Bajo

#### [IMPORTANT] R6 — Escritor: ciclo real de 8,55 s, filas como dicts y checkpoints de 60 s que son el 55 % de los bytes del lake

**Evidencia:** flush_timing_1017_12_13.txt (11.805 ficheros, 843 vaciados): ciclo medio 8,55 s (p90 9,24) frente a 5 s configurados; fase de escritura 0,64 s, con 42-59 ms por fichero incluso de 3,5 KB; codificación ~2,9 s por ciclo. ck_bytes_1017_12_13.txt: los checkpoints son el 54,9 % de los bytes (1,27 de 2,31 GB/día); coinbase 89,8 %, bitstamp 71 %, gemini 69,5 %, binance 59,5 %, kucoin 50,7 %, kraken 21,8 %. Coinbase solo tiene el 8 % de sus 40.884 niveles en ±300 pb (bitstamp 9 %, gemini 7 %). from_pylist del checkpoint de coinbase: 76,6 ms con el GIL tomado, frente a 19,7 ms en columnas (tablas idénticas); el lote kucoin de 8,5 s, 4,0 frente a 1,7 ms. parquet_sink._date_hour hace gmtime y 2 strftime POR FILA.

**Impacto:** El escritor consume 13-27 % de un núcleo y compite por el GIL con el bucle (ráfagas de ~77 ms cada 60 s). Lake ~2× más grande de lo necesario con D: justo de espacio.

**Recomendación:** (a) Construir columnas y usar Table.from_arrays, calculando la partición fecha/hora una vez por actualización. (b) Checkpoints escalonados: banda cercana (p. ej. ±5 %) cada 60 s, libro completo cada 15-30 min, y un checkpoint completo inmediato si dropped>0. Los deltas son completos y el invariante M17 se mantiene desde cualquier checkpoint completo; hay que ajustar el calentamiento del replay y lake_check. (c) Automatizar scripts/compact_lake.py sobre días cerrados: es verificado, pero borra los originales y necesita autorización.

**Riesgo de pérdida de datos:** Ninguno si se respeta el invariante; el replay tendrá que calentar desde el último checkpoint completo.

**Esfuerzo:** Medio

#### [IMPORTANT] R7 — Reloj del PC 247,7 ms por detrás; W32Time en marcha pero sin sincronizar

**Evidencia:** clock_offset.json: SNTP time.windows.com +247,7 ms (retardo 28 ms), time.google.com +247,7, hora.roa.es +247,7; REST binance +268, coinbase +261, okx +257, bybit +256 (±RTT/2). w32tm: Running, inicio Manual, 'Local CMOS Clock', indicador de salto 3, par time.windows.com,0x9 'En espera' (13.743 s restantes), SpecialPollInterval 16.384. El 10-06 eran ~323 ms. Documentación de Microsoft: MaxAllowedPhaseOffset vale 1 s en equipos no unidos a dominio; por debajo corrige gradualmente (slew) y por encima da un SALTO.

**Impacto:** Entre venues no afecta (comparten reloj). Sí sesga −250 ms cualquier cruce con marcas del exchange (trades, futuros, funding, latencias). Si el desfase superase 1 s, la sincronización daría un salto: hacia atrás lo fija el encoder; hacia delante deja un hueco artificial.

**Recomendación:** Con admin y en una ventana planificada: W32Time en Automático con 3 o más servidores sin 'leap smear' ('hora.roa.es,0x8 es.pool.ntp.org,0x8 time.cloudflare.com,0x8') y sondeo de 64-1024 s. Con 248 ms (<1 s) se corrige sin salto. En la grabación futura, guardar además la marca del exchange y un reloj monótono.

**Riesgo de pérdida de datos:** Ninguno, si se evita el salto.

**Esfuerzo:** Bajo

#### [MINOR] R8 — Tick a 5 Hz: build_combined recorre los 87.128 niveles y calcula mid() dos veces

**Evidencia:** py-spy: build_combined 6,8 % más mid/best 3,3 % del reloj. tick_bench_out.txt: Aggregator.snapshot 22,3 ms/tick (31,1 ms en microbench), active() 1,5-2,0 ms; solo el 39,6 % de los niveles está en ±300 pb. La alternativa numpy (bincount en el mismo orden) da un resultado IDÉNTICO bit a bit (mid, 60+60 bins, composiciones) en 11,3 ms (−49 %). La cola de snapshots (engine.py:37) no tiene límite.

**Impacto:** ~10 % de un núcleo para alimentar una estrategia de muros ya refutada y el dashboard (que consulta a 1 Hz).

**Recomendación:** Cachear best bid/ask en BookState; numpy exacto o ventana por índice ordenado para libros profundos (coinbase, bitstamp, gemini); bajar a 1 Hz lo que solo alimenta el dashboard o la estrategia refutada; cola de snapshots de 'último valor'. Si se mantiene la cabalgada en vivo, calcular la presión solo sobre los niveles cercanos.

**Riesgo de pérdida de datos:** Ninguno (no toca la grabación).

**Esfuerzo:** Bajo-medio

#### [MINOR] R9 — Observer: summarize() sobre los 21.950 trades y estado completo a 5 Hz, mientras la web consulta a 1 Hz

**Evidencia:** runtime.py:166-174 (observer en cada snapshot) y trade_log.py:45-61; microbench: summarize 9,7 ms por llamada (hasta 4,9 % de un núcleo; py-spy 1,3-1,8 %, contado dentro de signals/broker); index.html:847-874 consulta cada 1000 ms; server.py:183 endpoint síncrono (threadpool de AnyIO). HTTP p90 0,75-0,97 s y p99 3,3-3,6 s.

**Impacto:** CPU desperdiciada que crece con el número de trades.

**Recomendación:** Resumen incremental (O(1) por trade); construir el estado a ≤1 Hz o bajo demanda; endpoint async con JSON cacheado.

**Riesgo de pérdida de datos:** Ninguno.

**Esfuerzo:** Bajo

#### [MINOR] R10 — Memoria: 1,5 GB privados y sin fuga, pero con mucho margen de recorte

**Evidencia:** Muestreo de 15 min: privada 1.490-1.520 MB (−10,5 MB), working set 914-936 MB (+8,5 MB), handles 332-337 (−3), hilos 28-30 (−2). Picos de vida: 1.580 MB privados y 1.062 MB de working set. Mercados ccxt ~190 MB (medido por exchange). Libros kucoin enteros en la cola del escritor: ~230 por ciclo (estimado ~200 MB). A las 15:34 el working set estaba recortado a 586 MB, con fallos de página al tocarlo. Los 255 'Unclosed client session' se liberan por GC; los handles no crecen.

**Impacto:** Más memoria recortable implica más parones bajo presión (R1).

**Recomendación:** R2 y R4 ahorrarían ~300-400 MB. gc.freeze() tras el arranque y registrar las pausas de GC >50 ms con gc.callbacks.

**Riesgo de pérdida de datos:** Ninguno.

**Esfuerzo:** Bajo

#### [INFO] R11 — Red: ~136 kB/s (~1,1 Mbit/s) para las 14 venues; la compresión WS solo existe en las nativas

**Evidencia:** probe_spot.jsonl (60 s, 15:13-15:17 UTC). Con deflate negociado (ratio cable/carga): binance 0,34, coinbase 0,115 (snapshot inicial de 4,5 MB), okx 0,53, bybit 0,55, bitstamp 0,35. Sin compresión: kraken, gate, mexc (protobuf), cryptocom, bitget, htx (gzip propio), bitfinex, gemini. Kucoin acepta deflate pero el ratio es 1,02. Kucoin: 70 kB/s y 273 mensajes/s (51 % del total). Perpetuos (probe_more.jsonl): binance 9,8 mensajes/s (4,7 kB/s de cable), bybit 10,2 (3,3), okx 9,5 (3,6), bitget 10,2 (8,8), Kraken Futures PF_XBTUSD 279 mensajes/s (36 kB/s, un mensaje por nivel). aggTrade de Binance USD-M: 0 mensajes por /ws/ y 6,7/s por /market/ws/ (documentación de Binance: ruteo por ruta, fallo silencioso).

**Impacto:** El ancho de banda no es el cuello de botella; lo que limita es la CPU por mensaje.

**Recomendación:** Priorizar R2 y R5. Para futuros, usar las rutas /public y /market de fstream y presupuestar Kraken Futures como feed de alta tasa.

**Riesgo de pérdida de datos:** Ninguno.

**Esfuerzo:** n/a

#### [IMPORTANT] R12 — Escalado a N pares × venues + futuros: un proceso no basta sin R2/R5

**Evidencia:** GIL al 74 % hoy. Restando lo que ahorra P1 (R2 −40, R5 −8,3, R8 −5 o −8, R9 −1,3, R6 −0,6) queda un ~16-20 %. E/S, parseo e interno de ccxt cuestan ~9,4 % del reloj para ~520 mensajes/s (~0,18 ms por mensaje, máquina cargada). Mensajes de alts frente a BTC: binance ETH 0,96× y SOL 0,63×; bybit 0,92× y 0,82×; okx 0,87× y 0,63×. Perpetuos de binance, bybit, okx y bitget: ~10 mensajes/s cada uno; Kraken Futures 279/s. replay.py resetea TODOS los libros en cada arranque registrado en _runs.jsonl.

**Impacto:** Sin P1, un segundo símbolo en kucoin, gemini o mexc (+20-35 %) satura el GIL: más coalescencia, parones y cortes de keepalive. Con P1: base ~18 %, más ~7,5 % por ETH spot, ~5,6 % por SOL spot y ~6 % por cada bloque de perpetuos BTC; BTC+ETH+SOL spot más perpetuos quedaría en ~45-50 % de un núcleo.

**Recomendación:** Ejecutar P1 antes de añadir pares. Por encima de ~40-50 %, separar en 2-3 procesos (nativos+perpetuos / ccxt / analítica-dashboard), cada uno con su escritor (los nombres de fichero ya llevan el pid). Las marcas de arranque deben ser POR PROCESO o POR VENUE para que el replay no resetee libros ajenos. A explorar: Python 3.14 free-threaded y winloop.

**Riesgo de pérdida de datos:** n/a (diseño).

**Esfuerzo:** Medio-alto

#### [MINOR] R13 — Configuración del sistema: plan Alto rendimiento correcto; prioridad Normal; sin opt-out de EcoQoS en CPU híbrida; trazas repetidas en el log

**Evidencia:** powercfg: 'Alto rendimiento'. Recorder y supervisor con prioridad NORMAL; ProcessPowerThrottling ControlMask=0 (Windows decide); i5-12600K con 6P+4E y 16 hilos. Defender con tiempo real ACTIVO; las exclusiones no se pueden leer sin admin. logs/pavilos.log: 18,3 MB y 9.813 trazas en 10 días (rotará a 20 MB con 5 copias, ~2 semanas de historia). recorder_supervisor.log: 98 MB.

**Impacto:** Menos protección frente a la carga externa (R1).

**Recomendación:** Ver R1 (prioridad y EcoQoS). Tras R3, limitar las trazas repetidas: una traza por episodio y después un contador.

**Riesgo de pérdida de datos:** Ninguno.

**Esfuerzo:** Bajo

### Medidas

```text
PROCESO. Recorder 17592 ('python -m pavilos'), inicio 16-10 06:46:43 UTC. Supervisor 17536 ('-m scripts.run_recorder'). Lanzador: cmd 17472 (run_recorder.bat). Los PID 17652/17600 del encargo no existen.

MUESTREO 15 min (cada 30 s, 14:22:29-14:37:30 UTC; sampler_analysis.txt)
- CPU (% de un núcleo): media 60,3, mínimo 30,5, máximo 86,1. User 57,3; kernel 3,0.
- CPU por hilo: bucle (tid 17596) 46,7 %; book-recorder (18028) 13,5 %; resto ~0.
- Vida del proceso: 99.237 s de CPU en 114.647 s = 86,6 % de un núcleo (bucle 59,1 %, escritor 27,4 %).
- Memoria: working set 914-936 MB; privada 1.490-1.520 MB; pico de vida 1.062 MB de working set y 1.580 MB de privada. A las 15:34 UTC el working set estaba en 586 MB.
- Hilos 28-30; handles 332-337. Crecimiento en 15 min: privada −10,5 MB, working set +8,5 MB, handles −3. Sin fuga.
- Unas 31 conexiones TCP. Fallos de página del proceso: 82/s de media (máximo 446).
- Los contadores IO por proceso (Win32_Process) no reflejan los bytes de socket en este equipo. Por eso la red se midió con sondas.

PY-SPY PASIVO (--nonblocking, 50 Hz)
Ventana con GIL (14:28:55-14:31:25 UTC): GIL ocupado el 74,2 % del tiempo (5.563 de ~7.500 muestras): bucle 56,4 %, escritor 17,8 %.
Reparto en % del tiempo de reloj:
- conversión ccxt del libro entero: 17,1
- encoder: 14,7
- kraken (parseo y CRC): 8,5
- BookState.apply snapshot: 8,5
- build_combined: 6,8
- mid/best: 3,3
- asyncio: 2,6
- señales, broker y summarize: 2,3
- cola: 1,9
- interno de ccxt: ~4,1
- ssl: 1,4
- parquet_sink: 1,1
- json: 0,6
- websockets: 0,5
Ventana sin GIL (14:31:25-14:33:55 UTC): bucle activo el 70,7 %, escritor el 19,5 %.

ESCRITOR (nombres de fichero del 10-17, 12-13 UTC)
- 11.805 ficheros en 843 vaciados.
- Ciclo: media 8,55 s, p10 7,96, p90 9,24, máximo 11,76.
- Escritura: 0,64 s por vaciado (42-59 ms por fichero).
- Codificación: ~2,9 s por ciclo.

TASAS EN EL LAKE (hora 13 UTC del 10-17): 193,6 actualizaciones/s y 2.536 filas/s.
| venue | actualizaciones/s | niveles en checkpoint |
|---|---|---|
| coinbase | 17,2 | 40.838 |
| binance | 10,0 | 13.548 |
| kucoin | 27,1 | 12.629 |
| bitstamp | 3,0 | 7.178 |
| gemini | 7,1 | 4.565 |
| mexc | 7,1 | 2.958 |
| kraken | 71,1 | 2.240 |
| bitget | 6,0 | 1.000 |
| okx | 7,4 | 800 |
| bybit | 9,0 | 400 |
| htx | 7,0 | 300 |
| gate | 14,6 | 100 |
| cryptocom | 1,7 | 100 |
| bitfinex | 5,3 | 50 |
Niveles dentro de ±300 pb: coinbase 8 %, bitstamp 9 %, gemini 7 %, binance 85 %, kucoin 93 %, mexc 94 %, kraken 66 %.

FRESCURA DEL DASHBOARD
- A 1 Hz durante 5 min (14:24-14:29 UTC):
  - Lag: p50 0,164 s, p90 0,636, p99 2,19, máximo 3,63.
  - HTTP: p50 39 ms, p90 754 ms, p99 3,26 s.
  - 5 timeouts seguidos entre 14:25:08 y 14:25:32.
- A 2 Hz durante 10 min (14:39:30-14:49:30 UTC):
  - Lag: p50 0,216 s, p90 0,851, p99 3,13, máximo 5,90.
  - HTTP: p50 38 ms, p90 972 ms, p99 3,56 s.
  - 48 timeouts (4,3 %) y 128 sondeos en parón global (11,4 %).
  - CPU del proceso: 28 % en parón frente a 61 % normal.
  - Edad de last_update en venues sanas: p50 0,15-0,40 s, p90 0,6-1,4 s, p99 2,2-3,4 s. Kucoin p99 5,6 s; mexc p90 48,8 s; htx caída el 88,5 %.
- 15:34 UTC: lag 3,1-6,5 s y todas las venues con 2,5-4,7 s sin actualizar.

PARONES GLOBALES EN EL LAKE (huecos en el flujo fusionado de todas las venues)
| hora UTC | huecos >0,5 s | >1 s | máximo |
|---|---|---|---|
| 10-16 12h | 27 | 0 | 0,98 s |
| 10-16 16h | 34 | 0 | 0,96 s |
| 10-16 20h | 86 | 0 | 0,93 s |
| 10-17 03h | 7 | 0 | 0,69 s |
| 10-17 06h | 8 | 0 | 0,91 s |
| 10-17 09h | 12 | 0 | 0,80 s |
| 10-17 11h | 346 | 38 | 1,91 s |
| 10-17 12h | 349 | 33 | 2,03 s |
| 10-17 13h | 319 | 41 | 2,15 s |
| 10-17 14h | 363 | 97 | 12,22 s |
A las 14 UTC: 33 huecos >2 s, 4 >5 s, 1 >10 s y 200,6 s parados.
Cortes por keepalive en el log desde el 10-07: 97 (websockets) + 88 (ccxt). Por día: 10-08 31, 10-09 12, 10-10 6, 10-12 2, 10-13 21, 10-14 16, 10-15 82, 10-16 8, 10-17 7 (más los de después de las 15:17 UTC).

SISTEMA
- Hardware: i5-12600K (10 núcleos / 16 hilos, 6P+4E); 48,8 GB de RAM. Plan de energía: Alto rendimiento. Prioridad Normal; EcoQoS no fijado.
- Commit: 96-97 % (68,4 de 70,7 GB); pagefile en C: de 18,4 GB.
- Memoria disponible: 6,3-7,8 GB hasta las 14:50; 1,79 GB a las 15:34.
- Page-ins: ~6.000/s; ráfagas de page-out de hasta 91.000/s. Memory Compression: 4,2 GB. vmmemWSL: 30,7-32,6 GB privados.
- MsMpEng: 259 % de un núcleo (15:10) y 564 % (15:34).
- Disco D: ocupado 19 % de media (máximo 67 %), 14,6 MB/s de lectura ajena. Disco C: hasta 100 %.
- Cola del procesador: 44-76.

RELOJ: desfase +247,7 ms (el PC va por detrás) según time.windows.com, time.google.com y hora.roa.es. REST: binance +268, coinbase +261, okx +257, bybit +256 ms.

HTX: api.hbdm.vn y api.hbdm.com → 43.152.188.49, certificado autofirmado CN=core1.netops.test. Prueba con ccxt: con todos los tipos de mercado falla en 5,8 s (1,48 s de CPU); solo spot carga 2.173 mercados y 177 libros en 20 s.

RED (sondas de 60 s, 15:13-15:17 UTC)
| venue | mensajes/s | carga kB/s | cable kB/s | deflate |
|---|---|---|---|---|
| kraken | 94,1 | 18,8 | 19,2 | no |
| binance | 10,0 | 7,2 | 2,5 | sí |
| coinbase | 17,2 | 89,7 (incluye snapshot de 4,5 MB) | 10,3 | sí |
| okx | 7,9 | 4,1 | 2,2 | sí |
| bybit | 9,2 | 3,3 | 1,8 | sí |
| bitstamp | 3,0 | 1,5 | 0,5 | sí |
| gate | 27,0 | 5,0 | 5,1 | no |
| mexc | 10,0 | 8,4 | 8,5 | no |
| cryptocom | 16,0 | 6,0 | 6,0 | no |
| bitget | 9,5 | 3,8 | 3,8 | no |
| kucoin | 273,3 | 69,0 | 70,1 | aceptado sin efecto |
| htx | 7,9 | 1,3 | 1,3 | no (gzip propio) |
| bitfinex | 18,5 | 0,5 | 0,5 | no |
| gemini | 23,0 | 4,3 | 4,4 | no |
Total: ~136 kB/s de cable (≈11,8 GB/día).
- Perpetuos: binance 9,8 mensajes/s (13,4 kB/s de carga), bybit 10,2 (8,6), okx 9,5 (8,5), bitget 10,2 (8,7), Kraken Futures 278,8 (35,5).
- Trades: binance spot 11,8/s; binance perp aggTrade 6,7/s (solo por /market); bybit 1,0; okx 0,8; coinbase 3,3.
- Alts (mensajes/s): binance ETH 9,6 y SOL 6,3; bybit 8,5 y 7,6; okx 6,9 y 5,0.

COALESCENCIA (mismo minuto, registrados frente a recibidos por segundo)
| venue | registrados | recibidos |
|---|---|---|
| kucoin | 14,3 | 273,3 |
| cryptocom | 1,48 | 16,0 |
| bitfinex | 2,9 | 18,5 |
| gemini | 4,7 | 23,0 |
| gate | 9,3 | 27,0 |
| bitget | 4,2 | 9,5 |
| mexc | 5,6 | 10,0 |
| kraken (nativo) | 78,9 | 94,1 |

MICROBANCOS (libros reales; la máquina estaba cargada, así que los valores absolutos pueden estar inflados ~1,5-2×; las proporciones son robustas)
Por mensaje ccxt (conversión + apply + encode) frente a la vía delta:
| venue | actual | delta |
|---|---|---|
| kucoin | 11,0 ms | 8,6 µs |
| mexc | 3,3 ms | 12 µs |
| gemini | 3,0 ms | 11 µs |
| bitget | 0,82 ms | 14 µs |
| htx | 0,26 ms | 10 µs |
| gate | 88 µs | 13 µs |
| cryptocom | 109 µs | 12 µs |
| bitfinex | 46 µs | 9 µs |
- GC: ~38 % del coste de conversión.
- Kraken: 1.113 µs frente a 21,6 µs por mensaje.
- Tick: 22,3-31,1 ms; numpy exacto 11,3 ms.
- Escritor: lote kucoin 4,0 → 1,7 ms; checkpoint coinbase 76,6 → 19,7 ms; write_table 3,7 y 41,9 ms.
- summarize: 9,7 ms.
- Coste de load_markets (CPU): gate 5,47 s, bitget 3,61, kucoin 2,52, mexc 2,19, htx spot 1,63, bitfinex 0,64.

REPLAY OFFLINE (60 s del flujo real, 13:30 UTC): 46,5 % de un núcleo para el código propio de PAVILOS (sin red, parseo ni interno de ccxt). Reparto, en tiempo de reloj inflado ×1,77: tick 29,5 %, trading 14,1 %, encoder ccxt 13,5 %, conversión ccxt 12,1 %, apply ccxt 8,7 %, escritor 2,9 %. GC: 3.650 colecciones y 2,5 s.

CHECKPOINTS: 54,9 % de los bytes del lake (10-17, 12-13 UTC: 2,31 GB/día en total).

TRANSPARENCIA
- py-spy fue solo lectura de memoria, sin pausas.
- Las sondas WS abrieron una segunda conexión por exchange entre las 15:13 y las 15:17 UTC. El recorder registró errores de kucoin, mexc, kraken y bitfinex entre las 15:17 y las 15:34, en el pico de carga externa; la causalidad no está aclarada.
- No escribí nada en el repo ni en el lake; lo comprobé con find -newermt.
```

### Preguntas abiertas

- Los PID del encargo (17652/17600) no existen; el recorder vivo es 17592 y su supervisor 17536 (lanzados por cmd 17472, run_recorder.bat, el 16-10 a las 08:46 hora local). ¿Se esperaba otra instancia?
- ¿Autoriza el operador limitar YA el paralelismo, la memoria y la prioridad de los agentes de análisis concurrentes? Hay degradación en vivo medida: parones de hasta 12 s, 48 timeouts del dashboard en 10 min y 28 errores de conectores entre 15:20 y 15:34 UTC.
- ¿Se capa la memoria de WSL (32,6 GB privados)? Afecta a Brujita; es decisión del operador.
- Exclusión de Defender para D:\pavilos_book_data y reconfiguración de W32Time: ambas requieren admin y una ventana planificada.
- ¿Debe seguir corriendo dentro del grabador, a 5 Hz, la estrategia de muros ya refutada (detector + broker, 21.950 trades), o se separa del recorder o se baja a 1 Hz?
- Checkpoints escalonados (banda cercana cada 60 s y libro completo cada 15-30 min) y compactación automática de días cerrados: cambian la operativa del lake y la compactación borra los originales tras verificarlos. Requieren autorización expresa por la regla de retención eterna.
- Para las venues ccxt, ¿se quiere el 100 % de los mensajes del exchange (hook por mensaje o conectores nativos) o basta el estado fusionado actual? Hoy kucoin pierde el 95 % de los estados intermedios.
- Los niveles fantasma de kraken (105 bids y 110 asks fuera de la profundidad 1000 en el BookState y en los checkpoints): confirmar la semántica de truncado de Kraken v2. Probablemente lo cubre el agente de datos.
- El flujo l2 de gemini incluye trades: ¿se quieren grabar los trades en el lake?
- Las medidas absolutas de los microbancos se tomaron con la máquina muy cargada. Conviene repetir el perfil py-spy y el muestreo en una franja tranquila, sin agentes, para fijar la línea base.

---

## Recursos y arquitectura para ampliar PAVILOS: proyección S1–S4, disco y lake (caliente/frío, M17b, compactación, migración) y arquitectura de procesos

### Resumen

Todo es medido en vivo y en solo lectura (17-10, sábado; laborable = log y lake del 10-16). Hay dos bloqueantes antes de añadir nada.
(1) Disco D:: quedan 145,6 GiB libres y el suelo está en 100. Ha bajado de 241,7 a 145,6 GiB en 10 días (media 9,7 GiB/día, con un salto de 57 GiB en un día). La pausa llegaría en unos 5 días, o en menos de 1 si se repite el salto.
(2) El proceso único ya no aguanta, ni siquiera con S0. Comparé el grabador con sondas ligeras en la misma ventana y las mismas updates nativas:
- Paradas: en vivo hay huecos simultáneos en los venues de hasta 7,6 s; con sondas, 0 por encima de 0,5 s. El 10-16 a las 14 h UTC hubo 558 paradas de ≥0,5 s (el 23 % de la hora).
- Venues ccxt: graban solo entre el 0,3 % y el 42 % de las updates que recibe un proceso dedicado (kucoin 4 % → 0,3 %; gemini 14 % → 2 %).
- Panel: /api/health no respondió en más de 15–20 s.
- Causas: un solo event loop + ccxt + build_combined, en un equipo con la CPU al 83 % de media (Defender (MsMpEng) ≈4 núcleos) y la grabación a prioridad NORMAL.
- Prueba de fuerza: 3 pares en un proceso → retraso del loop p50 4,7 s, p99 18,7 s, y 19 de 42 streams sin datos.
Además, htx no tiene datos desde las 14:21Z por un fallo de SSL en api.hbdm.vn.
Volumen medido hoy (S0): 2,95 GB/día de media y 147.752 ficheros/día. Proyección en laborable (crudo → compactado F):
- S0: 3,10 → 1,77 GB/día
- S1 (+4 perpetuos BTC): 4,71 → 2,69
- S2 (4 pares × 14): 9,64 → 5,50
- S3 (+perpetuos + trades): 16,1 → 8,4
- S4 (8 pares): 28,7 → 14,7 GB/día, es decir 10,5 → 5,4 TB/año
Ficheros crudos: de 148.000/día a 3.000.000/día en S4 (pasa de 336 a 6.912 al compactar).
CPU en sábado, en % de un núcleo (conectores de hoy / nativos): S0 58/21, S1 65/29, S2 191/70, S3 233/112, S4 464/203. En laborable, ×1,7–2,5.
RAM: hoy 1,5 GB de privados; unos 4 GB en S2 y unos 6 GB en S4. Ancho de banda (laborable): 1,4 Mbit/s en S0, 12 en S4 y picos de unos 36; no limita.
Compactación (hora real verificada sin pérdidas): compact_lake actual deja el 61 % del crudo; la variante zstd 9 + diccionario, el 57,1 %; el pie Parquet de 2.090 B/fichero es el 10,5 % del lake.
Recomendado, en este orden:
1. Liberar D: moviendo los 291 GB pre-M17 a L: (copia verificada; el borrado de D: lo autoriza el operador). Subir la prioridad y excluir el lake de Defender.
2. M17b: raíz caliente + raíz fría (L:) y compactación horaria automática.
3. Multiproceso por familia de venue, con marcas de sesión por proceso y layout por stream.
4. S1, y después «S3-lite»: ETH/SOL en 6 venues núcleo + perpetuos + trades, unos 10 GB/día crudo.
S4 solo con disco nuevo (≥16 TB + segunda copia) y si los resultados lo justifican.

### Hallazgos

#### [BLOCKER] RA-01 — D: entrará en pausa en días: 45,6 GiB sobre el suelo y consumo a ráfagas de otros proyectos

**Evidencia:** Volumen D: (Samsung 980 PRO NVMe, 931 GB): 145,6-145,8 GiB libres (disk_free_log.txt, 17:17-18:03). Suelo book_min_free_gb=100 (src/pavilos/core/runtime.py:100). Trayectoria en logs/pavilos.log (lineas 'retention:'): 241,7 (10-07 19:37) -> 234,8 (10-09) -> 219,0 (10-14) -> 209,5 (10-15 08:32) -> 152,1 (10-16 08:46); hoy 145,6. Media 9,7 GiB/dia en 9,9 dias (PAVILOS escribe ~2,75 GiB/dia), con un salto de 57,4 GiB en 24 h (10-15->10-16). El lake ocupa 319,2 GB (323,0 en disco), de ellos 291,2 GB pre-M17 (kucoin 229,4; gemini 38,9; mexc 12,3) en 424.543 ficheros.

**Impacto:** Con la media actual la grabacion se pausa en ~4,7 dias; con otro salto como el del 15-16, en <1 dia. Durante la pausa no se graba nada (perdida de datos futuros, contra la retencion eterna). Impide cualquier ampliacion en D:.

**Recomendación:** Inmediato y sin codigo: copiar las particiones pre-M17 (date<=2026-10-07) de D:\pavilos_book_data a L:\pavilos_lake_cold con robocopy /E /COPY:DAT /DCOPY:DAT (sin /MOVE). Verificar recuento y tamanos por particion y comprobar que todos los ficheros son legibles (lake_files.check_files). Con autorizacion del operador, eliminar el origen y dejar una junction (mklink /J) por particion, o apuntar los lectores a L:. Asi D: gana ~291 GB y los lectores siguen funcionando porque las junctions son transparentes para os.listdir y DuckDB. Despues: M17b (RA-04) y raiz caliente fuera de D:.

**Riesgo de pérdida de datos:** Ninguno si se borra solo tras verificar y con autorizacion; la copia en G: del backup del 17-10 da una segunda red. El riesgo real es NO actuar: pausa de la grabacion.

**Esfuerzo:** 2-4 h del operador + copia de 291 GB / 424k ficheros a HDD (~2-4 h)

#### [BLOCKER] RA-02 — El proceso unico ya degrada los datos de S0: paradas del event loop de 0,5-8 s en todos los venues y ccxt coalesce el 58-99,7 % de las updates

**Evidencia:** A/B en la MISMA ventana, el grabador en vivo (PID 17592) frente a sondas ligeras (un proceso por venue, mismo codigo de conectores/encoder):
(a) ts de recepcion BTC (ab_stalls.py, 4,5 min):
- binance: vivo p99 1.460 ms, max 7.589 ms, 123 huecos >0,5 s y 58 >1 s; sonda p99 133 ms, max 442 ms, 0 y 0.
- coinbase: vivo 1.166 / 8.659 ms (130 y 56); sonda 338 / 613 ms (4 y 0).
- okx: 1.976 / 7.607 frente a 496 / 707. bybit: 1.529 / 7.612 frente a 300 / 605. kraken: 334 / 4.945 frente a 219 / 641.
- Mismo numero de ts distintos (2.703 frente a 2.696): no se pierden mensajes nativos, pero su hora se retrasa y se amontona.
(b) Horas sin sondas (live_stalls.py), huecos simultaneos en >=4 de 5 venues:
- 10-16 14h UTC: 558 de >=0,5 s (827 s, 23 % de la hora); 149 de >=1 s; max 2,4 s.
- 10-17 13h UTC: 493 (988 s, 27 %); 293 de >=1 s; 9 de >=2 s.
(c) Updates de venues ccxt, vivo/sonda (ab_updates.py):
- lote A: kucoin 1.221/27.190 (4 %).
- lote B: gate 12 %, mexc 22 %, cryptocom 42 %, bitget 16 %, bitfinex 28 %, gemini 14 %.
- ventana limpia 17:5x: kucoin 24/8.771 (0,3 %), gemini 2 %, gate 4 %, mexc 9 %.
- Causa: ccxt solo devuelve el ultimo libro cuando el consumidor va tarde.
(d) A las 17:50-18:00 local, /api/health (mismo loop) tardo 7,9 s, >15 s (x2) y >20 s (x2); la CPU del proceso estaba en 10-21 % (no es CPU propia); received/min bajo a 2.548-4.240 (habitual 10-14k).
(e) Host: CPU del sistema media 83 % (min 38, max 100) durante 1,58 h; MsMpEng.exe (Defender) 25,4 % de 16 logicos (~4 nucleos) en 60 s; vmmemWSL 9 %; el grabador va a prioridad NORMAL.

**Impacto:** La hora de recepcion de TODOS los venues lleva errores de 0,5-8 s en rafagas (un 14-27 % del tiempo con >=0,5 s), lo que invalida analisis de adelanto/retardo por debajo de unos segundos (perpetuos frente a contado). Los 8 venues ccxt guardan una resolucion temporal muy inferior a la publicada. El panel queda sin respuesta. Las sondas de este analisis anadieron carga entre 14:39 y 15:20Z y pudieron agravarlo, pero las horas previas (10-16 14h, 10-17 13h) ya lo muestran.

**Recomendación:** Inmediato, y lo decide el operador: subir el grabador a prioridad ABOVE_NORMAL (o HIGH) y anadir una exclusion de Defender para las raices del lake (Add-MpPreference -ExclusionPath). Corto plazo: anadir una metrica de retraso del loop (sleep 50 ms; p50/p99/max por minuto en el log y en el panel) y un vigilante que reinicie el proceso si su latido se queda viejo. Estructural: grabadores multiproceso (RA-06) y conectores nativos para los ccxt (RA-07); los grabadores sin build_combined ni panel.

**Riesgo de pérdida de datos:** Ya hay perdida de resolucion (ccxt) y de precision temporal en los datos grabados. Las medidas propuestas no tocan datos existentes.

**Esfuerzo:** Prioridad + exclusion: 15 min. Metrica de lag + vigilante: 1 dia. Multiproceso: ver RA-06.

#### [BLOCKER] RA-03 — Meter mas pares en el proceso actual colapsa: prueba de saturacion con BTC+ETH+SOL

**Evidencia:** single_proc_test.py: un solo loop, un solo escritor BookRecorder real (vaciado 5 s), Engine por par con snapshot a 5 Hz y checkpoint cada 60 s; host al 83 % de CPU.
- Solo BTC (replica de S0): CPU 44 % (loop 36,5 %, escritor 7,6 %). Retraso del loop p50 30 ms, p90 282, p99 1.150, max 2.267 ms; 14 paradas de mas de 1 s en 173 s. Ciclo del escritor 6,96 s.
- BTC+ETH+SOL: solo 32 muestras de lag en 180 s (p50 4,7 s, p99 18,7 s). 19 de 42 streams sin datos al final (todos los ccxt: RequestTimeout en load_markets y timeouts de handshake WS). Ciclo del escritor de hasta 42,8 s.
- Primera ejecucion de 3 pares: CPU 65 %, p99 4,8 s, ciclo del escritor de hasta 52 s.
- En vivo, el escritor tarda 8,17 s de mediana por ciclo (5 s de ventana + ~3,2 s de vaciado), es decir, un 39 % del tiempo ocupado. El vaciado crece con el volumen, asi que se satura hacia 2,5x el volumen actual.

**Impacto:** S2-S4 no son viables ampliando el proceso actual; el fallo es en cascada: paradas, timeouts, reconexiones, mas paradas. Los venues ccxt ni llegan a arrancar.

**Recomendación:** No anadir pares ni futuros al proceso actual. Requisito previo de cualquier escenario mayor que S1: grabadores separados por familia de venue (RA-06). S1 (4 perpetuos BTC, unos 7,6 % de CPU el sabado) puede ir en un proceso nuevo e independiente aunque RA-06 no este terminado.

**Riesgo de pérdida de datos:** Alto si se amplia en el mismo proceso (streams sin datos, ts erroneos); nulo si se respeta el requisito.

**Esfuerzo:** Ninguno (es una restriccion de diseno).

#### [IMPORTANT] RA-04 — M17b: lake caliente/frio, compactacion horaria automatica y conversion del lake pre-M17

**Evidencia:** Hora 14 UTC del 10-16, 14 venues: 6.145 ficheros, 155,4 MB.
- Pies Parquet: 9,1 MB (5,8 %); el sobrecoste fijo por fichero es de 2.090 B (10,5 % del lake: 148k ficheros/dia x 2.090 B = 0,31 GB/dia), mas 0,3 GB/dia de holgura de cluster de 4 KiB.
- A, DuckDB COPY zstd (= compact_lake actual): 94,8 MB (61,0 %).
- B, pyarrow zstd 1: 60,8 %.
- F, zstd 9 + diccionario + BYTE_STREAM_SPLIT de respaldo: 88,8 MB (57,1 %), 13,6 s de CPU de escritura por hora de S0.
- min(A,F) por venue: 55,8 %. Las variantes BYTE_STREAM_SPLIT sin diccionario son PEORES (96-104 %).
- Todas verificadas por multiconjunto con 0 diferencias.
- Hoy la compactacion es manual (scripts/compact_lake.py), se hace in situ y su verificacion EXCEPT ALL es cara: unos 20-30 s por venue y hora con 4 hilos.
- Pre-M17: 291,2 GB, 79 % kucoin; con el encoder M17, kucoin pasa de ~19 a ~0,28 GB/dia.

**Impacto:** Con la variante F, el lake de S0 baja de 3,10 a 1,77 GB/dia (de 1,13 a 0,65 TB/ano) y los ficheros frios, de 147.512 a 336 al dia. Copias incrementales triviales y menos trabajo de Defender y NTFS. Convertir el lake pre-M17 libera unos 250-270 GB.

**Recomendación:** M17b en dos piezas.
(1) RuntimeConfig con raiz CALIENTE (en un SSD, con su propio suelo) y raiz FRIA (L:, HDD de 714 GB libres). Lectores multirraiz (hour_dirs sobre varias raices; si existe el compactado frio de una hora, gana el frio).
(2) Compactador horario como proceso aparte de baja prioridad (Programador de tareas, HH:20; horas cerradas hace 15-60 min):
- un fichero por stream y hora en la raiz fria con la variante F, o la menor de A y F por venue;
- verificacion O(n): recuento + suma de hash() por columna + min/max ts, en vez de EXCEPT ALL;
- escritura .tmp + rename + fsync, y despues borrado de los crudos calientes (reglas actuales de promover y luego borrar, y de ficheros tardios).
Despues, convertir el lake pre-M17 a diferencias en frio, como dice la spec M17 (seccion 5).

**Riesgo de pérdida de datos:** Bajo con promover-y-luego-borrar y verificacion previa (la logica de compaction.py ya cubre los cortes a mitad). La conversion pre-M17 cambia la codificacion pero no la informacion; conservar el original en frio hasta verificar con check_lake.

**Esfuerzo:** 3-5 dias de desarrollo + pruebas (multirraiz + compactador). Conversion pre-M17: 2-3 dias + horas de CPU.

#### [IMPORTANT] RA-05 — Donde debe vivir el lake: ni D: ni C: son estables; caliente en SSD con suelo propio y frio en L:

**Evidencia:** Volumenes:
- C: 990 PRO NVMe (sistema), 486,7-530,3 GiB. Perdio 43,6 GiB en ~50 min durante la sesion por otros procesos y luego recupero parte.
- D: 980 PRO NVMe compartido, 145,6 GiB.
- E: 870 QVO (QLC SATA, usado por otros), 837,2.
- F: 870 EVO SATA, 269,7.
- H: 990 PRO NVMe 'DB2', 189,5 (uso desconocido).
- K: WD Red 4 TB, 77,6.
- L: WD Red 4 TB, 714,5.
- G:/I:/J: USB (ASMedia).
Dias de L: (767 GB) con datos compactados: S0 433, S1 285, S2 139, S3 92, S4 52. Libre total que no es USB: unos 2,75 TB, compartidos con otros proyectos.

**Impacto:** Un disco compartido con otros proyectos que consumen a rafagas pone en riesgo la grabacion continua. La retencion eterna a partir de S2 necesita hardware nuevo en meses.

**Recomendación:** Corto plazo: caliente en D: tras liberar los 291 GB (RA-01) o en F:, con suelo propio; frio en L:.
Medio plazo: caliente en un volumen dedicado a PAVILOS, por ejemplo un VHDX de tamano fijo de 200-300 GB en F: montado al arrancar, que reserva el espacio frente a otros procesos.
Para S2 o mas: un HDD de 16-20 TB para frio + un segundo disco para copia; las USB solo como copia fuera de linea, nunca para grabar.
No usar C: salvo temporalmente y con un suelo estricto.

**Riesgo de pérdida de datos:** Ninguno si la migracion sigue RA-06 y la seccion de migracion: el cambio de raiz se aplica en el siguiente reinicio, que ya es casi diario, sin hueco adicional.

**Esfuerzo:** Configuracion: 1 h. VHDX dedicado: 2 h (necesita administrador). Compra de disco: decision del operador.

#### [IMPORTANT] RA-06 — Arquitectura multiproceso: grabadores por familia de venue, marcas de sesion por proceso y layout por stream

**Evidencia:** Hoy hay 1 proceso: 14 conectores + Aggregator con build_combined a 5 Hz + detector/senal/broker + uvicorn en un solo loop, y 1 hilo escritor que compite por el GIL (runtime.py:229-253, recorder.py). Medido: loop 26,5-57,6 % y escritor 5,4-26,5 %.
- _runs.jsonl es unico y el replay resetea TODOS los libros en cada arranque (replay.py:112-120; runs.py).
- Las filas no llevan simbolo ni tipo de mercado (parquet_sink.py:20-24); seq_no es por venue y proceso.
- Lo bueno: los nombres de fichero ya son unicos entre procesos (pid-time_ns).
- Sondas por venue (3 streams por proceso): CPU 1,4-7,6 % nativos y perpetuos; kraken 20,2 % (Decimal + CRC); ccxt 2,4-13,8 %; kucoin 59,7 %. RSS: 112-186 MB nativos, 197-397 MB ccxt.
- En las sondas, el ciclo del escritor es de 8,23 s con 8,2 s de ventana: vaciado de unos 0,03 s.

**Impacto:** Sin esto no hay S2-S4. Con esto, un fallo de ccxt (htx, bitfinex) solo afecta a su proceso, las horas de recepcion no se contaminan entre venues y cada proceso se mantiene por debajo del 50 % de un nucleo.

**Recomendación:** (1) Procesos 'pavilos-rec-<grupo>' solo de grabacion (sin build_combined ni panel), cada uno con sus conectores, un BookState por stream para los checkpoints, un escritor y su suelo de disco. Grupos para S3:
- binance (contado + USDs-M);
- coinbase;
- kraken-contado y kraken-futures (hasta optimizar el CRC);
- okx (contado + swap);
- bybit (contado + linear);
- bitstamp;
- kucoin;
- ccxt-A (gate, mexc, cryptocom) y ccxt-B (bitget, htx, bitfinex, gemini).
En total, unos 9-11 procesos.
(2) 'pavilos-app' (paper trading + panel) aparte: a corto plazo con conexiones propias y sin grabar; mas adelante por IPC.
(3) Layout para streams nuevos: <raiz>/book/venue=X/market=spot|perp/symbol=Y/date=/HH/; tablas 'trade' (ts, ts_exch, trade_id, precio, tamano, lado) y 'deriv' (mark, index, funding, OI, liquidaciones). Las particiones BTC actuales se quedan como estan.
(4) Marcas por proceso: _runs/<proc>.jsonl con {start_ts, pid, proc, streams}; el replay resetea solo esos streams. seq_no por stream y ejecucion.
(5) Supervisor generalizado (scripts/run_recorder.py) con N hijos, reinicio y backoff por hijo, logs por hijo, latido _status/<proc>.json y vigilante de proceso colgado.
(6) check_lake por stream cada hora sobre la ultima hora cerrada, con resultado visible en el panel.
(7) Conservar el codigo M17 del encoder y los checkpoints: ya funcionan por stream.

**Riesgo de pérdida de datos:** Bajo: el cambio se despliega en un reinicio. Las marcas por proceso evitan que el replay mezcle libros entre procesos; mientras tanto, el replay actual debe resetear por stream.

**Esfuerzo:** 1,5-2 semanas: procesos + supervisor + layout + runs por proceso + replay por stream + pruebas.

#### [IMPORTANT] RA-07 — ccxt es el sumidero de CPU y RAM, y la causa del apagon de htx: conectores nativos y mercados solo de contado

**Evidencia:** Sonda, % de nucleo por stream (sabado):
- kucoin: BTC 23,3, ETH 4,4, SOL 34,1 (95-263 updates/s, con el libro entero convertido en cada mensaje: 63-66k niveles/s).
- gemini 2,3-6,8; gate 1,1-4,0; frente a nativos de 0,5-1,8 (binance, coinbase, okx, bybit, bitstamp).
- kraken nativo: 2,6-10,7, por el parse_float=Decimal y el CRC de cada mensaje (kraken_connector.py:118; kraken_book.py).
Procesos ccxt de 3 instancias: 197-397 MB. El proceso vivo tiene 1,47-1,51 GB privados.
Los libros ccxt crecen con el tiempo de proceso (filas por libro completo en vivo frente a la sonda recien conectada): kucoin 12.804 (max 23.879) frente a 639; mexc 4.141 frente a 278. Lo mismo pasa en binance nativo: 21.820 (max 32.272) frente a 10.085.
htx: sin datos desde 14:21:12Z (dashboard last_update_ts 1792246872), 1.138 errores en el log, un intento cada ~40 s. Las opciones de ccxt htx fetchMarkets.types incluyen linear/inverse, que llaman a api.hbdm.vn y fallan por SSL autofirmado (ccxt/async_support/htx.py:999-1005). En la sonda, htx fallo 12 de 12 intentos.
Cada reconexion crea una instancia ccxt nueva, con load_markets y sesion aiohttp nueva (ccxt_connector.py:55-83). En la sonda, el primer dato tardo 14-131 s.

**Impacto:** Con los conectores de hoy, S2 cuesta 191 % de nucleo en sabado (unos 3,3-4,8 nucleos en laborable). Con conectores nativos para los 8 venues ccxt y el CRC de kraken optimizado, unos 70 % (1,2-1,7 en laborable). El RAM baja unos 100 MB por venue. Y htx deja de perder horas de datos.

**Recomendación:** (1) Ya, solo configuracion: htx con options fetchMarkets.types {spot: True, linear: False, inverse: False}, y el mismo criterio de solo contado en el resto de ccxt.
(2) Reutilizar la instancia ccxt y sus mercados entre reconexiones, con una sola instancia por venue para todos los pares.
(3) Conectores nativos de deltas, por orden de coste: kucoin, gemini, gate, mexc, bitget, htx, bitfinex y cryptocom.
(4) Kraken: parse_float=str, CRC sobre los textos y conversion a float sin Decimal.
(5) Opcional: orjson para el JSON.

**Riesgo de pérdida de datos:** Ninguno; los nativos recuperan la resolucion que hoy se coalesce.

**Esfuerzo:** Configuracion de htx: 30 min. Nativos: 1-2 dias por venue. CRC de kraken: 0,5 dias.

#### [IMPORTANT] RA-08 — La cantidad de ficheros explota con los pares: hace falta multiplexar o compactar

**Evidencia:** Hoy salen 10.600 ficheros por stream y dia (vaciado cada 8,17 s; inventario de 10-08 a 10-16: 142.811-155.947 al dia). Proyeccion en crudo con el diseno actual: S1 189.659, S2 590.049, S3 1.517.268, S4 3.034.537 ficheros/dia. El pie de 2.090 B/fichero hace que un stream de trades en ficheros de 8 s gaste 22 MB/dia solo en pies, frente a unos 0,01-0,1 GB/dia de datos reales (7-15 B/trade compactado). Defender esta consumiendo unos 4 nucleos; probablemente por el escaneo de ficheros nuevos, aunque no lo he verificado.

**Impacto:** NTFS (MFT), las copias robocopy, DuckDB (abrir y leer miles de pies), Defender y las particiones horarias se degradan; en S3/S4, el 10-30 % de los bytes crudos serian pies.

**Recomendación:** (1) Un fichero por proceso y vaciado con columna de stream, en lugar de uno por stream (unos 10 procesos: 105k ficheros/dia sea cual sea el numero de streams).
(2) Vaciado de 30-60 s para trades y deriv (la perdida maxima ante un corte es esa ventana).
(3) Compactacion horaria a un fichero por stream y hora en frio (RA-04).
(4) A estudiar en M18: registro caliente en Arrow IPC por proceso y hora (anadir sin crear ficheros) + compactacion a Parquet.

**Riesgo de pérdida de datos:** Un vaciado mas largo amplia la ventana que se pierde si el proceso muere: hoy 8 s, con 30 s pasaria a 30 s. Conviene compensarlo con vaciado al apagar (RA-11).

**Esfuerzo:** Multiplexado por proceso: 2-3 dias, dentro de RA-06.

#### [IMPORTANT] RA-09 — La retencion eterna supera la capacidad libre desde S2: TB/ano por escenario

**Evidencia:** Anual (crudo -> compactado F):
- S0: 1,13 -> 0,65 TB/ano
- S1: 1,72 -> 0,98
- S2: 3,52 -> 2,01
- S3: 5,88 -> 3,06
- S4: 10,47 -> 5,37
Libre fijo que no es USB: C 487-530, D 146, E 837, F 270, H 189, K 78, L 714 GiB, todo compartido. Los checkpoints de 60 s son ~50 % del lake: en vivo, coinbase 79 % de sus bytes, con 40.557 filas cada 60 s.

**Impacto:** Sin disco nuevo, S2 llena L: en unos 139 dias y S4 en unos 52. Una sola copia en HDD no es retencion eterna.

**Recomendación:** Comprar para PAVILOS un HDD de 16-20 TB para frio y otro, o un NAS, para la segunda copia antes de S2. Decision del operador sobre una opcion sin perdida de informacion: en frio, conservar 1 checkpoint cada 10 min y quitar los intermedios que check_lake verifique como identicos al libro reconstruido. Ahorraria cerca del 45 % en S0 y la mayor parte de los bytes de coinbase y binance.

**Riesgo de pérdida de datos:** Reducir los checkpoints quita redundancia verificable, no informacion, pero reduce la capacidad de curar descartes (hoy 0). Debe ser opcional y estar documentado.

**Esfuerzo:** Compra + instalacion: 1 dia. Reduccion de checkpoints: 2 dias.

#### [IMPORTANT] RA-10 — Futuros Binance: los streams /market (aggTrade, markPrice, forceOrder) no llegan por la URL antigua

**Evidencia:** Sonda de cable con wss://fstream.binance.com/stream?streams=... : depth@100ms llega (BTC 506 niveles/s), pero aggTrade, markPrice@1s y forceOrder dan 0 mensajes en 5 min. La doc oficial divide en /public (depth, bookTicker) y /market (aggTrade, markPrice, forceOrder): https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Important-WebSocket-Change-Notice ('Channels under /market and /private will stop pushing data' en las URL antiguas). ccxt 4.5.56 ya enruta a /public y /market (ccxt/pro/binance.py:234-257).

**Impacto:** Un conector nativo de futuros que use la URL antigua graba libros pero se queda sin trades, funding/mark ni liquidaciones, sin dar ningun error.

**Recomendación:** Para S1/S3: una conexion en wss://fstream.binance.com/public para depth y otra en /market para aggTrade, markPrice y forceOrder. Semilla REST con fapi/v1/depth limit=1000 (peso 20) y regla de continuidad pu == u previo (implementada y probada en pipe_probe.py: 0 resincronizaciones en 5 min).

**Riesgo de pérdida de datos:** Ninguno si se hace bien; si no, se pierden los trades y deriv de Binance sin aviso.

**Esfuerzo:** Incluido en el conector de futuros: 1-2 dias por venue.

#### [MINOR] RA-11 — Calidad temporal: reinicios casi diarios, sin vaciado al apagar, reloj sin NTP y sin ts del exchange

**Evidencia:** recorder_supervisor.log: salidas con codigo 1073807364 (0x40010004, el proceso muere al apagar Windows) el 10-14 03:30, 10-15 08:30 y 10-16 08:45. Huecos de 77-267 s mas el lote no vaciado (hasta 8 s). _gaps.json esta sin actualizar desde el 10-07. Reloj del PC unos 323 ms por detras de binance, con el servicio de hora parado (spec M17). El ts es la hora de recepcion dentro del loop (RA-02).

**Impacto:** 1-5 min de hueco por reinicio y datos de futuros/contado sin hora del exchange con la que corregir la latencia.

**Recomendación:** (1) Atender CTRL_SHUTDOWN_EVENT en el supervisor y en los hijos para vaciar antes de morir.
(2) Generar _gaps.json automaticamente a partir de _runs y de los silencios.
(3) Activar w32time con NTP de alta precision, o un cliente NTP mejor, y registrar el desfase cada 10 min.
(4) Columna ts_exch (E/T del exchange) en libros y trades nuevos: unos 1-2 B/fila con codificacion delta.

**Riesgo de pérdida de datos:** Reduce la perdida en cada reinicio.

**Esfuerzo:** 1-2 dias.

#### [MINOR] RA-12 — RAM: el SO ya recorta el grabador; presupuesto por escenario

**Evidencia:** Proceso vivo: privados 1,47-1,51 GB; RSS de 932 a 528-643 MiB en 50 min. Host: 47,7 GiB en total y 11,4 GiB disponibles (76 % usados). Sondas: 112-186 MB por proceso nativo de 3 streams y 197-397 MB por ccxt. Prueba de un proceso: 0,54 GB (1 par) y 0,61-1,0 GB (3 pares).

**Impacto:** Bajo presion, los fallos de pagina anaden latencia al loop.

**Recomendación:** Presupuesto: S1 unos 1,65 GB; S2 unos 4 GB (2,5 con nativos); S3 unos 4,5; S4 unos 6 (4 con nativos). Reducir ccxt (RA-07) y limitar la cola del escritor por bytes, no solo a 200k updates.

**Riesgo de pérdida de datos:** Ninguno.

**Esfuerzo:** Incluido en RA-06 y RA-07.

#### [INFO] RA-13 — El ancho de banda no limita; Kraken no comprime

**Evidencia:** Carga util por stream en sabado (bandwidth.py): libro BTC de contado 6,1 KB/s, ETH 7,2, SOL 2,8, DOGE 3,9 y XRP 5,2 (media de 5 nativos); perpetuo BTC 14,3 KB/s (Kraken Futures 29,3: 242 mensajes/s de 1 nivel); trades 0,28 KB/s. Laborable (x2): S0 171 KB/s (1,4 Mbit/s), S1 2,3, S2 4,5, S3 7,0 y S4 12,1 Mbit/s (picos x3, unos 36). Binance, Bybit, OKX y Coinbase negocian permessage-deflate; Kraken (contado v2 y futuros), no.

**Impacto:** Ninguno con fibra residencial.

**Recomendación:** Nada urgente. Agrupar suscripciones por conexion (combined streams de Binance, multi-subscribe de OKX y Bybit) para reducir conexiones y TLS.

**Riesgo de pérdida de datos:** Ninguno.

**Esfuerzo:** -

#### [INFO] RA-14 — Kraken: el libro grabado arrastra niveles fuera de la profundidad 1000

**Evidencia:** analyze_live: kraken tiene 2.252 filas de media por libro completo (max 2.542) con depth=1000 por lado. KrakenRawBook recorta a la profundidad para el CRC, pero BookState y el encoder no, asi que conservan niveles que Kraken ya no actualiza.

**Impacto:** Niveles profundos posiblemente rancios en el replay y en los checkpoints de kraken.

**Recomendación:** Para la auditoria de exchanges: recortar a 'depth' tambien en lo que se emite (borrados explicitos de los niveles que salen), o documentarlo.

**Riesgo de pérdida de datos:** Ninguno.

**Esfuerzo:** 0,5 dias.

#### [INFO] RA-15 — Los PID del contexto estaban caducados

**Evidencia:** El grabador real es el PID 17592 (python -m pavilos) y el supervisor el 17536, lanzados por cmd 17472 el 2026-10-16 08:46:43 tras el arranque. El 17652 era la ejecucion del 10-15 (_runs.jsonl).

**Impacto:** Riesgo de que otros pasos del flujo actuen sobre un PID equivocado.

**Recomendación:** Usar 17592/17536, o resolver el PID por linea de comandos en cada paso.

**Riesgo de pérdida de datos:** Ninguno.

**Esfuerzo:** -

### Medidas

```text
A) GRABADOR EN VIVO (S0), medido 17-10
- PID 17592 (prioridad NORMAL, 28-29 hilos, 332-339 handles).
- CPU del proceso: 51,6 % (16:26-16:28) y 31,9 % (1,58 h; loop 26,5 %, escritor 5,4 %). Media desde el arranque (33,3 h): loop 57,6 % + escritor 26,5 % ≈ 84 %.
- Memoria: privados 1,47-1,51 GB; RSS 0,53-0,93 GB.
- Ciclo del escritor: mediana 8,17 s (p10 7,3; p90 9,1) frente a 5 s nominales.
- Log 10-07..10-17: updates/s mediana 250, p99 477, max 606. Filas/s: mediana 3.193, media 3.399, p99 7.000, max 10.856. 0 descartes y 0 fallos de escritura.
- Lake M17 (10-08..10-16): 2,37-3,19 GB/dia (media 2,946; 3,47 en disco el 10-16) y 147.752 ficheros/dia (10.600 por venue).
- Total lake 319,2 GB / 1.844.200 ficheros; pre-M17 291,2 GB / 424.543 ficheros.

B) BTC EN VIVO POR VENUE (10-16, horas 02/08/14/20). GB/dia | filas por libro completo (max) | peso de pies/completos/incrementales
- coinbase 0,994 | 40.557 (40.736) | 0,02/0,79/0,18
- binance 0,402 | 21.820 (32.272) | 0,06/0,58/0,36
- kucoin 0,300 | 12.804 (23.879) | 0,07/0,47/0,45
- kraken 0,266 | 2.252 (2.542) | 0,08/0,14/0,78
- mexc 0,240 | 4.141 | 0,09/0,21/0,70
- bitstamp 0,181 | 7.187 | 0,12/0,63/0,25
- okx 0,136 | 800 | 0,16/0,09/0,76
- gemini 0,134 | 4.565 | 0,17/0,60/0,24
- bybit 0,103 | 400 | 0,22/0,06/0,74
- bitget 0,096 | 1.000 | 0,23/0,13/0,65
- gate 0,072 | 100 | 0,31/0,02/0,68
- htx 0,071 | 300 | 0,31/0,05/0,66
- cryptocom 0,054 | 100 | 0,41/0,03/0,57
- bitfinex 0,048 | 50 | 0,46/0,02/0,53

C) PROYECCION POR STREAM (laborable, GB/dia) BTC / ETH / SOL, con CPU del sabado (% de un nucleo) B/E/S
- binance 0,400 / 0,408 / 0,340 (1,3 / 0,9 / 0,7)
- coinbase 0,991 / 0,582 / 0,349 (1,8 / 1,5 / 0,9)
- kraken 0,267 / 0,406 / 0,198 (7,5 / 10,7 / 2,6)
- okx 0,138 / 0,147 / 0,101 (0,9 / 0,6 / 0,4)
- bybit 0,104 / 0,115 / 0,065 (1,0 / 0,8 / 0,5)
- bitstamp 0,181 / 0,091 / 0,088 (0,7 / 0,4 / 0,3)
- kucoin 0,301 / 0,179 / 0,334 (23,3 / 4,4 / 34,1)
- gate 0,072 / 0,058 / 0,046 (4,0 / 1,8 / 1,1)
- mexc 0,239 / 0,238 / 0,090 (2,5 / 1,7 / 1,5)
- cryptocom 0,055 / 0,073 / 0,067 (1,2 / 0,6 / 0,6)
- bitget 0,097 / 0,116 / 0,076 (2,7 / 2,7 / 1,6)
- htx (estimado) 0,072 / 0,078 / 0,053
- bitfinex 0,049 / 0,052 / 0,064 (1,5 / 2,7 / 0,8)
- gemini 0,134 / 0,105 / 0,078 (6,8 / 5,1 / 2,3)
- TOTAL de contado: 3,10 / 2,65 / 1,95 GB/dia; CPU 57,8 / 35,4 / 49,0 %.

Perpetuos (sabado -> laborable, GB/dia; CPU B/E/S):
- binanceF: BTC 0,242 -> 0,533; ETH 0,654; SOL 0,260 (1,4 / 1,1 / 0,8)
- bybitL: 0,097 -> 0,168; 0,218; 0,103 (1,2 / 0,8 / 0,8)
- okxS: 0,111 -> 0,250; 0,318; 0,239 (0,8 / 1,3 / 0,7)
- krakenF: 0,239 -> 0,656; 0,323; 0,167 (4,1 / 2,3 / 1,2)
- TOTAL de los 4: 1,61 / 1,51 / 0,77 GB/dia.
- Libros: binanceF 3.979 filas tras la semilla de 1000 (en vivo crece por 2,16); okxS 800; bybitL 400; krakenF 3.261.

D) ESCENARIOS. Formato: GB/dia crudo -> compactado F | TB/ano crudo -> F | ficheros/dia crudo -> compactado | CPU sabado % hoy / con nativos | RAM | Mbit/s laborable (pico)
- S0: 3,10 -> 1,77 | 1,13 -> 0,65 | 147.512 -> 336 | 58 / 21 | 1,5 GB | 1,4 (4,1)
- S1, +4 perpetuos BTC: 4,71 -> 2,69 | 1,72 -> 0,98 | 189.659 -> 432 | 65 / 29 | ~1,65 | 2,3 (6,8)
- S1b, 6 perpetuos: 5,52 -> 3,15 | 2,01 -> 1,15 | 210.732 -> 480 | 69 / ~32
- S2, 4 pares x 14: 9,64 -> 5,50 | 3,52 -> 2,01 | 590.049 -> 1.344 | 191 / 70 | ~4 (2,5 nativos) | 4,5 (13,6)
- S3, S2 + 4 perpetuos x 4 pares + trades: 16,12 -> 8,38 | 5,88 -> 3,06 | 1.517.268 -> 3.456 | 233 / 112 | ~4,5 | 7,0 (21)
- S4, 8 pares: 28,69 -> 14,70 | 10,47 -> 5,37 | 3.034.537 -> 6.912 | 464 / 203 | ~6 (4 nativos) | 12,1 (36)
- S3-lite recomendado (BTC x14 + ETH/SOL en 6 venues nucleo + 4 perpetuos x 3 + 30 streams de trades): ~10,0 -> ~5,7 GB/dia, CPU ~104 % / ~60 %, RAM 2,5-3 GB.
- CPU en laborable: x1,7-2,5. Dias de L: compactado: 433 / 285 / 139 / 92 / 52.
- Compactacion (hora 14 UTC del 10-16, 6.145 ficheros, 155,4 MB): A DuckDB 61,0 %; F 57,1 %; min(A,F) 55,8 %; verificado sin perdidas.

E) PRUEBAS A/B Y DE UN SOLO PROCESO (detalle en RA-02 y RA-03)
- Paradas del loop: en vivo p99 1,2-2,0 s y max 7,6 s; en sonda p99 0,13-0,5 s y max 0,7 s.
- Updates ccxt en vivo frente a sonda: 0,3-42 %.
- 3 pares en un proceso: lag p50 4,7 s; 19 de 42 streams sin datos.

F) DISCOS (GiB libres el 17-10)
- C 486,7-530,3 (990 PRO, sistema, volatil); D 145,6 (980 PRO); E 837,2 (870 QVO); F 269,7 (870 EVO); H 189,5 (990 PRO 'DB2'); K 77,6 y L 714,5 (WD Red 4 TB); G/I/J USB.
- CPU del host: media 83 %; Defender unos 4 nucleos.

G) TRADES
- Compactados: 7-15 B/trade (BTC binance 9,9; ETH 7,2).
- Sabado: BTC binance 7,0/s; coinbase 4,5/s; ETH binance 9,7/s.
- binanceF sin medir (ver RA-10).
```

### Preguntas abiertas

- ¿Autoriza el operador mover los 291 GB pre-M17 de D: a L:, es decir, copiar, verificar, borrar el origen y dejar junctions? Sin ello D: entra en pausa en días.
- ¿Aplica el operador la exclusión de Defender para las raíces del lake y la prioridad ABOVE_NORMAL/HIGH de los grabadores? Es la mitigación más barata de RA-02 y toca seguridad.
- ¿Qué uso tiene H: ('DB2', 990 PRO, 189 GiB libres)? ¿Puede alojar el nivel caliente o un VHDX dedicado de 200-300 GB en F:?
- ¿Se compra disco dedicado antes de S2: HDD de 16-20 TB para frío más una segunda copia? Con el espacio actual la retención eterna no llega a S2+.
- ¿Acepta el operador quitar en frío los checkpoints intermedios ya verificados (−45 % en S0, sin perder información) o se conservan todos?
- Para S2+, ¿los 14 venues por par o un subconjunto núcleo (binance, coinbase, kraken, okx, bybit, bitstamp + perpetuos)? El subconjunto (S3-lite) cuesta un 40-60 % menos y la cola larga aporta poca liquidez.
- ¿La app de paper trading consumirá libros de los grabadores por IPC o mantendrá conexiones propias (doble conexión BTC)?
- ¿Cuál será el 4.º par (DOGE frente a XRP) y cuáles los 5-8? Los modelé como «tipo SOL»; en Kraken, DOGE/XRP generan unas 3-4× más mensajes de libro que SOL.
- Las tasas son de sábado y se pasaron a laborable con factores de 1,7-3,2. Conviene repetir la sonda de tubería un día laborable, en solitario, antes de dimensionar.
- No he medido Binance USDⓈ-M trades/s (la URL antigua no los entrega) ni Bitget/Gate perpetuos. Queda pendiente para el plan de futuros.
- Causa exacta de las paradas largas del loop en vivo, incluidas las de más de 15 s y el panel sin respuesta a las 17:50-18:00 con la CPU del proceso al 10-21 %. Inanición por el host al 83-100 %, Defender o una llamada bloqueante: hace falta la métrica de lag de RA-02, o un py-spy autorizado por el operador, para atribuirla.

---

## Captura de DERIVADOS (perpetuos) para PAVILOS: acceso real desde la IP española, latencia, sincronización, diseño de grabación sin pérdidas, volumen y prioridades (solo lectura del repo y del lake; pruebas en C:/Users/FSOCIETY/AppData/Local/Temp/pavilos_wf/derivados/cap_deriv_claude)

### Resumen

1) ACCESO: los 8 venues de derivados (Binance USDM, Bybit linear, OKX swap, Bitget, Gate, Kraken Futures, Hyperliquid y Deribit) son accesibles desde la IP residencial española. REST da 200 en todos y el handshake WS tarda 0,5-1,9 s. Se hicieron dos lotes de 180 s (17-10, 14:38-14:45 UTC) con BTC y SOL, más ETH en Kraken y Deribit: 0 reconexiones y 0 huecos de secuencia en los 21 libros secuenciados. No hay geobloqueo. Solo exigen login/VIP OKX books-l2-tbt (error 60011) y Deribit *.raw (error 13778). HTX derivados (api.hbdm.vn) presenta un certificado de prueba autofirmado (CN=core1.netops.test) en franjas de fin de semana: 991 de las 1.093 reconexiones de htx del log. Hay que excluirlo.
2) LATENCIA, con offset corregido (el PC va 250-267 ms por detrás de UTC; los 7 venues coinciden ±3 ms). Kraken Futures 27 ms p50: es el más cercano y además el venue de ejecución. Deribit 52, Bybit 84, Gate 113, Bitget 117, Binance depth 125 (p99 417 ms en conexión propia, 1.267 ms si la comparte con bookTicker), OKX 132, Hyperliquid 330-400. El bookTicker de Binance llega a p50 375 ms y p99 2,3 s: no se debe usar.
3) URGENTE (spot): OKX deja de aceptar el puerto 8443 el 31-10-2026 y venues.py:69 lo usa. Sin cambio, el feed OKX spot muere en 14 días. Con el puerto 443 funciona (probado).
4) VALOR, medido con datos que ya existen (aggTrades de Binance USDM de data.binance.vision contra el mid de Binance spot del lake; 15-10 y 16-10, de 12 a 18 UTC). La desviación del basis futuros-spot predice el spot con IC 0,58-0,62 a 5 s y una diferencia entre quintiles de 3,0-3,6 pb a 30-60 s. El CVD de futuros aporta información casi independiente de la presión del libro spot (correlación 0,05-0,18). Sigue por debajo de los 10 pb de comisiones: sirve como piloto, no es una ventaja por sí sola.
5) CONDICIONANTE CRÍTICO: USDT cotiza a 0,99872 USD (−12,8 pb). En el lake, los 9 venues USDT están +12,6 a +14,6 pb por encima de Kraken USD, pero el agregador fija USDT=1,0. La mediana cae en el grupo USDT y la banda de ±10 pb excluye a Kraken, Coinbase, Bitstamp y Gemini. Hay que grabar USDT/USD ya.
6) DISEÑO: raíz y proceso separados del lake spot (con venues desconocidos el replay lanza KeyError) y fuera de D: (146 GiB libres, suelo de 100 GiB). Se mantienen el BookEncoder de M17, la hora y la secuencia del exchange en cada fila, y tablas de trades, series y liquidaciones. Vaciado cada 30 s (libros) y 60 s (tablas), con compactación horaria. Con 5 s salen 17.368 ficheros/día por clave.
7) VOLUMEN, día medio, ±50 %: P1 = Binance BTC+SOL 0,78 GB/d + Kraken Futures BTC+SOL 1,12 GB/d (unos 0,6 con codificación DELTA). P2 = Bybit 0,48 + Deribit 0,30. P3 = OKX 0,55 + Hyperliquid 0,07. P4 = Bitget 0,82 + Gate 0,19. Todo junto suma unos 4,8 GB/d.
8) PRIORIDAD: P0, casi gratis: arreglar el puerto 8443, grabar USDT/USD, backfill diario de data.binance.vision y de las ejecuciones/analytics de Kraken Futures. P1: Binance USDM y Kraken Futures. P2: Bybit (orderbook.1000) y Deribit (DVOL). P3: OKX (trades-all) e Hyperliquid (trades/ctx). P4: Bitget y Gate solo si hace falta.

### Hallazgos

#### [BLOCKER] DER-01 — OKX apaga el puerto WS 8443 el 31-10-2026 y el recorder spot lo usa

**Evidencia:** Changelog oficial de OKX, entrada 'WebSocket port 8443 discontinuation', actualizada el 16-10-2026: 'Public internet: October 31, 2026 … Remove :8443 from your WebSocket URLs' (https://www.okx.com/docs-v5/log_en/). El repo usa wss://ws.okx.com:8443/ws/v5/public (src/pavilos/connectors/venues.py:69). ccxt 4.5.56 también usa wss://ws.okx.com:8443/ws/v5. Prueba: wss://ws.okx.com/ws/v5/public (puerto 443) funcionó 180 s con 13 canales, 0 huecos de seqId y 9,9 msg/s en books BTC. El 8443 todavía funciona hoy (40 s, mark-price 1,07 msg/s).

**Impacto:** A partir del 31-10 el venue OKX spot (uno de los 6 nativos) deja de grabar y el agregador pierde un venue. Cualquier conector de derivados por ccxt también fallaría.

**Recomendación:** Cambiar la URL a wss://ws.okx.com/ws/v5/public (sin puerto) antes del 31-10. Es una línea; el operador decide la ventana de despliegue. Los conectores de derivados deben nacer ya con el puerto 443.

**Riesgo de pérdida de datos:** ALTO: pérdida total de OKX spot desde el 31-10 si no se cambia.

**Esfuerzo:** 5 min de código + despliegue

#### [BLOCKER] DER-02 — Los derivados no pueden ir a la raíz del lake spot ni a D:

**Evidencia:** (a) Aggregator.apply lanza KeyError ante un venue desconocido (src/pavilos/aggregator/aggregator.py:35-41). hour_dirs recorre todos los exchange=* (src/pavilos/persistence/lake_files.py:18-25). replay_snapshots usa VENUE_SPECS (backtest/replay.py:38) y aplica cada update (replay.py:129). Por tanto, un 'exchange=binance_usdm' en D:/pavilos_book_data rompería _pressure_pilot, _pressure_leads y _oos_window. (b) D: tiene 146 GiB libres (df, 17-10 15:37 UTC) y el suelo de pausa es de 100 GiB (core/runtime.py:100). Según el contexto, D: baja ~30 GiB/día por otros proyectos. P1-P4 añadirían 1,9-4,8 GB/día.

**Impacto:** Mezclarlos rompe todos los replays de investigación y acelera la pausa del recorder spot.

**Recomendación:** Raíz propia (p. ej. <disco>/pavilos_deriv/) en un disco no compartido: L: tiene 715 GiB libres (HDD) y C: 485 GiB (sistema). Nunca G:/I:/J: (USB). Proceso propio supervisado (p. ej. python -m pavilos.deriv), separado del bucle de trading. Reutilizar las piezas de M17: BookEncoder, ficheros .tmp atómicos, _runs.jsonl, suelo de disco y conserje propios. Si se quisiera la misma raíz, habría que añadir antes una lista blanca de venues en hour_dirs y replay.

**Riesgo de pérdida de datos:** Alto si se mezcla; ninguno con raíz separada.

**Esfuerzo:** Incluido en el diseño P1

#### [IMPORTANT] DER-03 — USDT no vale 1 USD: el libro combinado mezcla dos grupos separados ~14 pb

**Evidencia:** Kraken REST 17-10 ~15:00 UTC: USDT/USD bid/ask 0,99871/0,99872 (−12,8 pb) y USDC/USD 0,9998/0,9999. En el lake (checkpoints de la hora 13 UTC del 17-10), el mid de cada venue frente al de Kraken da: binance +13,88 pb, okx +14,18, bybit +13,55, bitget +13,78, kucoin +14,55, mexc +13,49, gate +13,70, htx +12,62, cryptocom +13,76, bitfinex +16,10, frente a coinbase −0,33, bitstamp −0,11 y gemini +0,18. PegProvider fija USDT=1,0 (aggregator/normalize.py:21-22) y nadie llama a set_rate (runtime.py:121, replay.py:104). combine.py:65 toma la mediana de los mids, que cae en el grupo USDT (10 de 14), y combine.py:84-87 descarta los niveles del lado equivocado: las asks de los venues USD (≈−14 pb) desaparecen y sus bids quedan fuera de ±10 pb. En perpetuos se ve lo mismo: los USDT están −4 a −6 pb frente a su índice, PF_XBTUSD +1,0 y Deribit +2,3.

**Impacto:** La banda de ±10 pb de la presión (la única pista viva) solo ve venues USDT. Las bandas anchas quedan sesgadas hacia los bids. El sesgo cambia cuando cambia el peg, así que es un cambio de régimen escondido.

**Recomendación:** Grabar ya la serie USDT/USD y USDC/USD: ticker/libro de Kraken (y opcionalmente Coinbase USDT-USD), unos pocos MB/día. Sirve también para corregir ventanas pasadas con el histórico público de Kraken. Después, usarla en PegProvider, en vivo y en replay, y repetir la prueba de la presión con el peg corregido.

**Riesgo de pérdida de datos:** Ninguno en crudo (los precios están en moneda nativa). Riesgo de conclusiones sesgadas.

**Esfuerzo:** Serie: 0,5-1 día. Integración en el agregador y re-test: 1-2 días.

#### [IMPORTANT] DER-04 — Valor medido: los perpetuos de Binance adelantan al spot (2 días con datos existentes)

**Evidencia:** scripts/lead_test.py. Ventanas del 16-10 y del 15-10, 12-18 UTC, 4.321 puntos cada 5 s. Futuros: aggTrades diarios de data.binance.vision (checksum SHA256 OK), usados con un retraso de 0,5 s. Spot: mid de Binance reconstruido del lake. Resultados en el orden 16-10/15-10: basis_dev (precio del perpetuo − mid spot, menos su mediana de 5 min) tiene IC 0,616/0,579 a 5 s, con diferencia de quintiles de 2,76/3,64 pb a 5 s, 3,03/3,63 a 30 s y 3,19/3,11 a 60 s. CVD10 de futuros: IC 0,239/0,217 a 5 s y 0,101/0,122 a 30 s. Presión del libro de Binance spot (±10 pb): IC 0,284/0,198 a 5 s, 0,109/0,044 a 30 s y 0,091/0,007 a 60 s. La correlación entre la presión y el CVD10 es 0,184/0,045. Dentro de cada tercil de la presión, el CVD10 conserva 0,8-2,6 pb.

**Impacto:** El basis y el flujo de los perpetuos dan información adicional y más persistente que la presión del libro spot, que es la base de la cabalgada. Aun así, la magnitud (≤3,6 pb) sigue por debajo de los 10 pb de comisión.

**Recomendación:** Dar a Binance USDM (aggTrade y mid) la prioridad 1. Investigar ya con el histórico gratuito, sin esperar a grabar. Re-probar la cabalgada con un piloto combinado (presión con peg corregido + basis_dev + CVD) y con más días antes de cualquier conclusión.

**Riesgo de pérdida de datos:** No aplica.

**Esfuerzo:** Investigación con datos existentes: 1-2 días.

#### [IMPORTANT] DER-05 — Binance USDM: las URL legadas ya no reciben aggTrade/markPrice; ccxt 4.5.56 usa la legada

**Evidencia:** Aviso oficial: wss://fstream.binance.com/ws y /stream se retiran tras el 2026-04-23. Las rutas nuevas son /public (depth, bookTicker) y /market (aggTrade, markPrice, forceOrder) (https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Important-WebSocket-Change-Notice). Prueba con la URL legada (/stream?streams=markPrice@1s/aggTrade/depth@500ms) durante 90 s: 89 mensajes de depth y 0 de markPrice y de aggTrade. En ccxt.pro, binanceusdm tiene urls.api.ws.future='wss://fstream.binance.com/ws'. 'btcusdt@trade' en /market no emite nada: en USDM por WS solo existe aggTrade.

**Impacto:** Un conector ccxt para futuros de Binance no recibiría trades ni mark/funding, y no daría ningún error.

**Recomendación:** Conector nativo: /public/stream con <s>@depth@100ms en conexión propia y semilla REST /fapi/v1/depth?limit=1000 (peso 20; reglas U≤lastUpdateId≤u y pu=u anterior, verificadas sin huecos). /market/stream con <s>@aggTrade, <s>@markPrice@1s y !forceOrder@arr. Reconexión planificada antes de las 24 h. Límites: 10 msg/s entrantes y 1.024 streams por conexión.

**Riesgo de pérdida de datos:** Alto si se usa ccxt.

**Esfuerzo:** 2 días (se puede partir de binance_connector.py)

#### [IMPORTANT] DER-06 — bookTicker de Binance es lento y degrada el libro si comparte conexión

**Evidencia:** Latencia con offset corregido. En el lote A, depth@100ms y bookTicker en la misma conexión: depth p50 119 / p90 202 / p99 1.267 ms; bookTicker p50 246 / p99 1.814 ms. En la sonda separada (120 s): depth solo p50 125 / p90 133 / p99 417 / máx 1.146 ms; bookTicker solo p50 375 / p90 1.616 / p99 2.337 ms (24.729 mensajes). El retraso del bucle propio fue p99 15,6 ms, así que el retraso es del servidor. aggTrade: push p50 113-138 ms y 260 ms desde la hora del trade.

**Impacto:** bookTicker no aporta frescura frente a depth@100ms y cuesta ~24 kB/s por símbolo.

**Recomendación:** No grabar bookTicker. Poner depth en una conexión propia y los streams lentos en otra. Guardar la hora del exchange para seguir la latencia de forma continua.

**Riesgo de pérdida de datos:** Bajo.

**Esfuerzo:** Incluido en P1

#### [IMPORTANT] DER-07 — Varios canales de libro no cubren ±10 pb en BTC (y tres venues spot tampoco)

**Evidencia:** Libros mantenidos durante 180 s, extensión desde el mid. Bybit orderbook.200 BTC: ±3,9 pb (orderbook.1000: ±24 pb). OKX books, 400 niveles: ±6,3 pb. Hyperliquid l2Book, 20 niveles: ±2,4 pb. Bitget v2 books, 500 niveles: ±18 pb; v3, 1.000 niveles: ±36 pb. Gate obu400: ±18 pb. Libro completo: Binance (diff), Kraken Futures (1.748/1.518 niveles) y Deribit (1.117/1.082). En el lake spot (checkpoints de la hora 13 del 17-10): gate 50 niveles por lado (±4,4-5,7 pb), cryptocom 50 (±3,6-4,5), bitfinex 25 (±3,9-5,0), okx 400 (bids ±9,8) y bybit 200 (±12-15).

**Impacto:** La imbalance de ±10 pb queda truncada en esos venues: se suma solo la parte interior. En spot ya pasa en 3-4 venues.

**Recomendación:** Derivados: Bybit orderbook.1000, Bitget v3 books y OKX books asumiendo ±6 pb. Hyperliquid solo para trades y contexto. Spot: revisar los límites de profundidad de ccxt para gate, cryptocom y bitfinex en la auditoría spot.

**Riesgo de pérdida de datos:** Medio: la profundidad no suscrita no se puede recuperar.

**Esfuerzo:** Bajo

#### [IMPORTANT] DER-08 — Kraken Futures (el venue de ejecución) no se graba; el broker simula con el mid spot y funding 0

**Evidencia:** execution/broker.py:53-54 tiene funding_rate_hourly=0.0 y signals/engine.py:66-67 llama a broker.on_price(analysis.mid), es decir, al mid spot combinado. Medido en PF_XBTUSD: basis +1,0 pb (p10 0,5, p90 1,3) frente al índice; funding relativo horario 1,25e-5 (10,9 %/año); en PF_SOLUSD −3,0e-5/h (−26,7 %/año). Spread 0,12 pb (tick de 1 $). Profundidad a ±10 pb de 11,7/15,4 M$. Latencia 27 ms p50. Trades a 0,3/s (sábado). Hay histórico público de ejecuciones (https://futures.kraken.com/api/history/v3/market/PF_XBTUSD/executions, verificado desde 2024-01-01, 1.000 por página, con órdenes maker/taker) y analytics de 1 min sin clave: open-interest, liquidation-volume, long-short-ratio, funding, future-basis… (https://docs.kraken.com/api-reference/analytics/market-analytics).

**Impacto:** El P&L simulado ignora el basis real del PF y su variación (más los ~14 pb del mid USDT de DER-03) y también el funding.

**Recomendación:** P1: grabar PF_XBTUSD y PF_SOLUSD (PF_ETHUSD opcional): book, trade (type=liquidation), ticker (1 s) y heartbeat. Ping de protocolo cada <60 s. Backfill diario de ejecuciones y analytics. Simular los fills sobre su libro y sus trades.

**Riesgo de pérdida de datos:** Medio: el libro no es recuperable; los trades sí.

**Esfuerzo:** 2-3 días

#### [IMPORTANT] DER-09 — Coste en disco y ficheros: el vaciado de 5 s y la codificación por defecto encarecen los streams pequeños y Kraken

**Evidencia:** Simulación con el BookEncoder real (solo lectura del repo), checkpoints de 60 s y Parquet zstd. Con vaciado de 5 s salen 17.368 ficheros/día por clave y un sobrecoste de ~2-4 kB por fichero. Trades: 49-58 MB/d a 5 s frente a 2-25 MB/d compactados. Series: 61-80 MB/d frente a 3-12. Kraken Futures envía un nivel por mensaje (228 msg/s en BTC, 2,75 deltas por milisegundo del exchange) y sale a 9,6-10,8 B/fila. Con ts como entero, codificación DELTA_BINARY_PACKED en ts/seq y zstd 9 baja a 5,0-5,4 B/fila (−47 a −50 %); Deribit baja un 29 %; Binance empeora un 12 % (allí ya repite ts y seq, y gana el diccionario). Agrupar Kraken por milisegundo del exchange solo ahorra un 26 % (updates −64 %, filas −5,5 %).

**Impacto:** Sin ajustes, P1-P4 suman ~107.000 ficheros/día con vaciado de 30/60 s (~300.000 con 5 s), frente a los 148.000/día actuales del spot, y GB innecesarios.

**Recomendación:** Vaciado de 30 s en libros y 60 s en tablas (los checkpoints curan los libros y los trades se pueden rellenar desde archivo). Compactación horaria automática y verificada, al estilo M17. ts_recv como int64 en ns (time.time_ns()). Elegir la codificación de cada columna por venue al compactar. Ficheros multi-símbolo por venue.

**Riesgo de pérdida de datos:** Bajo.

**Esfuerzo:** 1-2 días

#### [IMPORTANT] DER-10 — El reloj del PC va ~0,25-0,27 s por detrás de UTC y se mueve; hay que guardar la hora del exchange

**Evidencia:** Offset (servidor − PC) medido por REST de hora en 7 venues que coinciden ±3 ms. A las 14:38 UTC: binance 250,3, bybit 251,3, okx 253,0, bitget 254,8, gate 249,1, deribit 250,2, kraken 252,7 ms. Entre 15:26 y 15:36 UTC: deribit 260,4-263,6 y kraken 264,2-267,4 ms (RTT de 42-63 ms, incertidumbre ≤±30 ms). El 06-10 eran 323 ms. Unos +11 ms en 45 min.

**Impacto:** Alinear con datos de otras fuentes (data.binance.vision, histórico de Kraken) arrastra un sesgo de ~0,3 s que además deriva. Las latencias sin corregir engañan.

**Recomendación:** En derivados, guardar en cada fila ts_exch (E/T/cts/ts) y seq_exch, y un _clock.jsonl con el offset medido cada 10 min (REST de hora de Deribit y Kraken). Activar la sincronización horaria de Windows es decisión del operador.

**Riesgo de pérdida de datos:** No.

**Esfuerzo:** Bajo

#### [IMPORTANT] DER-11 — HTX: los dominios de derivados están interceptados con un certificado de prueba en franjas de fin de semana

**Evidencia:** api.hbdm.vn, api.hbdm.com y api.btcgateway.pro resuelven a 43.152.188.49 y presentan un certificado autofirmado 'CN=core1.netops.test, O=Widgits Pty Ltd' (openssl, 17-10 ~16:30 local). api.huobi.pro presenta un certificado válido de Amazon. En logs/pavilos.log, 991 de los 1.093 errores de htx son SSL contra api.hbdm.vn, concentrados el 10-10 de 17 a 23 h, el 11-10 de 14 a 18 h y el 17-10 desde las 16 h (hora local). Es un patrón de bloqueo en días de partido: 1.012 errores en fin de semana frente a 83 en laborables. ccxt htx carga por defecto spot, linear e inverse (ccxt/htx.py, options.fetchMarkets.types).

**Impacto:** Los derivados de HTX son inviables desde esta red. Además, el conector spot de htx reconecta por mercados que no usa.

**Recomendación:** Excluir HTX del plan de derivados. En spot, pasar options {'fetchMarkets': {'types': {'spot': True, 'linear': False, 'inverse': False}}}; esto corresponde a la auditoría spot. Vigilar los venues detrás de Cloudflare (OKX, Deribit, Kraken). Hasta hoy no hay evidencia en spot: kraken tuvo 13 errores en fin de semana frente a 147 en laborables.

**Riesgo de pérdida de datos:** Medio para htx spot.

**Esfuerzo:** Bajo

#### [MINOR] DER-12 — Liquidaciones: la semántica cambia por venue y solo algunos dan todas

**Evidencia:** Todas: Bybit allLiquidation (push cada 500 ms; 0 en BTC/SOL durante 3 min), Kraken (trades con type=liquidation) y Deribit (campo liquidation del trade). Muestreadas o agregadas: Binance forceOrder (solo la última por símbolo cada 1 s; 28 en 3 min con !forceOrder@arr), Gate public_liquidates (agregado de 1 s, !all; 3), Bitget UTA 'liquidation' (solo la mayor long/short por par y segundo, https://www.bitget.com/docs/uta/websocket/public/Liquidation-Channel) y OKX liquidation-orders (2 eventos; según la documentación es un muestreo, no pude re-verificarlo hoy). Hyperliquid no tiene feed público con marca de liquidación.

**Impacto:** Sumar fuentes muestreadas como si fueran completas falsea los picos de liquidación.

**Recomendación:** Tabla liq con columna 'semantics' y unidad de tamaño. Usar los analytics de liquidation-volume (1 min) de Kraken como control.

**Riesgo de pérdida de datos:** Medio: las liquidaciones solo se capturan en vivo (salvo los trades marcados).

**Esfuerzo:** Incluido

#### [INFO] DER-13 — Hay histórico gratuito: no hace falta hacer polling de OI/long-short ni depender solo de los trades en vivo

**Evidencia:** data.binance.vision USDM, verificado del 14-10 al 16-10: trades (24-35 MB zip/día; 3,2-4,6 M fills/día), aggTrades (15-22 MB; 1,19-1,82 M/día), bookDepth (cada 30 s, ±1..5 %) y metrics (OI, ratios long/short de top traders y taker cada 5 min), publicados al día siguiente. El REST de OI/long-short de Binance solo guarda 30 días. Bybit /v5/market/account-ratio tiene datos desde 2020. public.bybit.com/trading/BTCUSDT tiene un CSV diario (verificado el 16-10). Bybit history-data ob500 se descarga a mano (solo fuentes de terceros; por verificar). Kraken Futures: ejecuciones desde 2024 y analytics de 1 min. Deribit: REST de trades por tiempo.

**Impacto:** Se puede investigar ya y auditar la completitud de lo grabado.

**Recomendación:** Tarea diaria de backfill a la raíz de derivados, más un cruce de completitud por ID: los IDs son consecutivos en Binance 'a', OKX trades-all, Gate, Deribit trade_seq y Kraken seq; Bybit seq no es único y Bitget/Hyperliquid no tienen continuidad. En vivo grabar solo lo que no se puede recuperar: libros, hora de recepción, liquidaciones y mark/funding de alta frecuencia.

**Riesgo de pérdida de datos:** Lo reduce.

**Esfuerzo:** 1 día

#### [INFO] DER-14 — Catálogo de streams y límites por venue (documentación oficial actual)

**Evidencia:** Binance USDM: /public <s>@depth@100ms (+REST de 1.000), /market aggTrade, markPrice@1s (mark, índice, funding), forceOrder; OI y long/short solo por REST (/fapi/v1/openInterest, /futures/data/*, 30 días); 24 h por conexión, 10 msg/s, 1.024 streams. Bybit linear: orderbook.{1:10ms, 50:20ms, 200:100ms, 1000:200ms} (u continuo; u=1 = reinicio), publicTrade, tickers (100 ms: funding, mark, índice, OI), allLiquidation; ping cada 20 s; 500 conexiones/5 min. OKX: books (400 niveles, 100 ms, seqId/prevSeqId, checksum fijo a 0 desde el 2026-06-23), bbo-tbt, trades (agregado), trades-all (/business), funding-rate, mark-price, open-interest, index-tickers, liquidation-orders; 3 conexiones/s; puerto 443. Bitget: v2 books/trade/ticker; v3 (UTA) books (maxdepth 1.000, 50 ms, seq/pseq), publicTrade, ticker, liquidation; ping cada 30 s, 10 msg/s, 100 conexiones/IP. Gate: futures.obu ob.X.400 (100 ms, U/u), trades, tickers, public_liquidates; cabecera X-Gate-Size-Decimal:1; tamaños en contratos (BTC 0,0001). Kraken Futures: book (completo, delta por nivel, seq), trade, ticker (1 s), heartbeat; 100 conexiones; ping cada <60 s. Hyperliquid: l2Book (20 niveles, 0,19 msg/s medido), bbo, trades, activeAssetCtx (1/s: funding, OI, oráculo); 10 conexiones/IP, 1.000 suscripciones. Deribit: book.X.100ms (completo, change_id/prev_change_id), trades.X.100ms, ticker.X.100ms (funding_8h, OI), perpetual, deribit_price_index, deribit_volatility_index (DVOL, 1/s); raw exige autenticación; BTC y ETH inversos en USD (contrato de 10 $ / 1 $). URLs en measurements.

**Impacto:** Base para los conectores.

**Recomendación:** Ver el diseño en DER-15.

**Riesgo de pérdida de datos:** -

**Esfuerzo:** -

#### [INFO] DER-15 — Diseño de grabación sin pérdidas propuesto (encaje con M17)

**Evidencia:** Simulado con el BookEncoder real y validado: 0 huecos de secuencia. Tablas propuestas: (1) book/exchange=<venue>.<SYMBOL>/date=/HH con las columnas del lake (seq_no, ts, exchange, is_snapshot, side, price, size) más ts_exch int64 ms y seq_exch int64; checkpoint cada 60 s, base tras 15 s de silencio, marca NaN de pérdida y _runs.jsonl propio. (2) trades: ts_ns, ts_exch, venue, symbol, trade_id, seq_exch, price, size, side ±1, flags (liq/block/RPI/internal), n_fills, first_id/last_id. (3) series: ts_ns, ts_exch, venue, symbol, mark, index, last, funding_rate, funding_interval_h, next_funding_ts, open_interest, bid1, ask1, premium, extra. (4) liq: ts_ns, ts_exch, venue, symbol, side, price, size, size_unit, semantics. (5) _instruments.json (multiplicador de contrato, inverso, tick) y _clock.jsonl.

**Impacto:** Replay fiel, auditable y separado del spot.

**Recomendación:** Tamaños en unidad nativa más metadatos, para no perder exactitud con los contratos de OKX/Gate ni con los inversos de Deribit (normalizar al leer). Una conexión por clase de stream. Proceso separado. Prueba de 48 h en sombra antes de darlo por bueno.

**Riesgo de pérdida de datos:** -

**Esfuerzo:** P1 3-5 días; P2 +2-3; P3 +2

#### [INFO] DER-16 — Recomendación priorizada (valor frente a coste)

**Evidencia:** Volumen de un día medio, ±50 %. P1: Binance USDM BTC+SOL 0,78 GB/d; Kraken Futures BTC+SOL 1,12 GB/d (~0,6 con DELTA); ETH +0,40. P2: Bybit 0,48 y Deribit 0,30 (+ETH 0,11). P3: OKX 0,55 e Hyperliquid 0,07. P4: Bitget v3 0,82 y Gate 0,19. Valor: Binance es el líder de precio (DER-04), con el mayor OI (94,8 k BTC) y backfill gratuito. Kraken es el venue de ejecución y el más cercano (27 ms). Bybit tiene el 2.º OI (59,3 k BTC) y liquidaciones completas. Deribit aporta DVOL y el basis USD a 52 ms. OKX solo cubre ±6 pb en BTC. Hyperliquid tiene un libro pobre. Bitget y Gate repiten USDT y están en Tokio.

**Impacto:** Orden de construcción.

**Recomendación:** P0 (ya, casi gratis): DER-01, la serie USDT/USD (DER-03) y el backfill (DER-13). P1: Binance USDM y Kraken Futures. P2: Bybit y Deribit. P3: OKX e Hyperliquid (solo trades/ctx). P4: Bitget y Gate solo si la investigación lo pide. SOL en todos (todos lo listan; spread de ~1 pb con tick de 0,01; PF_SOLUSD fino: 0,5-0,8 M$ a ±10 pb).

**Riesgo de pérdida de datos:** -

**Esfuerzo:** -

### Medidas

```text
A) RELOJ (offset = hora del servidor − PC, ms; muestra de menor RTT entre 5)
- 14:38 UTC (lote A): binance 250,3 | bybit 251,3 | okx 253,0 | bitget 254,8 | gate 249,1 | deribit 250,2 (RTT 40) | krakenfut 252,7 (RTT 45). Mediana global 252,1.
- 14:42-14:45 UTC (lote B): 250,7 | 252,5 | 254,4 | 251,0 | 253,7 | 251,7 | 255,2.
- 15:26-15:36 UTC (6 rondas): deribit 260,4-263,6 (RTT 42-53) | kraken 264,2-267,4 (RTT 46-63) | binance 262,3-271,8 | bybit 263,3-268,9. El 06-10 eran 323 ms.

B) STREAMS (sábado 17-10, 14:38-14:45 UTC, 180 s; msg/s · kB/s de carga útil descomprimida · latencia p50/p99 ms con offset corregido)
- Binance depth@100ms: BTC 9,8 · 14,4; SOL 9,3 · 5,6 · 119/1.267 compartiendo conexión (125/417 sola).
- Binance bookTicker: BTC 118 · 24,0; SOL 81 · 16,5 · 246/1.814 (sola: 375/2.337).
- Binance aggTrade: BTC 3,4 · 0,7; SOL 1,4 · 0,3 · push 113/397, desde el trade 260 p50.
- Binance markPrice@1s: 1,0 · 0,22 · 151/213. !forceOrder@arr: 0,16 msg/s.
- Bybit orderbook.1000: BTC 5,0 · 8,3; SOL 4,9 · 2,2 · 85/100. orderbook.200: BTC 10,0 · 8,1. orderbook.50: BTC 27,6 · 6,8.
- Bybit publicTrade: BTC 2,2 · 0,9 · 84/93. tickers: BTC 6,2 · 1,1; SOL 4,3 · 0,8.
- OKX books: BTC 9,9 · 9,3; SOL 9,4 · 7,0 · 132/145.
- OKX trades-all: BTC 4,3 · 0,8 · 130/452. trades (agregado): BTC 2,3. mark-price 5/s; index-tickers 2,8/s; open-interest 0,13/s; funding-rate 0,02/s; bbo-tbt 3/s.
- Gate obu400: BTC 10,5 · 2,9; SOL 6,8 · 1,7 · 113/293. trades: BTC 2,4 · 0,5. tickers 0,5/s.
- Bitget v2 books: BTC 10,2 · 7,9 · 116/161. v3 books: BTC 19,9 · 10,9; SOL 20,6 · 6,6 · 118/158. ticker 9,2/s · 5,4 kB/s. trades 0,4/s.
- Kraken Futures book: BTC 228 · 28,3; ETH 85 · 10,3; SOL 36 · 4,3 · 27/75. ticker ~2/s. trade 0,3/s (27 ms). heartbeat 25/41.
- Hyperliquid: l2Book 0,19 · 0,3 · 398/652; bbo 4,4/s · 331/652; trades BTC 0,9 · 0,6 · 344/615; activeAssetCtx 1/s.
- Deribit: book BTC 2,95 · 1,9 · 52/162; ETH 2,9; SOL 2,7; ticker 1,4-2,0/s; trades 0,2/s; DVOL 1/s; índice 0,6/s (24 ms).
- permessage-deflate negociado: Binance, Bybit, OKX, Deribit. No negociado: Kraken, Bitget, Gate, Hyperliquid.
- Retraso del bucle de la sonda: p50 2,6-4,2 ms, p99 13,8-15,5 ms.

C) LIBROS TRAS 180 s (niveles bid/ask · extensión pb · USD a ±10 pb bid/ask · spread pb · huecos)
- BTC:
  binance 2.606/2.627 · 9.877 · 21,3/22,2 M · 0,012 · 0
  bybit1000 1.000/1.000 · 24 · 29,0/25,7 M · 0,012 · 0
  bybit200 · 3,9 pb
  okx 400/400 · 6,3 · 13,3/17,0 M · 0,012 · 0
  gate 400 · 18,4 · 5,4/5,1 M · 0
  bitget v2 500 · 18,1 · 49,7/44,3 M; v3 1.000 · 35,6 · 0
  kraken 1.748/1.518 · 10.000 · 11,7/15,4 M · 0,123 · 0
  deribit 1.117/1.082 · 9.175 · 2,5/2,9 M · 0,061 · 0
  hyperliquid 20 · 2,4 · 3,3/5,4 M · 0,123
- SOL (USD a ±10 pb): binance 2,44/1,99 M | bybit 1,73/1,83 | okx 1,64/2,23 | bitget 1,75/1,88 | hyperliquid 2,08/3,07 | deribit 1,87/2,19 | kraken 0,51/0,82 | gate 0,14/0,20. Spread ~0,96 pb (tick de 0,01); bitget y deribit 0,096.
- 21 libros con secuencia, 0 huecos: Binance pu, Bybit u, OKX prevSeqId, Gate U/u, Bitget pseq, Kraken seq, Deribit prev_change_id.
- IDs de trade consecutivos: Binance 'a' 616/616, OKX trades-all 772/772, Gate 426/426, Deribit trade_seq, Kraken seq. Bybit seq no es único. Binance aggTrade f..l tiene 10-13 discontinuidades (abierto).

D) VOLUMEN (encoder real; filas/s medidas × factor de actividad; factor de libros A 2,4 / B 4,3, de trades 5-7, de series 1,3; compactado | vaciado 30 s; GB/d)
- Por clave:
  binance BTC 0,459 | 0,477 — SOL 0,222 | 0,232
  bybit1000 BTC 0,261 | 0,275 — SOL 0,083 | 0,091
  okx BTC 0,238 | 0,251 — SOL 0,209 | 0,218
  gate BTC 0,077 — SOL 0,043
  bitget v3 BTC 0,497 — SOL 0,232
  kraken BTC 0,906 | 0,915 — SOL 0,144 — ETH 0,361
  deribit BTC 0,125 — SOL 0,084 — ETH 0,082
  hyperliquid l2Book 0,02
  trades 0,01-0,06 por clave; series 0,003-0,012; liquidaciones <0,002
- Por fase (vaciado 30/60 s): P1 Binance 0,78 + Kraken 1,12 (+ETH 0,40) | P2 Bybit 0,48 + Deribit 0,30 (+ETH 0,11) | P3 OKX 0,55 + Hyperliquid 0,07 | P4 Bitget 0,82 + Gate 0,19. Todo: 4,83 GB/d. Un día activo, ×1,25.
- Ficheros: 17.368/día por clave con 5 s (medido); 2.880 con 30 s; 1.440 con 60 s; P1 unos 24.500/día.
- Medido en sábado, sin factor (crudo de 5 s / compactado, MB/d): binance BTC 304/200, kraken BTC 289/234, bybit1000 BTC 196/113, okx BTC 179/101.
- Calibración en el lake spot: la ventana tuvo 0,21-0,52× las filas por segundo del 16-10. Binance USDM hizo 1,19-1,82 M aggTrades/día (13,8-21,1/s) frente a 3,44/s en la ventana. Lake spot por día (MB): binance 313-523, kraken 124-303, okx 69-173, bybit 61-131.

E) PRUEBA DE VALOR (Δ quintil alto − bajo en pb · IC Spearman; 16-10 / 15-10)
- basis_dev: 5 s 2,76·0,616 / 3,64·0,579 | 30 s 3,03·0,234 / 3,63·0,228 | 60 s 3,19·0,177 / 3,11·0,128
- lead5: 5 s 1,86·0,381 / 2,66·0,393 | 30 s 2,25 / 3,28 | 60 s 2,01 / 3,08
- cvd10: 5 s 0,98·0,239 / 1,45·0,217 | 30 s 1,56·0,101 / 2,11·0,122 | 60 s 1,98·0,083 / 1,95·0,078
- imb10 (libro de Binance spot): 5 s 1,14·0,284 / 0,93·0,198 | 30 s 1,44·0,109 / 0,63·0,044 | 60 s 1,78·0,091 / 0,16·0,007
- Correlación imb10-cvd10: 0,184 / 0,045. N = 4.321 por ventana.

F) BASIS, FUNDING Y OI (instantánea del 17-10)
- Basis mediano (pb) BTC/SOL: binance −5,4/−6,4 | bybit −4,5/−5,7 | okx −4,5/−5,3 | gate −4,8/−5,8 | bitget −4,7/−5,6 | hyperliquid −4,1/−5,8 | kraken +1,0/−0,5 (ETH +0,4) | deribit +2,3/+2,9 (ETH +3,4).
- Funding anualizado BTC: binance 5,2 % | bybit 8,2 | okx 11,0 | gate 7,0 | bitget 3,3 | kraken 10,9 | hyperliquid 10,1 | deribit 0,4. SOL: kraken −26,7 %.
- OI BTC: binance 94.786 (metrics) | bybit 59.302 | hyperliquid 41.083 | bitget 34.681 | okx 30.470 | kraken 2.237 | deribit 839 M$ | gate 50.918 (dudoso).

G) SPOT Y PEG
- Cobertura del lake spot: ver DER-07.
- Mid frente a Kraken: USDT +12,6 a +14,6 pb; USD −0,33 a +0,18.
- Kraken USDT/USD 0,99871/0,99872; USDC/USD 0,9998/0,9999.

H) CODIFICACIÓN (B/fila: actual → ts int + DELTA + zstd 9)
- kraken 9,58 → 5,04 | deribit 6,41 → 4,55 | okx 4,07 → 3,98 | binance 3,62 → 4,05
- Kraken agrupado por ms del exchange: 10,75 → 8,41 (por defecto) y 5,26 (con DELTA).

I) HTX
- 1.093 errores en el log: 991 SSL contra hbdm. Por día: 10-10 519, 10-11 463, 10-17 28.

URLs:
- https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams (y …/Important-WebSocket-Change-Notice, …/How-to-manage-a-local-order-book-correctly, …/Connect, …/Liquidation-Order-Streams, …/market-data/rest-api/Open-Interest-Statistics)
- https://data.binance.vision
- https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook (…/ticker, …/all-liquidation, …/v5/ws/connect, …/v5/market/long-short-ratio)
- https://public.bybit.com/trading/
- https://www.okx.com/docs-v5/log_en/
- https://status.tardis.dev/incidents/bv68jvpj10z9
- https://www.bitget.com/docs/uta/websocket/public/Order-Book-Channel (…/Liquidation-Channel, …/uta/best-practices-guide, …/classic/websocket/intro)
- https://www.gate.com/docs/developers/futures/ws/en/
- https://docs.kraken.com/api/docs/futures-api/websocket/book (…/ticker, …/trade)
- https://docs.kraken.com/exchange/guides/futures/ratelimits.md
- https://docs.kraken.com/api-reference/analytics/market-analytics
- https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions (…/rate-limits-and-user-limits)
- https://docs.deribit.com/articles/market-data-collection-best-practices
```

### Preguntas abiertas

- ¿Qué disco usamos para la raíz de derivados? L: tiene 715 GiB libres (HDD) y C: 485 GiB (sistema, ha bajado desde 552 GB). ¿Quién usa F: y H:? En D: no cabe sin hacer antes M17b.
- ¿Grabamos los tamaños en unidad nativa más metadatos (recomendado, exacto) o en unidades base, como el lake spot?
- ¿Se aplica ya el peg USDT/USD en el agregador vivo? Cambia el mid y la presión que ve el paper trading. ¿O primero solo se graba la serie y se repite la prueba de la presión?
- ¿Entra ETH en P1 (Kraken y Deribit, +0,5 GB/d), en vista de la decisión de pares de otro agente?
- Semántica de OKX liquidation-orders: no pude re-verificarla hoy porque la página oficial es demasiado grande para la herramienta. Confirmar.
- El OI de Gate (total_size de 509 M contratos ≈ 50,9 k BTC) parece contar los dos lados. Verificar antes de usarlo.
- Binance aggTrade muestra 10-13 discontinuidades en first/last trade id en 3 min (¿excluye fills RPI o ADL?). Cruzar con el fichero diario de trades.
- Estabilidad de 24-48 h de cada WS: solo se probaron 3 min. Queda por ver el corte de 24 h de Binance, el aviso 64008 de OKX y los bloqueos de Cloudflare en días de partido. Hace falta una prueba en sombra antes de dar P1 por hecho.
- Bybit history-data ob500 (descarga manual) solo está confirmado por fuentes de terceros. Verificar disponibilidad y condiciones antes de contar con él como respaldo.
- En esta carpeta de trabajo (derivados) escriben también otros agentes del flujo con la misma etiqueta. Mis resultados están copiados en cap_deriv_claude/; conviene no reutilizar la etiqueta.

---

## Condicionantes del precio, además del libro spot, para la cabalgada de 5-60 min (BTC): evidencia, disponibilidad gratuita verificada desde esta máquina, coste de grabarlos y forma de usarlos

### Resumen

Revisé más de 20 condicionantes de tres formas:
- literatura;
- sondas en vivo desde esta máquina: 54 REST y 9 WS de 4 min;
- pruebas propias: 12 meses del perp BTCUSDT de Binance a 1 min (525.600 min; 24 meses para el calendario), 6 meses de Coinbase/USDT-USD y DVOL, 3 meses de Upbit, 12 meses de las analíticas de Kraken Futures, ~4 semanas de ES/NQ/DXY y 5 horas del lake M17 (solo lectura).

1) BLOQUEANTE: el peg fijo USDT=1,0 sesga la presión ±10 pb que pilota la cabalgada.
- Código: normalize.py:22 fija USDT=1,0; set_rate no se llama nunca.
- Mercado: USDT/USD está hoy a −13,3 pb; ha pasado de −2 a −14 pb durante el periodo M17 y en 2 años ha oscilado entre −21,5 y +15,6 pb en días sueltos.
- En 5 h del lake:
  - el desequilibrio de producción queda desplazado entre −0,09 y −0,18 (−0,011 por pb; r=−0,92);
  - los venues USD pasan de aportar ~30 % de las pujas de la banda a 0,5-14 %;
  - el signo coincide con la versión corregida solo el 27-92 % del tiempo;
  - los 11 cruces por debajo de −0,3 fueron todos cortos (con el peg corregido hubo 3).

2) Ningún condicionante público supera solo los 10 pb a 5-60 min. El régimen base de BTC es reversión (IC ret_15 −0,047, t −8,9).
- CVD/agresor, OI, funding, base, ratios L/S y libro del perp: |IC| ≤ 0,045, explicado casi entero por esa reversión (t parcial < 2,5); diferencial de deciles ≤ 3 pb.
- La prima de Coinbase parecía predecir (IC 0,02-0,04), pero con 1 min de retraso cae a 0,005-0,009: era un artefacto de asincronía.
- Prima coreana: nada.
- ES/NQ/DXY: solo se mueven a la vez que BTC (r 0,39/0,41/−0,19), sin adelanto de más de 1 min.

3) Donde sí aportan es en el régimen de volatilidad.
- La volatilidad futura se predice bien (rv_60 IC 0,78).
- Suman información incremental: liquidaciones (t parcial 20), DVOL (14), volumen relativo (12), |ΔOI| (11), |Δprima| (9,5).
- Proxy de cabalgada con el libro del perp: con volatilidad baja el bruto es negativo en 4/4 variantes; la sesión EE.UU. (13-21 UTC) mejora en 4/4.

4) Calendario.
- CPI/NFP/FOMC multiplican la volatilidad ×2,4-3,2 en 15 min, y el primer movimiento tiende a revertir (NFP −23 pb, FOMC −20 pb; no significativo).
- Fin de semana: volatilidad ×0,68 y más reversión.
- Pico 15 UTC frente a valle 10 UTC: ×2,2.
- Vencimientos de opciones y liquidación de funding: sin efecto.

5) Liquidaciones grandes (Kraken, n=46): el movimiento revierte −16/−21 pb a 5/15 min (t −1,7), con movimientos ×4.

Clasificación:
- IMPRESCINDIBLES: USDT/USD en vivo para el peg, régimen de volatilidad (DVOL + OI + liquidaciones), calendario/sesiones, liquidaciones de 3 venues y trades de los perps. Coste ~30-60 MB/día (≈1-2 % del lake) si se escriben en lotes de ≥60 s.
- ÚTILES: funding/mark/base, cadena de opciones de Deribit, bookDepth gratis de Binance para un pre-test con 9 meses.
- DESCARTAR a 5-60 min: primas Coinbase/coreana, ES/NQ/DXY, ETF, stablecoins/on-chain, ballenas, heatmaps, noticias y max pain.

Mucha historia se descarga gratis (data.binance.vision, analíticas de 1 min de Kraken Futures con ≥1 año, DVOL, velas): no hace falta grabarla para investigar.

Red: el ISP intercepta TLS (certificado autofirmado core1.netops.test) en IPs bloqueadas en horario de partidos. Explica los 986 errores de htx (api.hbdm.vn, solo en fines de semana) y hoy bloquea farside, cryptopanic y frankfurter.

### Hallazgos

#### [BLOCKER] F01 — El peg fijo USDT=1,0 sesga la presión ±10 pb que pilota la cabalgada (sesgo de −0,09 a −0,18 que varía con USDT/USD)

**Evidencia:** Código: src/pavilos/aggregator/normalize.py:22 fija `Quote.USDT: 1.0`; runtime.py:121 y backtest/replay.py:104 crean PegProvider() sin tasas; set_rate (normalize.py:29) no se llama en ningún sitio. combine.py:65 toma la mediana de mids (sin gemini/kucoin hay 8 venues USDT y 4 USD, así que la mediana cae en el racimo USDT) y combine.py:84-86 descarta los niveles del lado equivocado. Mercado (sondas REST de hoy): USDT/USD 0,99867-0,99869 en Kraken, Coinbase y Bitstamp (−13,3 pb). Kraken 1 h: de −2,0 pb (10-06) a −14,1 pb (10-17). En 2 años: medias mensuales entre −10,6 y +5,0 pb; días sueltos entre −21,5 y +15,6. Coinbase 1 min en 6 meses: mediana −7,2 pb. Replay de 5 ventanas de 1 h del lake M17 (paso 5 s, 12 venues), con A = producción, B = USDT real y C = banda local por venue: base medida entre racimos de 5,6 a 13,9 pb. Desequilibrio medio: A de −0,100 a −0,175; B de −0,061 a +0,023; C de −0,040 a +0,011. Sesgo A−B = −0,0111·pb − 0,025 (r=−0,92), que son 2-4 desviaciones típicas del indicador (sd 0,041-0,053). Cuota de pujas USD en la banda: 0,5-14 % en A frente a 28-31 % en B. El signo de A coincide con B solo el 27-92 % del tiempo. Cruces por debajo de −0,3: 11 en A (todos cortos, 0 largos) frente a 3 en B. Ficheros: results/usdt_peg_summary.json y usdt_peg_w*.json.

**Impacto:** La presión en vivo, el dashboard y todo replay están empujados hacia 'venta' en una cantidad que deriva con USDT/USD. Los 17 trades e0.3/x−0.2 de RESEARCH_FINDINGS y cualquier re-test quedan contaminados en dirección y en frecuencia.

**Recomendación:** 1) Peg vivo: suscribir USDT/USD (WS ticker de Kraken, más Coinbase o Bitstamp como respaldo) y llamar a PegProvider.set_rate cada ≤10 s, con límites de cordura y, si falla, la última tasa buena. 2) Replay: aplicar la serie histórica USDT/USD de 1 min (velas Coinbase USDT-USD, ya descargadas para 6 meses). 3) Alternativa robusta: medir la banda alrededor del mid de cada venue y sumar (variante C, corr 0,73-0,93 con B); es inmune a primas de USDT y de venue. 4) Repetir _pressure_leads y _pressure_pilot con la corrección antes de cualquier veredicto sobre la cabalgada.

**Riesgo de pérdida de datos:** Ninguno: el lake guarda precios nativos por venue y la corrección es retroactiva en replay.

**Esfuerzo:** S (2-4 h de código + re-ejecución de los estudios)

#### [IMPORTANT] F02 — Ningún condicionante público, por sí solo, supera los 10 pb a 5-60 min; el régimen base de BTC es reversión

**Evidencia:** 12 meses de perp Binance a 1 min, IC de Spearman en muestras no solapadas y t parcial frente a ret_5/15/60 (OLS HC1). Precio: ret_15→15 min −0,047 (t −8,9; mismo signo en 92 % de los meses); ret_60→60 min −0,035 (t −3,2). Flujo agresor/CVD: ti_perp_15 −0,037 (t −6,9; parcial −1,1); cvdz_spot_15 −0,044 (t −8,3; parcial −1,6). OI: ΔOI 15→60 min −0,033 (t −3,1; parcial −1,4). Funding, prima, base perp-spot y ratios L/S: |IC| ≤ 0,025 y |t parcial| ≤ 2,4. Libro del perp (bookDepth ±1 %): +0,039/+0,045 (t 7,3/4,2) pero t parcial −0,2. Kraken F: CVD −0,026 (parcial −0,5). Diferencial decil superior−inferior ≤ 3 pb (excepción no monótona: dlsr_acc_60 −5,2 pb a 60 min). Seguir el momentum de 15 min da −0,44 pb a 15 min (t −3,5) y +0,43 a 60 (t 0,9). Literatura: el flujo explica el precio simultáneo (Silantyev 2019, ideas.repec.org/a/spr/digfin/v1y2019i1d10.1007_s42521-019-00007-w.html); la microestructura predice volatilidad y autocorrelación, no la dirección (Easley-O'Hara-Yang-Zhang 2024, stoye.economics.cornell.edu/docs/Easley_ssrn-4814346.pdf). Futuros: aún adelantan al spot, pero menos tras los ETF (arxiv.org/pdf/2506.08718).

**Impacto:** Añadir fuentes no crea por sí mismo una ventaja direccional que pague las comisiones. La cabalgada tiene que vivir de movimientos grandes, y los condicionantes sirven como filtro de régimen y de salida.

**Recomendación:** No construir señales direccionales con funding, OI, ratios o primas. Usarlos para (a) filtrar entradas por régimen y (b) gestionar salida y tamaño. Todo condicionante nuevo se evalúa con el mismo protocolo: IC no solapado, t parcial frente a momentum y diferencial en pb frente a 10 pb (scripts analysis_ic.py y analysis_ext.py).

**Riesgo de pérdida de datos:** Ninguno.

**Esfuerzo:** —

#### [IMPORTANT] F03 — La volatilidad futura sí se predice, y es el mejor uso de los condicionantes (filtro, tamaño, stops)

**Evidencia:** IC con la volatilidad realizada futura (15 | 60 min): rv_60 0,781 | 0,794. Información incremental sobre rv_60 (t parcial, 15 | 60 min): liquidaciones Kraken 20,1 | 4,9; DVOL 13,9 | 10,7; volumen relativo 11,9 | −0,7; |ΔOI 15| Binance 11,0 | 3,3; |ΔDVOL 15| 9,8 | 3,7; |Δprima 15| 9,5 | 5,2; tamaño medio de operación 8,2 | 0,2; libro fino ±0,2 % 4,0 | 3,2. Cabalgada proxy (libro del perp, 9 meses): el tercil de volatilidad baja tiene bruto negativo en 4/4 variantes (de −1,1 a −21,9 pb por operación); el tercil alto da +1,8 a +6,3. DVOL de 1 min es gratis por REST (6 meses descargados en ~2 min).

**Impacto:** El estudio original (unas 12 h, sobre todo de baja volatilidad) cayó en el peor régimen. Filtrar por régimen puede subir el bruto por operación sin tocar la señal.

**Recomendación:** Puerta de entrada por volatilidad esperada: f(rv_60 del propio libro, DVOL, |ΔOI|, liquidaciones, volumen relativo). Escalar stop, salida y tamaño con esa volatilidad esperada. Fijar el filtro antes del re-test para no sobreajustar. Fuentes: Deribit WS deribit_volatility_index.btc_usd (1 Hz); OI por WS de Bybit tickers y OKX open-interest; historia gratis en data.binance.vision metrics (5 min) y analíticas de Kraken.

**Riesgo de pérdida de datos:** Ninguno: la historia es descargable gratis.

**Esfuerzo:** M

#### [IMPORTANT] F04 — Calendario: la macro multiplica la volatilidad ×2,4-3,2 y el primer movimiento tiende a revertir; fin de semana y sesiones importan

**Evidencia:** 24 meses de perp Binance. CPI (n=24): volatilidad realizada 0-15 min ×3,24, 0-60 ×2,02, hora previa ×1,06; |ret 0-15| mediana 48,7 pb; el movimiento de los 5 primeros minutos sigue en 5→60 min el 62 % de las veces, +6,4 pb (t 0,6). NFP (24): ×2,37 / ×1,70; continuidad 33 %, −23,2 pb (t −1,6). FOMC (16): ×2,80 / ×2,24; 38 %, −19,9 pb (t −0,9). Laborables: volatilidad máxima a las 15 UTC (10,9 pb/min) frente a la mínima a las 10 UTC (4,95): ×2,2. Fin de semana: volatilidad ×0,68; autocorrelación de 15 min −0,089 (sesión EE.UU.) y −0,117 (21-24 UTC), frente a ≈0 en laborables. Proxy de cabalgada: la sesión EE.UU. 13-21 UTC mejora en 4/4 variantes (+1,6 a +19,8 pb bruto). Vencimientos del viernes 08:00 UTC: ×0,99 (último viernes ×1,14, n=12). Liquidación de funding 00/08/16 UTC: |t| ≤ 1,13 (n=2.189). Fechas de BLS (cpi.htm, empsit.htm) y de la Fed (fomccalendars.htm). El calendario JSON de ForexFactory responde (horas exactas e impacto). Literatura: Benigno-Rosa NY Fed SR1052; Ben Omrane et al. 2025; Hansen-Kim-Kimbrough 2021 (arxiv.org/pdf/2109.12142).

**Impacto:** Entrar con presión justo tras CPI/NFP/FOMC es entrar en el movimiento que más a menudo revierte. El fin de semana añade vaivén que se come las comisiones.

**Recomendación:** Bloquear entradas entre T−5 y T+30 min en CPI, NFP y FOMC (y en PPI y ventas minoristas de alto impacto), con el calendario de ForexFactory nfs.faireconomy.media/ff_calendar_thisweek.json más BLS y Fed. Tratar el fin de semana aparte (umbral más alto o sin operar). Priorizar 13-21 UTC en laborables. Coste de grabación: cero.

**Riesgo de pérdida de datos:** Ninguno.

**Esfuerzo:** S

#### [IMPORTANT] F05 — Liquidaciones: el movimiento revierte tras una cascada; hay que grabarlas en vivo porque solo Kraken ofrece historia gratis

**Evidencia:** Analíticas de Kraken PF_XBTUSD a 1 min, 12 meses: hay liquidación en el 3,8 % de los minutos (media 29/día, unidades no documentadas). Eventos del top 0,5 % (n=46, como mucho uno cada 30 min): el movimiento de 5 min revierte −16,3 pb a 5 min (t −1,68), −21,3 a 15 (t −1,71), −13,4 a 30 y −4,4 a 60, con movimientos ×4,7 a ×2,9. Control sin liquidación: −0,6 / −6,2. Sin efecto direccional propio (IC≈0 aparte del momentum). Sondas: Binance wss://fstream.binance.com/market (!forceOrder@arr: 53 msgs en 4 min; BTCUSDT: 1). Desde 2026-04-10 Binance solo envía la MAYOR liquidación por símbolo cada 1000 ms (changelog oficial), así que el flujo está muestreado; K33 denuncia infra-reporte de liquidaciones en los exchanges (forklog). Las rutas legacy /ws y /stream dejaron de emitir los canales /market el 2026-04-23. Bybit allLiquidation.BTCUSDT empuja todas, cada 500 ms (doc oficial; 0 en 4 min de sábado). OKX liquidation-orders: 12 msgs en 4 min para todas las swaps. data.binance.vision no tiene dataset de liquidaciones para USD-M.

**Impacto:** La cabalgada puede entrar a favor de una cascada justo antes de que revierta. Además, las liquidaciones son el mejor predictor incremental de volatilidad (F03).

**Recomendación:** Grabar las 3 fuentes WS (<1 MB/día) con hora de evento y de recepción. Uso: no entrar en la dirección de una cascada en curso; recoger o ceñir la salida cuando aparece una cascada a favor de la posición. Investigar ya con la historia de Kraken y, con 2-3 semanas grabadas, comparar los recuentos de Binance (muestreado), Bybit (todas) y OKX.

**Riesgo de pérdida de datos:** Sí, si no se graban: Binance, Bybit y OKX no publican historia descargable de liquidaciones.

**Esfuerzo:** S-M

#### [IMPORTANT] F06 — Gran parte de la historia se descarga gratis: grabar solo lo que no se puede recuperar

**Evidencia:** Verificado y descargado en esta tarea:
- data.binance.vision: aggTrades y trades diarios; klines 1 m con volumen agresor; premiumIndexKlines 1 m; metrics de 5 min con OI y ratios L/S desde 2020; bookDepth del perp cada 30 s con bandas ±0,2 a ±5 % desde 2023 (±0,2 % desde 2026-01-20); fundingRate. 850 ficheros, 275 MB, a 1 fichero/s desde S3.
- Analíticas públicas de Kraken Futures futures.kraken.com/api/charts/v1/analytics/PF_XBTUSD/<tipo>: 1 min, ≥1 año, máximo 2.000 puntos por petición. Tipos: liquidation-volume, cvd, open-interest, future-basis, funding, long-short-ratio, top-traders, liquidity, slippage, spreads (liquidity y slippage se refrescan cada pocos minutos).
- Deribit DVOL: 1 min (6 meses) y 1 h (2 años).
- Velas de 1 min de Coinbase (BTC-USD, USDT-USD) y Upbit.

**Impacto:** La investigación de condicionantes no tiene que esperar semanas de grabación: se puede probar ya con 9-24 meses.

**Recomendación:** No grabar lo que ya tiene historia gratis (funding, OI, premiumIndex, DVOL, klines). Grabar: liquidaciones (F05); trades del venue de ejecución y de los perps principales, con nuestra hora (F07); USDT/USD (F01); cadena de opciones solo si se quiere investigar skew/GEX (F08). Añadir un descargador diario de estas fuentes a un directorio aparte del lake.

**Riesgo de pérdida de datos:** Bajo: las fuentes podrían retirar historia en el futuro.

**Esfuerzo:** S

#### [MINOR] F07 — Trades (CVD, agresor, tamaño): baratos; valen como confirmación, para predecir volatilidad y para el broker, no como señal

**Evidencia:** Coste medido: aggTrades de Binance UM del 2026-10-15 (día activo), 1,82 M filas → 8,3 MB en parquet zstd (4,5 B/fila); spot 1,11 M → 6,3 MB. Estimado para un día medio (actividad del sábado ×2,1): Bybit ~1 M filas (~5 MB), OKX ~0,56 M (~3 MB), Coinbase ~0,66 M (~3 MB), Kraken F ~0,13 M (~0,7 MB), Hyperliquid ~0,2 M (~1 MB). Latencia p50 evento→recepción: Kraken F 34 ms, Coinbase 66, Bybit 88, OKX 135, Binance 136, Hyperliquid 361. Poder predictivo: ver F02 (reversión) y F03 (tamaño medio y volumen predicen volatilidad, t parcial 8-12).

**Impacto:** ~20-35 MB/día (≈1 % del lake) en ficheros grandes. Con el vaciado actual cada 5 s, cada flujo añadiría ~17.000 ficheros/día.

**Recomendación:** Grabar los trades de PF_XBTUSD en Kraken Futures (realismo de fills del broker) y de los perps Binance/Bybit/OKX más Coinbase spot. Hacerlo con un escritor auxiliar en lotes de ≥60 s o con compactación diaria, no con el patrón de 5 s del libro. No esperar señal direccional de ellos.

**Riesgo de pérdida de datos:** Bajo para Binance (historia gratis); medio para el resto.

**Esfuerzo:** M

#### [MINOR] F08 — Opciones: el DVOL sí sirve (régimen); GEX, max pain y skew no tienen evidencia a minutos, y Deribit ya no es la mayoría del mercado

**Evidencia:** DVOL predice volatilidad (F03) pero no dirección (ΔDVOL 15, t parcial 1,25). Con DVOL alta el momentum es peor (−0,68 pb a 15 min, t −2,1). Max pain: folclore, sin estudios. GEX: notas de proveedores (Glassnode) sin test. Skew/risk-reversal: predice a diario y se debilitó tras 2024 (tesis SMU 2026). Flujo informado en opciones OTM: Alexander et al., arXiv 2109.02776. IBIT superó a Deribit en OI de opciones (Bloomberg 2025-09-29: ~38 frente a 32 mil M$), y los datos de IBIT no son gratis en tiempo real. Coste: un snapshot REST de Deribit pesa 418 KB (~938 instrumentos); cada 5 min son ≈270 k filas/día (~5 MB). El WS markprice.options da ~20 filas/s (~1,7 M/día, ~15-25 MB).

**Impacto:** Un GEX solo con Deribit sería incompleto; sin historia de OI por strike no se puede probar ahora.

**Recomendación:** Grabar o descargar el DVOL ya. Hacer snapshot de la cadena de opciones cada 5 min solo si se quiere investigar skew/GEX más adelante. No usar max pain.

**Riesgo de pérdida de datos:** El OI por strike de la cadena de opciones no tiene historia gratis.

**Esfuerzo:** S

#### [MINOR] F09 — Primas: ni la de Coinbase ni la coreana predicen a 5-60 min (la de Coinbase era un artefacto de asincronía)

**Evidencia:** Coinbase (USD puro, descontando USDT/USD; 6 meses): sin retraso, IC +0,020 (t 4,6) a 5 min y +0,037 (t 2,4) a 60. Con 1 min de retraso cae a +0,005 (t 1,2) y +0,009 (t 0,6), con mitades y meses inestables (4-5 de 7 meses positivos); results/robust_cbprem.json. Prima coreana (Upbit KRW-BTC/KRW-USDT frente a Binance; 3 meses): media 0,0 pb, sd 6,3; |IC| ≤ 0,017, |t| ≤ 2,1; no predice volatilidad (t parcial 0,9). USDT/USD no predice el precio de BTC (|t|<2). Literatura: Choi-Lehar-Stauffer (prima coreana estructural, a diario); de la prima de Coinbase solo hay comentarios de analistas.

**Impacto:** Grabar Upbit o Bithumb (RTT 660 ms) no aporta a 5-60 min.

**Recomendación:** Descartar ambas como señal de 5-60 min. La prima de Coinbase se calcula gratis desde el lake (quizá aporte algo a escala de segundos).

**Riesgo de pérdida de datos:** Ninguno.

**Esfuerzo:** —

#### [MINOR] F10 — Bolsa y dólar: se mueven a la vez que BTC pero no lo adelantan; no hay tiempo real gratis de CME

**Evidencia:** ~27.000 min de Yahoo a 1 m (~4 semanas). Correlación simultánea con BTC: ES 0,394, NQ 0,410, DXY −0,188, VIX −0,254, IBIT 0,697. Adelanto a 1 min ≤0,023 (BTC adelanta a IBIT: 0,042). IC de ES/NQ/DXY a 5-60 min: |t| ≤ 2,7 (DXY 1 min→5 min, t parcial −3,3, muestra corta). Yahoo da CME con 10 min de retraso (help.yahoo.com/kb/SLN2310.html). Alternativa gratis en tiempo real 24/7 (WS verificado): perps HIP-3 de Hyperliquid xyz:XYZ100 (243 M$ en 24 h) y xyz:SP500 (224 M$).

**Impacto:** Sin valor como señal de 5-60 min.

**Recomendación:** Descartar como señal. Como mucho, filtro de régimen risk-off (caídas fuertes de XYZ100/SP500 vía Hyperliquid). Prioridad baja.

**Riesgo de pérdida de datos:** Ninguno.

**Esfuerzo:** S

#### [INFO] F11 — Descartar a 5-60 min: ETF, stablecoins/on-chain, ballenas, heatmaps, noticias/sentimiento

**Evidencia:** - ETF: flujos diarios publicados tras el cierre que siguen al precio.
- Stablecoins: Griffin-Shams (horario, 2017-18) frente a Lyons-Viswanath-Natraj (cepr.org/voxeu: la emisión responde al peg, sin efecto sistemático).
- Ballenas/inflows: evidencia diaria (arXiv 2211.08281; WP de la Fed). Whale Alert no tiene API gratis (29,95 $/mes, WS solo uso personal; developer.whale-alert.io/pricing.html).
- Heatmaps de liquidación: modelos con apalancamiento supuesto, sin backtests publicados. Coinglass cuesta 29-699 $/mes; verificado 401 'API key missing'.
- Sentimiento: significativo hasta 15 min, pero sin beneficio económico (Guégan-Renault).
- Noticias: CryptoPanic exige clave y hoy está bloqueado por el ISP. El RSS de CoinDesk responde. Tree of Alpha conecta pero dio 0 mensajes en 40 s (no verificado si exige autenticación).
- DefiLlama (diario) y mempool.space responden.

**Impacto:** Coste o folclore sin retorno a esta escala.

**Recomendación:** No invertir en estas fuentes para la cabalgada.

**Riesgo de pérdida de datos:** Ninguno.

**Esfuerzo:** —

#### [IMPORTANT] F12 — El ISP intercepta TLS en IPs bloqueadas en horario de partidos: afecta a htx y a fuentes tras Cloudflare compartido

**Evidencia:** openssl s_client hoy: farside.co.uk, api.frankfurter.app y cryptopanic.com (Cloudflare 188.114.96.5/188.114.97.5) y api.hbdm.vn (43.152.188.49) presentan un certificado autofirmado 'CN=core1.netops.test, O=Widgits Pty Ltd, OU=Packetland' (verify 18). En logs/pavilos.log hay 986 'Cannot connect to host api.hbdm.vn', y solo en tres franjas (hora local): sábado 10-10 17:47-23:57, domingo 10-11 14:35-18:52 y sábado 10-17 desde las 16:21. El patrón encaja con las franjas de partidos. Foros de Movistar documentan bloqueos por IP de 188.114.96.0/20 atribuidos a LaLiga (comunidad.movistar.es). Fuentes candidatas tras Cloudflare con otras IPs (hoy OK): Deribit, Kraken spot y futuros, OKX, Coinbase, ForexFactory, DefiLlama.

**Impacto:** Huecos de datos no recuperables en franjas de partido (htx ya los tiene); cualquier fuente nueva puede caer igual.

**Recomendación:** 1) Comprobar el certificado de cada fuente y alarmar si falla. 2) Marcar estas franjas en _gaps.json. 3) Redundar cada dato: liquidaciones de 3 venues, USDT/USD de 3 fuentes. 4) Valorar un túnel o VPN solo para el recorder (decisión del operador). 5) En htx, evitar que ccxt toque el dominio de derivados api.hbdm.vn (área de otro agente).

**Riesgo de pérdida de datos:** Sí: huecos en las franjas de partido.

**Esfuerzo:** S-M

#### [INFO] F13 — Proxy de cabalgada con el libro del perp de Binance: sin ventaja por sí solo; con filtros, pista exploratoria

**Evidencia:** Datos: bookDepth ±0,2 % y ±1 %, enero-octubre 2026. Reglas: entrada cuando |desequilibrio| supera su cuantil 95; salida al cruzar 0 o la mitad del umbral contrario; 10 pb de coste. Sin filtro: de −9,3 a −9,8 pb netos por operación (349-1.805 operaciones). Con filtros: sesión EE.UU. en ±1 %, +10,3 pb bruto (t 2,55, n 211) y +19,8 (t 1,95, n 157); DVOL alta en ±1 %, +13,7 (t 2,29, n 122) y +52,0 (t 3,68, n 80, ~4 h de tenencia mediana); volatilidad baja negativa en 4/4. Se probaron 36 combinaciones: riesgo alto de sobreajuste. Fichero: results/ride_proxy.json.

**Impacto:** Apoya usar la sesión EE.UU. y el régimen de volatilidad como filtros, y descarta el libro del perp como piloto en solitario.

**Recomendación:** Usar bookDepth (gratis) para fijar los filtros (sesión EE.UU. + régimen de volatilidad) y validarlos fuera de muestra sin esperar semanas de lake. Después, repetirlo sobre la presión multi-venue con el peg corregido (F01).

**Riesgo de pérdida de datos:** Ninguno.

**Esfuerzo:** M

#### [INFO] F14 — Reloj del PC y latencias de las fuentes

**Evidencia:** Desfase del PC frente a 5 servidores (Binance, Bybit, OKX, Deribit, Coinbase): el PC va 255-297 ms por detrás (mediana 261 ms). Latencia p50 evento→recepción, corregida por ese desfase: Kraken F trades 34 ms, Deribit índice 37 / DVOL 58, Coinbase 66, Bybit 88, OKX 135, Binance 136, Upbit 143, Hyperliquid 361. RTT REST: Kraken 68-115 ms, Deribit 60-120, Coinbase 116-130, Bybit 180-230, Binance fapi 250-340, OKX 255-340, Hyperliquid 480-510, Upbit/Bithumb 660-670.

**Impacto:** Irrelevante para 5-60 min. Sí importa para alinear fuentes con hora de evento frente al lake, que usa hora de recepción.

**Recomendación:** Grabar siempre la hora de evento del exchange y la de recepción. Activar la sincronización horaria de Windows (decisión del operador).

**Riesgo de pérdida de datos:** Ninguno.

**Esfuerzo:** S

### Medidas

```text
A) MÉTODO Y MUESTRAS
- Precio de referencia: perp BTCUSDT de Binance, 1 min (data.binance.vision), 2025-10-17→2026-10-16 = 525.600 min. Para el calendario, 2024-10-01→2026-10-16.
- Estadísticos: IC de Spearman en muestras no solapadas (una cada h min); t parcial = OLS HC1 con ret_5/15/60 como controles; diferencial decil superior−inferior sobre todas las observaciones. Coste de referencia: 10 pb ida y vuelta.

B) DIRECCIÓN. Formato: h=15 min: IC (t) [t parcial] decil pb | h=60 min: IC (t) [t parcial] decil pb | meses con el mismo signo 15/60
- ret_15: −0,047 (−8,9) decil −0,3 | −0,021 (−1,9) −1,3 | 0,92/0,77
- ret_60: −0,038 (−7,2) | −0,035 (−3,2) −1,7
- ti_perp_15: −0,037 (−6,9) [−1,05] −1,3 | −0,018 (−1,6) [−0,14] −2,2 | 1,0/0,77
- ti_spot_15: −0,042 (−7,8) [−2,31] | −0,024 (−2,2) [−0,94]
- cvdz_spot_15: −0,044 (−8,3) [−1,63] −2,1 | −0,024 (−2,2) [−0,36] −2,9
- doi_15: −0,015 (−2,8) [−1,14] | −0,033 (−3,1) [−1,37]
- dprem_5: +0,008 (1,4) | +0,025 (2,3) [0,87]
- prem_z, basis_ps_z, funding_bp, lsr_top_z: |IC| ≤ 0,016, |t| ≤ 1,5
- dlsr_acc_60: [−2,41] a 15 min; decil −5,2 a 60 min (IC +0,014: no monótono)
- taker_ratio_5m: −0,018 (−3,5) [−0,49]
- bd_imb_02: +0,018 (2,9) [−0,08] | +0,003 (0,3)
- bd_imb_1: +0,039 (7,3) [−0,23] | +0,045 (4,2) [−0,15] | 0,85/0,85
- bd_imb_5: +0,025 (4,7) [1,47] | +0,040 (3,8) [1,37] decil +2,0
- Kraken F (12 meses): kf_ti_15 −0,026 (−4,9) [−0,48]; kf_basis [−3,4] a 5 min, decil −5,3 a 60 min; kf_liq_x_ret_15 −0,030 (−5,7) [−0,48]
- Coinbase (6 meses), sin retraso: cbprem_z +0,020 (4,6) [5,4] a 5 min; +0,037 (2,4) [3,4] a 60. Con 1 min de retraso: +0,005 (1,2) / +0,009 (0,6); decil 3,5 pb a 60; meses positivos 5/7 y 4/7.
- Prima coreana (3 meses): |IC| ≤ 0,017, |t| ≤ 2,1; media 0,0 pb, sd 6,3.
- USDT/USD → BTC: |t| < 2.
- Yahoo (~27k min): correlación simultánea ES 0,394, NQ 0,410, DXY −0,188, VIX −0,254, IBIT 0,697; adelanto a 1 min ≤ 0,023; IC a 5-60 min |t| ≤ 2,7.
- Momentum de 15 min (seguir el signo): −0,44 pb a 15 min (t −3,5); −0,06 a 30; +0,43 a 60 (t 0,9). Laborables +0,88 a 60 min frente a −0,71 en fin de semana. Movimiento con OI a la baja: +2,26 a 60 min (t 2,08).

C) VOLATILIDAD FUTURA. IC 15 | 60 min [t parcial sobre rv_60]
- rv_60: 0,781 | 0,794
- kf_liq_15: 0,313 [20,1] | 0,278 [4,9]
- dvol: 0,386 [13,9] | 0,422 [10,7]
- vol_rel_15: 0,408 [11,9] | 0,371 [−0,7]
- |ΔOI_15|: 0,375 [11,0] | 0,364 [3,3]
- |ΔDVOL_15|: 0,328 [9,8] | 0,321 [3,7]
- |Δprima_15|: 0,268 [9,5] | 0,270 [5,2]
- avgsize_z: 0,088 [8,2] | 0,036 [0,2]
- bd_depth02_z: −0,061 [4,0] | −0,056 [3,2]
- |Δprima Coinbase|: 0,079 [3,5]
- |Δkimchi|: 0,053 [0,9]

D) CALENDARIO (24 meses)
- CPI (n=24): volatilidad 0-15 min ×3,24; 0-60 ×2,02; hora previa ×1,06; |ret 0-15| mediana 48,7 pb (media 59). El movimiento de 5 min sigue en 5→60 min el 62 % de las veces: +6,4 pb (t 0,6).
- NFP (n=24): ×2,37 / ×1,70; 33,0 pb; 33 %, −23,2 pb (t −1,6).
- FOMC (n=16): ×2,80 / ×2,24; 25,5 pb; 38 %, −19,9 pb (t −0,9).
- Volatilidad por minuto en laborables: máxima 15 UTC 10,87 pb, mínima 10 UTC 4,95 (×2,2). Fin de semana/laborable: 0,68.
- Autocorrelación de 15 min: laborables entre −0,005 y −0,035; fin de semana EE.UU. −0,089; fin de semana 21-24 UTC −0,117.
- Viernes 08:00 UTC: ×0,99; último viernes ×1,14 (n=12).
- Funding: ±30 min alrededor de la liquidación, |t| ≤ 1,13 (n=2.189).

E) LIQUIDACIONES (Kraken analytics, 12 meses)
- Minutos con liquidación: 3,8 %; media 29/día; p99 por minuto 0,069.
- Evento top 0,5 % (umbral 37,6 en 5 min; n=46): continuidad del movimiento −16,3 pb a 5 min (t −1,68), −21,3 a 15 (t −1,71), −13,4 a 30, −4,4 a 60.
- Movimiento absoluto respecto al normal: ×4,7 / ×4,0 / ×3,5 / ×2,9.
- Control sin liquidación: −0,56 (15 min) / −6,2 (60 min).

F) CABALGADA PROXY (bookDepth del perp, ene-oct 2026, 10 pb)
- ±0,2 %, cuantil 95, salida a 0: 1.805 operaciones, bruto +0,66, neto −9,34 pb. Por filtro: finde +2,31; EE.UU. +1,56; volatilidad alta +1,76 (t 2,07); volatilidad baja −1,94.
- ±0,2 %, salida a la mitad: 1.103 operaciones, +1,38 pb. Finde +7,99 (t 2,27); EE.UU. +3,04; volatilidad baja −1,07.
- ±1 %, salida a 0: 508 operaciones, +0,28 pb. EE.UU. +10,28 (t 2,55; n 211); DVOL alta +13,65 (t 2,29; n 122); volatilidad baja −9,32.
- ±1 %, salida a la mitad: 349 operaciones, +0,21 pb. EE.UU. +19,76 (t 1,95); DVOL alta +51,96 (t 3,68; n 80; ~4 h); volatilidad baja −21,94.
- Los cuantiles 99/99,5 salen degenerados (bandas vacías). Se probaron 36 combinaciones.

G) SESGO DEL PEG USDT (lake M17, paso 5 s, 12 venues). Formato: ventana | USDT/USD pb | base medida pb | desequilibrio A / B / C | % pujas USD A/B | signo A=B | cruces <−0,3 A/B
- 10-07 20h | −5,1 | 5,58 | −0,148 / −0,061 / −0,040 | 14,1/28,4 | 91,5 % | 1/0
- 10-09 15h | −9,0 | 8,57 | −0,100 / +0,001 / −0,011 | 5,0/27,8 | 51,9 % | 0/0
- 10-11 18h | −10,7 | 9,67 | −0,170 / −0,015 / −0,022 | 1,1/30,8 | 64,2 % | 5/3
- 10-16 12h | −13,2 | 12,56 | −0,175 / −0,004 / −0,010 | 0,7/30,4 | 53,5 % | 5/0
- 10-17 06h | −14,1 | 13,89 | −0,147 / +0,023 / +0,011 | 0,5/31,5 | 26,8 % | 0/0
- Ajuste: sesgo A−B = −0,0111·pb − 0,025 (r −0,92). Desviación típica de A: 0,041-0,053. Cada hora tarda 103-251 s de replay (0,55-1,03 M actualizaciones).
- USDT/USD en Coinbase (6 meses): media −6,6 pb, mediana −7,2, p25 −10,4, p75 −2,5, mínimo −118. USDC/USD en Kraken (30 días): media −1,2 pb.

H) SONDAS EN VIVO (sábado 2026-10-17, ~14:30-14:40 UTC)
REST: 47 de 54 dieron 200.
- RTT por fuente: Binance fapi (premiumIndex, OI, fundingRate, OI/ratios de 5 min, basis) 250-340 ms; Bybit 180-230; OKX (incluidas rubik y liquidation-orders) 255-340; Kraken F (ticker, histórico de funding, analíticas de 1 min) 68-115; Deribit 60-120; Coinbase 116-130; Kraken spot (USDT/USD, USDC/USD, EUR/USD) 94; Bitstamp (usdtusd, eurusd) 26-111; Upbit 659; Bithumb 668; Hyperliquid 478-513; ForexFactory 44; Yahoo 173-255; DefiLlama 72; mempool 103; CoinDesk RSS 198.
- Fallan: Dunamu (DNS); frankfurter, farside y cryptopanic (certificado autofirmado del ISP); Whale Alert 404; Coinglass 401 sin clave.

WS (240 s cada una). Formato: msg/s · MB/día en crudo (ritmo de sábado) · latencia p50
- Binance UM /market (aggTrade + forceOrder + !forceOrder@arr + markPrice@1s): 5,23 · 94,7 · aggTrade 136 ms, markPrice 175
- Binance spot aggTrade: 3,26 · 57,6 · 137 ms
- Bybit linear (trades + tickers + allLiquidation): 9,40 · 208 · 88 ms
- OKX (liquidaciones + funding + OI + mark + índice + trades): 10,94 · 179 · trades 135 ms
- Deribit (DVOL + ticker del perp + trades de opciones + índice + markprice de opciones): 4,78 · 328 · DVOL 58 ms, índice 37
- Kraken F (ticker + trade): 2,78 · 135 · trade 34 ms
- Coinbase market_trades (BTC-USD + USDT-USD): 3,66 · 99 · 66 ms
- Upbit: 0,53 · 21 · 143 ms
- Hyperliquid (trades + activeAssetCtx): 1,66 · 56 · 361 ms

Trades contados en 4 min: Binance UM 965, spot 782, Bybit 1.311, OKX 736, Coinbase BTC 875 y USDT 157, Kraken F 170, Hyperliquid 279, Upbit 127. Filas de markprice de opciones en Deribit: 4.878.

Actividad: el sábado a las 14 UTC es 0,48 de la media (laborable ×2,5); el día máximo es ×3,1 la media.

I) COSTE DE GRABAR (parquet zstd en ficheros grandes)
- Medido: aggTrades Binance UM, 8,3 MB/día (1,82 M filas, día activo); spot, 6,3 MB/día.
- Estimado:
  - Bybit ~5 MB; OKX ~3; Coinbase ~3; Kraken F ~0,7; Hyperliquid ~1; Upbit ~0,5 MB/día.
  - Liquidaciones de 3 venues <1 MB.
  - Tickers a 1 Hz ~1-3 MB cada uno.
  - Opciones de Deribit 5 MB (snapshot cada 5 min) a 25 MB (WS).
- Paquete imprescindible ~30-40 MB/día; todo junto ~60-70 MB/día (≈2 % de 3,16 GB/día). En JSON crudo serían ~1,2 GB/día.
- Con el vaciado actual cada 5 s, cada flujo añadiría ~17.000 ficheros/día.

J) LOG DEL RECORDER
- 986 'Cannot connect to host api.hbdm.vn': 511 el 10-10 (17:47-23:57), 456 el 10-11 (14:35-18:52) y 20 el 10-17 (desde las 16:21), en hora local.
- Fallos residuales en otros hosts: 4-7 por venue.
```

### Preguntas abiertas

- ¿Cambian de número o de signo los 17 trades e0.3/x−0.2 de RESEARCH_FINDINGS con el peg corregido? Hay que re-ejecutar _pressure_pilot sobre las 5 ventanas con USDT/USD histórico de 1 min (las velas de Coinbase USDT-USD cubren 2026-04→10). Por el ajuste, el sesgo en aquellas ventanas (USDT entre −2 y −5 pb) sería de −0,05 a −0,08.
- Unidades de liquidation-volume en las analíticas de Kraken Futures: no están documentadas (probablemente BTC). Además, liquidity y slippage repiten valores durante varios minutos (refresco cada pocos minutos), así que no son instantáneas de 1 min.
- ¿Tiene tope por segundo el canal liquidation-orders de OKX? ¿Cuánto infra-cuenta Binance desde 2026-04-10 (solo la mayor liquidación cada 1000 ms)? Se puede responder comparando con Bybit allLiquidation tras 2-3 semanas grabadas.
- ¿Coincide el patrón de bloqueo exactamente con el calendario de partidos? ¿Ha afectado alguna vez a fuentes tras Cloudflare con otras IPs (Kraken, OKX, Coinbase, Deribit)? Hay que cruzar todas las reconexiones del log con los horarios de partido.
- El resultado de la cabalgada proxy con DVOL alta (+52 pb bruto, t 3,68, n 80, ~4 h de tenencia) puede ser sobreajuste tras 36 combinaciones; necesita validación fuera de muestra antes de usarlo.
- ¿Se comportan igual estos condicionantes en ETH/SOL? La reversión a 5-60 min y el peso del régimen de volatilidad podrían ser distintos; depende de los pares que elija otro agente.
- PIDs: el brief dice 17652/17600, pero ahora corren 17592 (python -m pavilos) y 17536 (scripts.run_recorder). _runs.jsonl registra un arranque el 2026-10-16 06:46:45Z con pid 17592.
- Tree of Alpha: conecta pero no dio mensajes en 40 s; no verificado si exige autenticación para el flujo de noticias.
- Los mismos 10 pb de coste se aplican a todo. Con órdenes maker (0,02 %) o con un venue de ejecución distinto (área de otro agente), los umbrales económicos de F02 cambiarían.

