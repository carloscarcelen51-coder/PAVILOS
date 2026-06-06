# PAVILOS — Diseño Fase 1

**Fecha:** 2026-06-06
**Estado:** Diseño aprobado conceptualmente; pendiente de revisión del documento.
**Repo:** `PAVILOS/` (local) → `github.com/carloscarcelen51-coder/PAVILOS` (privado, `main`).
**Runtime:** distro WSL nativa en `D:\` (Docker Engine nativo, mismo patrón que el resto del ecosistema), aislado y hermético respecto a otros proyectos.

---

## 1. Resumen y objetivo

PAVILOS es un servicio en Python que:

1. **Agrega order books de BTC en tiempo real** del máximo de exchanges posible (WS nativo en los de liquidez profunda + ccxt para la cola larga, con proxy como contingencia).
2. **Detecta soportes y resistencias combinados** (muros de liquidez + clústeres de profundidad + anti-spoofing) sobre un book combinado normalizado a USD.
3. **Genera señales** de posicionamiento *encima del soporte* (LONG) o *debajo de la resistencia* (SHORT), espera al fill, y **gestiona la posición** con trailing stop anclado al soporte vigente + suelo ATR, cerrando cuando el soporte se consume/retira o aparece un muro opuesto dominante.
4. **Opera en modo paper** (simulación de fills contra datos en vivo) modelando el contrato **Kraken Futures perp `PF_XBTUSD`** (fees + funding), midiendo PnL/ROI/equity.
5. **Alerta por Telegram** y **visualiza** todo en un **dashboard web** en tiempo real.

La **ejecución real en Kraken Futures queda fuera de la Fase 1**, pero el sistema se diseña detrás de una interfaz `Broker` para que la Fase 2 sea un swap de implementación sin tocar la estrategia.

### Filosofía de calidad

Calidad por encima de velocidad: TDD en toda la lógica de negocio, validación de integridad de cada feed, y degradación elegante ante caídas parciales. Una señal falsa por un book corrupto o un soporte-fantasma es peor que no operar.

---

## 2. Alcance

### Dentro de la Fase 1
- Conectores WS nativos: Binance, Coinbase, Kraken, OKX, Bybit, Bitstamp.
- Conectores ccxt (REST + `watchOrderBook` gratuito) para la cola larga configurable.
- Agregador con normalización de quote (USD/USDT/USDC → USD; KRW/JPY/EUR en capa de contexto).
- Detector de soportes/resistencias con anti-spoofing y score de confianza.
- Motor de señales + gestión de posición (entrada, cancelación pre-fill, trailing, salida).
- `PaperBroker` modelando `PF_XBTUSD` (fees, funding, PnL, equity, log de trades).
- Alertas Telegram.
- API FastAPI (REST + WebSocket) + dashboard web.
- Persistencia: Postgres (estado/trades/config) + parquet/DuckDB (snapshots crudos para backtest futuro).

### Fuera de la Fase 1 (diseñado-para, no implementado)
- Ejecución real con órdenes reales en Kraken Futures (`KrakenFuturesBroker`).
- Backtesting formal sobre snapshots históricos.
- Modelo ML de calidad de soporte (cuando se haga, entrenará en la RTX 3060 según preferencia de GPU del entorno).
- Multi-símbolo (ETH, etc.) y multi-instrumento.
- Migración a arquitectura de microservicios con broker de mensajes.

---

## 3. Decisiones cerradas (con el usuario)

| Decisión | Elección |
|---|---|
| Ejecución Fase 1 | **Alertas + paper trading** (sin dinero real) |
| Lenguaje/stack | **Python** (asyncio + FastAPI), dashboard web |
| Cobertura exchanges | **Híbrido**: WS nativo top-liquidez + ccxt cola larga, proxy contingente |
| Interfaz | **Dashboard web + Telegram** |
| Instrumento modelado | **Kraken Futures perp `PF_XBTUSD`** (lineal USD; long+short) |
| Definición de soporte | **Combo + anti-spoofing** (muros + clústeres + filtro de retirada) |
| Horizonte | **Intradía configurable** (por defecto scalping/intradía) |
| Trailing stop | **Anclado al soporte + suelo ATR** |

---

## 4. Arquitectura

**Enfoque A — monolito asíncrono** (un proceso Python, asyncio, bus en memoria), con componentes modulares y fronteras limpias para poder extraerlos a servicios si algún día se va a multi-símbolo.

### 4.1 Flujo de datos

```
┌─────────────────────────────────────────────────────────────────┐
│  CONNECTORS (asyncio tasks, uno por exchange)                     │
│   WS nativos: binance, coinbase, kraken, okx, bybit, bitstamp     │
│   ccxt:       bitfinex, kucoin, htx, cryptocom, mexc, ... (cola)  │
│   cada uno → BookState local (L2) validado por secuencia/checksum │
└───────────────────────────────┬───────────────────────────────────┘
                                 │ BookUpdate normalizado (async queue)
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│  AGGREGATOR                                                       │
│   - mantiene L2 por exchange                                      │
│   - normaliza quote a USD (peg USDT/USDC live; FX KRW/JPY/EUR)    │
│   - agrega por niveles/bins de precio (Tier A core + Tier B ctx)  │
│   - emite CombinedDepthSnapshot a ~5–10 Hz                        │
└───────────────────────────────┬───────────────────────────────────┘
                                 │ CombinedDepthSnapshot
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│  DETECTOR                                                         │
│   - walls (z-score vs profundidad local)                          │
│   - clusters (zonas contiguas pesadas)                            │
│   - lifecycle + anti-spoofing (persistencia, consumido vs pulled) │
│   → SupportZone[] / ResistanceZone[] con {nivel, fuerza, conf}    │
└───────────────────────────────┬───────────────────────────────────┘
                                 │ zonas + confianza
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│  SIGNAL ENGINE (estrategia)                                       │
│   setup → entrada pendiente → (cancel si soporte cae) → fill      │
│   → trailing anclado a soporte + suelo ATR → salida               │
└───────────────────────────────┬───────────────────────────────────┘
                                 │ órdenes / eventos
                                 ▼
┌──────────────────┐   ┌──────────────────┐   ┌────────────────────┐
│  PAPER BROKER    │   │  ALERTS          │   │  STORAGE           │
│  fills, fees,    │   │  Telegram        │   │  Postgres + parquet│
│  funding, PnL    │   │                  │   │                    │
└──────────────────┘   └──────────────────┘   └────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│  API (FastAPI: REST + WS)  →  DASHBOARD web (heatmap, zonas,      │
│                                posición, equity, trade log)        │
└─────────────────────────────────────────────────────────────────┘
```

### 4.2 Bus interno

`asyncio.Queue` (o un pub/sub ligero en memoria) entre etapas. Cada etapa es una corrutina que consume de su cola de entrada y publica en la(s) de salida. Sin dependencia de Redis/Kafka en Fase 1. Contrapresión: colas acotadas; si el detector va lento, el agregador descarta snapshots intermedios (nos interesa el último estado, no el histórico, en el hot-path — el histórico va a `storage` por separado).

---

## 5. Diseño por componente

### 5.1 `connectors/` — un conector por exchange

**Responsabilidad única:** mantener un `BookState` L2 correcto y normalizado para un exchange, y publicar `BookUpdate` al bus. Aislar todas las idiosincrasias del exchange aquí.

**Interfaz común:**
```python
class Connector(Protocol):
    exchange: str
    async def run(self, out: Queue[BookUpdate]) -> None: ...   # loop con reconexión
    def book(self) -> BookState: ...                            # snapshot actual
    def health(self) -> ConnectorHealth: ...                    # last_update_ts, staleness, errors
```

**Dos familias de mantenimiento de book** (confirmado por investigación, ver Apéndice A):

- **Full-snapshot-WS + deltas** → Coinbase (`level2`), Kraken (`book`), OKX (`books`), Bybit (`orderbook.200`). El primer mensaje es snapshot completo; los updates traen **cantidad absoluta** por nivel (`qty=0` elimina). Si llega un nuevo snapshot mid-stream (Bybit), reconstruir.
- **REST-seed + WS-diff** → Binance (`@depth@100ms` + REST `/api/v3/depth?limit=5000`), Bitstamp (`diff_order_book_*` + REST `/api/v2/order_book/` reconciliado por microtimestamp). NO hay snapshot inicial por WS.

**Validación de integridad (crítico):**
- **Kraken**: CRC32 sobre top-10 niveles (verificación periódica).
- **OKX**: CRC32 sobre top-25 — **se deprecia 2026-06-23 (pasa a 0)** → usar **`seqId`/`prevSeqId`** como mecanismo primario; checksum solo si presente y ≠ 0.
- **Resto (Binance, Coinbase, Bybit, Bitstamp)**: **continuidad de secuencia** (`U`/`u` en Binance, `sequence_num` en Coinbase, `u`/`seq` en Bybit, microtimestamp en Bitstamp). Cualquier gap → **rebuild** del book (re-seed REST o re-suscripción).

**Reconexión:** backoff exponencial (1s, 2s, 4s, … cap 30s) con jitter; al reconectar, re-seed. Heartbeats donde aplique (Coinbase `heartbeats`, Kraken/Bybit/OKX ping cada <60s).

**Proxy:** cada conector acepta un proxy opcional desde config (`connectors.<name>.proxy`). Por defecto directo (host residencial español). Contingencia documentada para Bybit (CloudFront) y Binance (GCP/AWS) si el egress fuera de datacenter.

**ccxt connectors:** envoltura única sobre `ccxt.pro` `watchOrderBook` (gratuito) para venues de cola larga; fallback a `fetch_order_book` REST con throttle por instancia (`enableRateLimit=True`) y backoff ante `RateLimitExceeded`/`DDoSProtection`. Misma interfaz `Connector`.

### 5.2 `aggregator/` — book combinado normalizado

**Responsabilidad única:** fusionar los L2 por exchange en un mapa de profundidad combinado, comparable en precio.

**Normalización de quote (clave para evitar soportes-fantasma):**
- **Tier A — core, comparable en precio**: venues con quote **USD / USDT / USDC**. USDT/USDC → USD con **peg en vivo** (rate desde un mercado USDT/USD, p. ej. Kraken `USDT/USD` o Coinbase). Para BTC≈100k, un depeg de 0.1% mueve niveles ~$100, así que el peg se trackea, no se asume 1.0.
- **Tier B — contexto/breadth**: venues con quote **KRW / JPY / EUR** (Upbit, Bithumb, bitFlyer, etc.). Se convierten con FX en vivo pero **NO se mezclan por defecto** en el mapa de niveles del Tier A: el "kimchi premium" desplaza el precio varios %, y mezclarlo crearía soportes en niveles equivocados. Se usan como **señal de divergencia/sentimiento** y se muestran aparte. Configurable subirlos a Tier A si el usuario lo quiere.

**Binning:** niveles de precio agrupados en bins configurables (por defecto en **bps relativos al mid**, p. ej. 1 bp ≈ $10 a 100k, o tamaño fijo en $). Por bin se suma `size` (en BTC) entre exchanges del Tier A y se guarda la **composición por venue** (qué exchange aporta cuánto) — necesario para anti-spoofing y para el dashboard.

**Salida:** `CombinedDepthSnapshot { ts, mid, bins_bid[], bins_ask[], per_venue_contribution, tierB_context }` a cadencia fija (5–10 Hz) o por cambio significativo.

**Degradación elegante:** si un feed está stale (sin update > N s) o en rebuild, se **excluye** del agregado y se marca; el snapshot lleva `venues_active` / `venues_total`. El detector no actúa si la cobertura cae por debajo de un mínimo configurable.

### 5.3 `detection/` — soportes y resistencias

**Responsabilidad única:** a partir de `CombinedDepthSnapshot`, producir zonas rankeadas con score de confianza, manteniendo su ciclo de vida.

- **Muros (walls):** bins donde el `size` agregado ≫ mediana/percentil de la profundidad local (z-score o múltiplo configurable sobre ventana de bins vecinos). Filtra picos aislados de un solo venue salvo que sean enormes.
- **Clústeres:** zonas contiguas de bins pesados (agrupación de niveles adyacentes con densidad alta) → un "muro ancho" cuenta como soporte de zona.
- **Ciclo de vida + anti-spoofing:** cada zona se trackea en el tiempo:
  - `appeared_at`, `persistence` (cuánto lleva viva), `growing/shrinking`.
  - Distinción **consumido vs retirado**: si el precio toca la zona y el size baja **junto con trades** (prints en esa zona) → consumido (real). Si el size desaparece **sin trades** (cancelaciones) → *pulled* / spoof → penaliza confianza fuerte.
  - **Confianza** = f(persistencia, nº de venues que aportan, tamaño relativo, ausencia de comportamiento spoof, distancia al mid). Una zona necesita persistir ≥ `min_persistence_s` y aporte de ≥ `min_venues` para ser "operable".

**Salida:** `SupportZone[]` (debajo del mid) y `ResistanceZone[]` (encima) con `{price, low, high, strength, confidence, persistence_s, venues, composition}`.

### 5.4 `signals/` — estrategia y gestión de posición

**Responsabilidad única:** convertir zonas en setups, gestionar entradas pendientes y posiciones abiertas vía la interfaz `Broker`. Es una **máquina de estados explícita** (testeable de forma determinista).

**Estados:** `IDLE → SETUP → PENDING_ENTRY → IN_POSITION → EXITING → IDLE` (+ `CANCELLED`).

- **SETUP / entrada (lo que describe el usuario):** cuando hay una `SupportZone` operable cerca del precio → setup **LONG**: entrada *justo encima* del soporte, stop inicial *justo debajo*. Simétrico con `ResistanceZone` → **SHORT**. La zona es la **tesis** de la operación (se guarda referencia a su id).
- **PENDING_ENTRY (esperar al fill):** entrada como orden en reposo (paper) en el nivel. Se "ejecuta" cuando el precio negocia a través de él (simulado contra trades/book de Kraken).
- **Cancelación pre-fill ("si el soporte se va sin ejecutarse, retírate"):** si la zona-tesis pierde confianza / se retira (pulled) **antes** de que la entrada se llene → **cancelar** la entrada pendiente y volver a `IDLE`.
- **IN_POSITION + trailing ("subir el stop mientras los order books lo digan"):** stop anclado **justo por debajo de la `SupportZone` vigente** (para long); cuando aparece un soporte **más alto** y consolidado, el stop **sube** con él. **Suelo ATR**: el stop nunca se coloca más lejos que `k·ATR` del precio (red de seguridad anti-stops absurdos en books finos). Para short, simétrico con resistencias por encima.
- **Salida ("cerrar si detecta lo contrario"):** cierra si (a) la zona-tesis se **consume/retira**, o (b) aparece un **muro opuesto dominante** (resistencia fuerte por encima que frena el long / soporte fuerte por debajo que frena el short), o (c) salta el stop (trailing o suelo ATR).

**Parámetros configurables:** umbrales de confianza para entrar, `min_persistence_s`, `k` del ATR, distancia de entrada/stop al borde de zona, horizonte, sizing.

**Sizing/riesgo (paper):** riesgo fijo por trade (% del equity al stop inicial), respetando el cap de apalancamiento EEA (~10x) modelado. Guardas de cordura: tamaño máximo, exposición máxima, un solo trade BTC a la vez en Fase 1.

### 5.5 `execution/` — Broker (paper ahora, real después)

**Interfaz única `Broker`** que la estrategia consume:
```python
class Broker(Protocol):
    async def place(self, order: Order) -> OrderId: ...
    async def cancel(self, oid: OrderId) -> None: ...
    async def modify_stop(self, oid: OrderId, new_stop: float) -> None: ...
    def position(self) -> Position | None: ...
    def equity(self) -> float: ...
```

**Fase 1 — `PaperBroker`:**
- Simula colocación y **fills contra datos en vivo**: por defecto la entrada se llena cuando los **trade prints de Kraken `PF_XBTUSD`** (el venue donde realmente operaríamos) cruzan el nivel, usando el **mid combinado** solo como referencia de cordura/divergencia; modela slippage simple y, opcionalmente, requisito de que haya liquidez suficiente.
- **Fees**: maker 0.02% / taker 0.05% del notional (entrada/salida según tipo de orden).
- **Funding**: cargo/abono **horario** sobre el notional según `funding_rate` (modelado; en paper se puede aproximar con el funding real publicado por Kraken vía API pública, o un proxy del premium). Positivo = longs pagan shorts.
- Mantiene `Position`, PnL realizado/no realizado, **equity curve**, y **log de trades** persistido.

**Fase 2 — `KrakenFuturesBroker`** (no en Fase 1): misma interfaz, órdenes reales vía REST `POST /derivatives/api/v3/sendOrder` (auth HMAC-SHA-512) y fills vía WS privado `wss://futures.kraken.com/ws/v1` (challenge-signing). La estrategia no cambia.

### 5.6 `alerts/` — Telegram

Bot Telegram que emite eventos legibles: setup detectado, entrada colocada, fill, movimiento de stop, cancelación pre-fill, salida (con motivo), y resúmenes periódicos (PnL/equity). Token/chat-id por config/env. Rate-limit y deduplicación de alertas.

### 5.7 `api/` + `ui/` — dashboard

- **FastAPI**: REST para estado/históricos (`/health`, `/depth`, `/zones`, `/position`, `/trades`, `/equity`, `/config`) + **WebSocket** que empuja `CombinedDepthSnapshot` decimado, zonas y eventos al dashboard en vivo.
- **Dashboard web** (Next.js o front ligero): **heatmap de profundidad combinada** con soportes/resistencias resaltados y su composición por venue, **estado de la posición** (entrada, stop trailing, PnL), **equity curve**, **log de trades**, y **salud de feeds** (qué venues activos/stale).

### 5.8 `storage/` — persistencia

- **Postgres** (contenedor propio, volumen en ext4 de D:): config, zonas relevantes, trades, posiciones, equity, eventos de alerta. Estado consultable y duradero.
- **parquet/DuckDB** rotatorio: snapshots crudos de profundidad combinada y por-venue, para **backtesting futuro** (Fase posterior). Append por lotes, fuera del hot-path.

### 5.9 `config/` — configuración

YAML + env (sin secretos en git; `.env` ignorado, `.env.example` versionado). Incluye: lista de exchanges y su tier, mapa de proxies, bin size, umbrales de muro/confianza, `min_persistence_s`, `min_venues`, ATR `k`, sizing/riesgo, fees/funding, peg de stablecoins, credenciales Telegram, params del paper broker.

---

## 6. Manejo de errores y robustez

- **Por conector:** reconexión con backoff+jitter; detección de gaps → rebuild; staleness → exclusión del agregado + alerta; validación de checksum/secuencia.
- **Agregador:** degradación elegante (opera con subconjunto de venues); marca cobertura; nunca emite niveles de quote no normalizada.
- **Detector:** no genera señales con cobertura < mínimo, ni sobre zonas que no superen persistencia/venues mínimos.
- **Señales/Broker:** guardas de sizing y de exposición; el suelo ATR evita stops degenerados; idempotencia en colocación/cancelación.
- **Proceso:** logging estructurado; un panel de **salud de feeds** en el dashboard; el fallo de un conector no tumba el resto.

---

## 7. Estrategia de testing (TDD)

- **Unit (determinista):**
  - Reconstrucción de book para cada modelo (full-snapshot vs REST-seed+diff) con fixtures.
  - CRC32 Kraken/OKX y detección de gaps de secuencia.
  - Normalización de quote (peg/FX) y binning del agregador.
  - Detección de muros/clústeres sobre books sintéticos con respuestas esperadas.
  - **Ciclo anti-spoofing**: consumido (con trades) vs pulled (sin trades) → score correcto.
  - Máquina de estados de señales (todas las transiciones: cancel pre-fill, trailing sube, salida por muro opuesto, salida por stop).
  - PaperBroker: fills, fees, funding horario, PnL, equity.
- **Integración (replay determinista):** grabar frames WS reales de cada exchange una vez (fixtures) y reproducirlos contra el pipeline completo, verificando que el book combinado y las zonas coinciden con lo esperado.
- **Smoke en vivo (manual/scriptable):** probar conectividad real de cada endpoint desde el host antes de confiar en él (especialmente Gate.io, Upbit, Bithumb y los datacenter-blocked Bybit/Binance).

CI corre unit + integración; nada de claims de "funciona" sin tests verdes (verificación antes de completar).

---

## 8. Despliegue (distro nativa en D:)

- **Código** en `PAVILOS/` (repo, en C: por ahora) y **runtime** en la distro WSL nativa de `D:` con **Docker Engine nativo**, aislado de otros proyectos.
- **`docker-compose.yml`** con servicios:
  - `core` — el motor asíncrono (conectores + agregador + detector + señales + paper broker + alerts + FastAPI).
  - `ui` — dashboard web.
  - `postgres` — instancia propia de PAVILOS, volumen en ext4 de D:.
- Desarrollo local con venv (Python 3.13) para iterar tests rápido; el runtime "de verdad" en compose.
- **Aislamiento:** red, volúmenes y DB propios; cero solapamiento con Brujita/LAIN.
- Push diario a GitHub (privado) una vez haya código sustancial.

---

## 9. Estructura de proyecto (propuesta)

```
PAVILOS/
├── docker-compose.yml
├── pyproject.toml            # deps: aiohttp/websockets, ccxt, fastapi, uvicorn, numpy, pandas, asyncpg, pydantic, pytest
├── .env.example
├── config/
│   └── pavilos.yaml
├── src/pavilos/
│   ├── connectors/           # base.py + binance.py, coinbase.py, kraken.py, okx.py, bybit.py, bitstamp.py, ccxt_connector.py
│   ├── aggregator/           # book_state.py, normalize.py, combine.py
│   ├── detection/            # walls.py, clusters.py, lifecycle.py, confidence.py
│   ├── signals/              # state_machine.py, strategy.py, sizing.py
│   ├── execution/            # broker.py (Protocol), paper_broker.py
│   ├── alerts/               # telegram.py
│   ├── storage/              # pg.py, snapshots.py
│   ├── api/                  # app.py (FastAPI), ws.py
│   └── core/                 # bus.py, config.py, models.py, health.py, main.py
├── ui/                       # dashboard (Next.js o front ligero)
└── tests/
    ├── fixtures/             # frames WS grabados por exchange
    ├── unit/
    └── integration/
```

---

## 10. Milestones de la Fase 1 (cada uno con su propio plan al llegar)

- **M1 — Ingesta + agregación.** Conectores top-6 WS + ccxt cola larga; agregador con normalización y binning; book combinado emitido; heatmap básico en el dashboard. *Salida visible: ver la profundidad combinada en vivo.*
- **M2 — Detección.** Muros/clústeres + ciclo de vida + anti-spoofing + score de confianza; zonas visualizadas sobre el heatmap. *Salida visible: ver soportes/resistencias y su confianza.*
- **M3 — Señales + paper.** Máquina de estados de la estrategia + `PaperBroker` (fills/fees/funding) + trailing + salidas. *Salida visible: trades simulados con PnL/equity.*
- **M4 — Alertas + dashboard completo + métricas.** Telegram + dashboard completo (posición, equity, salud de feeds) + persistencia y métricas (ROI, win-rate). *Salida visible: el sistema completo operando en paper, alertando y reportando.*

---

## 11. Riesgos y cuestiones abiertas

- **Mezcla de quotes:** decisión por defecto = KRW/JPY en Tier B (contexto), no en el mapa de niveles. Confirmar si se quiere forzar su inclusión.
- **Spoofing sofisticado:** el anti-spoof basado en trades reduce falsos, no los elimina; calibrar umbrales con datos reales en M2/M3.
- **Latencia y reloj:** agregar venues con latencias dispares requiere timestamps y staleness consistentes; monitorizar.
- **Checksum OKX deprecado (2026-06-23):** primar continuidad de secuencia desde el inicio.
- **Datacenter-IP blocks:** como corremos en host residencial, riesgo bajo; tener proxy de contingencia para Bybit/Binance.
- **Realismo del paper:** los fills simulados son una aproximación; el funding/slippage modelado debe revisarse antes de pasar a Fase 2 real.
- **Fidelidad mark/funding Kraken:** usar datos públicos reales de `PF_XBTUSD` donde sea posible para no inventar el modelo.

---

## Apéndice A — Matriz de feeds WS (datos verificados, 2026-06-06)

| Exchange | WS público | Canal L2 | Modelo de book | Checksum | Profundidad | Quote | Auth |
|---|---|---|---|---|---|---|---|
| Binance | `wss://stream.binance.com:9443` | `<sym>@depth@100ms` | REST seed (`/api/v3/depth?limit=5000`) + WS diff; continuidad `U`/`u` | No (secuencia) | 5000 (seed) / full vía diffs | USDT | No |
| Coinbase (Adv. Trade) | `wss://advanced-trade-ws.coinbase.com` | `level2` (+`heartbeats`) | Full snapshot WS + updates (qty absoluta) | No (`sequence_num`) | Full | USD/USDC | No |
| Kraken (spot) | `wss://ws.kraken.com/v2` | `book` (depth 10/25/100/500/1000) | Full snapshot WS + updates (qty absoluta) | **CRC32 top-10** | hasta 1000 | USD | No |
| OKX | `wss://ws.okx.com:8443/ws/v5/public` (EEA: `wseea.okx.com`) | `books` (400, 100ms) | Full snapshot WS + deltas; `seqId`/`prevSeqId` | CRC32 top-25 **(deprecado 2026-06-23 → 0)** | 400 | USDT | No (`books`) |
| Bybit | `wss://stream.bybit.com/v5/public/spot` (EU: `api.bybit.eu`) | `orderbook.200.<sym>` | Snapshot WS + delta (qty absoluta); reset si llega snapshot | No (`u`/`seq`) | 200 (spot) | USDT | No |
| Bitstamp | `wss://ws.bitstamp.net` | `diff_order_book_<pair>` | REST seed (`/api/v2/order_book/`) + WS diff; reconciliación por microtimestamp | No (microtimestamp) | full vía diffs | USD | No |

**Cola larga vía ccxt** (`watchOrderBook` gratis desde 2022; REST `fetch_order_book` fallback, throttle por instancia): Bitfinex (USD), KuCoin (USDT), HTX (USDT), Crypto.com (USDT), MEXC (USDT), Gate.io (USDT, EEA restringido en cuenta — verificar IP), BitMart (USDT), Bitget (USDT). **Tier B contexto (FX):** Upbit (KRW), Bithumb (KRW), bitFlyer/Coincheck (JPY). Gemini (USD) en retirada de EEA.

---

## Apéndice B — Modelo Kraken Futures `PF_XBTUSD` (verificado)

- **Símbolo:** `PF_XBTUSD` — perpetuo **lineal**, margen/liquidación en **USD** (multi-collateral). *No* el inverso legacy `PI_XBTUSD`.
- **Fees:** maker **0.02%** / taker **0.05%** base (bajan por volumen 30d; rebates en tiers altos).
- **Funding:** **horario**, peer-to-peer (positivo ⇒ longs pagan shorts), rango ±0.5%/h, índice CF Bitcoin, suavizado por multiplicador de 8h (desde 2026-06-01).
- **Apalancamiento:** **España/EEA retail ~10x** (vs hasta 100x no-EEA), margen inicial ~10%.
- **Contrato:** lote mínimo 0.0001 BTC, tick 1 USD, mark = índice + EMA del premium (cap ±1%), liquidación al romper margen de mantenimiento, sin vencimiento, 24/7.
- **APIs (Fase 2):** REST `https://futures.kraken.com/derivatives/api/v3/sendOrder` (APIKey + Authent HMAC-SHA-512); WS `wss://futures.kraken.com/ws/v1` (público `book/ticker/trade`; privado `fills/open_orders/open_positions` por challenge-signing).
- **Disponibilidad España/EEA:** sí, vía Payward Europe Digital Solutions (CY) Ltd (CySEC, MiFID II + MiCA); requiere test de idoneidad + NIF.

---

## Apéndice C — Geo / proxy (verificado)

- **Los datos públicos de order book NO están geobloqueados por país** para España/EU en ninguno de los 15 venues.
- **El bloqueo real = IPs de datacenter/cloud:** Bybit (CloudFront) y Binance (rangos GCP/AWS) devuelven 403/451 a muchas IPs de VPS. Desde **IP residencial española** (host local en D:) funcionan directos.
- **Sin proxy:** Kraken, Coinbase, Bitstamp, KuCoin, Crypto.com, Bitfinex, HTX, MEXC.
- **Usar endpoint regional EU cuando enruten:** OKX (`eea.okx.com`), Bybit (`api.bybit.eu`) — el book global y el EU pueden diferir; elegir el que necesitemos.
- **Verificar empíricamente antes de depender:** Gate.io (geofencing EEA a nivel cuenta — mayor riesgo de bloqueo de IP), Upbit, Bithumb (fricción IP datacenter).
- **Nota (no es asesoría legal):** usar proxy para **datos públicos** es de menor riesgo que para evadir restricciones de **trading**; aun así, algunos ToS prohíben "enmascarar ubicación". Preferir endpoint EU + egress residencial limpio. PAVILOS solo consume datos públicos; **opera exclusivamente en Kraken Futures**.
