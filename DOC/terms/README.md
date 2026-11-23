# Словарь терминов

Технические термины, которые встречаются в [README](../../README.md) и коде
шлюза. У каждого термина: короткое определение, как именно он применён в
этом проекте и где лежит код.

| Файл | Тема | Термины |
|---|---|---|
| [candles.md](candles.md) | Свечи и эпохи | [свеча](candles.md#свеча), [таймфрейм](candles.md#таймфрейм), [закрытая и формирующаяся свеча](candles.md#закрытая-и-формирующаяся-свеча), [эпоха обновления](candles.md#эпоха-обновления), [грейс публикации](candles.md#грейс-публикации), [покрытие интервала](candles.md#покрытие-интервала), [серия](candles.md#серия) |
| [cache.md](cache.md) | Кеш и свежесть | [singleflight](cache.md#singleflight), [stale-while-revalidate](cache.md#stale-while-revalidate), [реплика-кеш](cache.md#реплика-кеш), [свежесть](cache.md#свежесть), [прогрев](cache.md#прогрев), [ретеншн](cache.md#ретеншн) |
| [load.md](load.md) | Нагрузка и допуск | [rate limit](load.md#rate-limit), [бюджет](load.md#бюджет), [скользящее окно](load.md#скользящее-окно), [приоритеты](load.md#приоритеты), [квота клиента](load.md#квота-клиента), [backoff](load.md#backoff), [джиттер](load.md#джиттер), [drain](load.md#drain), [батч и fan-out](load.md#батч-и-fan-out), [capacity plan](load.md#capacity-plan) |
| [cluster.md](cluster.md) | Кластер и отказоустойчивость | [weighted rendezvous hashing](cluster.md#weighted-rendezvous-hashing), [primary и standby](cluster.md#primary-и-standby), [peer-хоп](cluster.md#peer-хоп), [failover](cluster.md#failover), [split-brain](cluster.md#split-brain), [аренда](cluster.md#аренда), [fencing-токен](cluster.md#fencing-токен), [кворум и свидетели](cluster.md#кворум-и-свидетели) |
| [reliability.md](reliability.md) | Инженерные принципы | [fail-closed](reliability.md#fail-closed), [честная деградация](reliability.md#честная-деградация), [инвариант](reliability.md#инвариант), [типизированная ошибка](reliability.md#типизированная-ошибка), [golden-тест](reliability.md#golden-тест), [smoke-тест](reliability.md#smoke-тест), [миграции только вперёд](reliability.md#миграции-только-вперёд), [атомарный rename](reliability.md#атомарный-rename), [CGO-free](reliability.md#cgo-free) |
| [protocols.md](protocols.md) | Протоколы и интеграции | [MCP](protocols.md#mcp), [stdio](protocols.md#stdio), [Streamable HTTP](protocols.md#streamable-http), [bearer-токен](protocols.md#bearer-токен), [gRPC и protobuf](protocols.md#grpc-и-protobuf), [Prometheus-метрики](protocols.md#prometheus-метрики), [loopback и приватный адрес](protocols.md#loopback-и-приватный-адрес) |
| [market.md](market.md) | Рынок и индикаторы | [OHLCV](market.md#ohlcv), [фьючерс](market.md#фьючерс), [физический контракт](market.md#физический-контракт), [алиас](market.md#алиас), [экспирация](market.md#экспирация), [ролл](market.md#ролл), [пункты и лоты](market.md#пункты-и-лоты), [ATR](market.md#atr), [ADX](market.md#adx), [EMA](market.md#ema), [realized volatility](market.md#realized-volatility), [стакан L2](market.md#стакан-l2) |
