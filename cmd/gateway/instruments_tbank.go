package main

import (
	"github.com/jedi108/market-data-gateway/internal/model"
	tbankprovider "github.com/jedi108/market-data-gateway/internal/provider/tbank"
)

// tbankInstruments returns the canonical TBank instrument registry served by
// the gateway. Share FIGIs were read from the live TBank InstrumentsService
// (read-only probe) and represent a fixed, reviewed universe of liquid shares.
// AFR is not registered: TBank does not list it at all, and requests resolve
// as unknown symbol.
//
// F20 DoD (2026-10-02 reviewed online verification against live TBank,
// read-only InstrumentsService + GetCandles probe): the active physical
// USD-gold contract of the MOEX GOLD family was verified to serve candles
// through GetCandlesRequest.instrument_id by its instrument_uid, with prices
// in points and integer lot-denominated volume. That reviewed contract is
// registered below with the logical rolling alias "GOLD"; no synthetic
// fixture may enter this list (guard test). When the contract rolls, the next
// family member is added only through the same reviewed verification (or the
// runtime registry refresh driven by -tbank-instruments-file), never by
// aliasing an assumed contract.
//
// Issue #8 (2026-10-06, same reviewed verification recipe): the liquid
// physical contracts of the BR (Brent), Si (USD/RUB), MIX (IMOEX index) and
// NG (natural gas) families joined GOLD. Series selection was made against
// the live SPBFUT snapshot by traded volume with the driver's entry-eligibility
// horizon (min_entry_days_to_expiry=30): the dying near months BRX6/NGV6
// carry more volume but fall below that horizon, so the most liquid
// entry-eligible series are pinned (BRZ6/SIZ6/MXZ6/NGX6). Each physical uid
// was verified to serve GetCandles with prices on the tick grid.
//
// Every canonical symbol additionally accepts its "SYM/RUB" pair spelling.
// VTBR also accepts the pre-rename "VTB"/"VTB/RUB" spellings for
// compatibility.
func tbankInstruments() []tbankprovider.Instrument {
	instruments := []tbankprovider.Instrument{
		{
			// Physical GOLD-12.26 (provider ticker GDZ6, figi FUTGOLD12260,
			// class code SPBFUT): uid verified live on 2026-10-02. The
			// logical "GOLD" alias targets exactly this active contract;
			// "GDZ6" is the provider ticker spelling. Futures price domain
			// is points, candle volume is lots (contracts); the settlement
			// currency is deliberately not exposed (see model.InstrumentMetadata).
			CanonicalSymbol:      "GOLD-12.26",
			ProviderInstrumentID: "91f84d07-14a1-43a6-a6b8-648b62a41994",
			MarketType:           "futures",
			Aliases:              []string{"GOLD", "GDZ6"},
			InstrumentType:       model.InstrumentTypeFuture,
			ProviderIDKind:       model.ProviderIDKindInstrumentUID,
			PriceUnit:            model.PriceUnitPoints,
			VolumeUnit:           model.VolumeUnitLots,
			ExpirationUTCMS:      1797811200000, // 2026-12-21T00:00:00Z, provider expiration_date
		},
		{
			// Physical BR-12.26 (provider ticker BRZ6, figi FBR122600000,
			// class code SPBFUT): uid verified live on 2026-10-06. Brent
			// family; the near month BRX6 carries more volume but sits below
			// the driver's min_entry_days_to_expiry=30 horizon, so the most
			// liquid entry-eligible series is pinned here.
			CanonicalSymbol:      "BR-12.26",
			ProviderInstrumentID: "5f72f0f4-c326-46e6-a2d3-1632c74c76df",
			MarketType:           "futures",
			Aliases:              []string{"BR", "BRZ6"},
			InstrumentType:       model.InstrumentTypeFuture,
			ProviderIDKind:       model.ProviderIDKindInstrumentUID,
			PriceUnit:            model.PriceUnitPoints,
			VolumeUnit:           model.VolumeUnitLots,
			ExpirationUTCMS:      1796169600000, // 2026-12-02T00:00:00Z, provider expiration_date
		},
		{
			// Physical SI-12.26 (provider ticker SIZ6, figi FUTSI1226000,
			// class code SPBFUT): uid verified live on 2026-10-06; the
			// dominant Si series by traded volume.
			CanonicalSymbol:      "SI-12.26",
			ProviderInstrumentID: "2dd5eb6b-7a52-4186-a0ec-9f7f6ed6fbd7",
			MarketType:           "futures",
			Aliases:              []string{"SI", "SIZ6"},
			InstrumentType:       model.InstrumentTypeFuture,
			ProviderIDKind:       model.ProviderIDKindInstrumentUID,
			PriceUnit:            model.PriceUnitPoints,
			VolumeUnit:           model.VolumeUnitLots,
			ExpirationUTCMS:      1797552000000, // 2026-12-18T00:00:00Z, provider expiration_date
		},
		{
			// Physical MIX-12.26 (provider ticker MXZ6, figi FUTMIX122600,
			// class code SPBFUT): uid verified live on 2026-10-06; the
			// dominant IMOEX-index series by traded volume.
			CanonicalSymbol:      "MIX-12.26",
			ProviderInstrumentID: "3d96c0f8-9e6e-4c0a-bac8-44a5a1db8050",
			MarketType:           "futures",
			Aliases:              []string{"MIX", "MXZ6"},
			InstrumentType:       model.InstrumentTypeFuture,
			ProviderIDKind:       model.ProviderIDKindInstrumentUID,
			PriceUnit:            model.PriceUnitPoints,
			VolumeUnit:           model.VolumeUnitLots,
			ExpirationUTCMS:      1797552000000, // 2026-12-18T00:00:00Z, provider expiration_date
		},
		{
			// Physical NG-11.26 (provider ticker NGX6, figi FNG112600000,
			// class code SPBFUT): uid verified live on 2026-10-06. Natural
			// gas family; the near month NGV6 carries more volume but sits
			// below the driver's min_entry_days_to_expiry=30 horizon, so the
			// most liquid entry-eligible series is pinned here.
			CanonicalSymbol:      "NG-11.26",
			ProviderInstrumentID: "89efcc3f-ef59-4e26-b2f0-0f659f94ac93",
			MarketType:           "futures",
			Aliases:              []string{"NG", "NGX6"},
			InstrumentType:       model.InstrumentTypeFuture,
			ProviderIDKind:       model.ProviderIDKindInstrumentUID,
			PriceUnit:            model.PriceUnitPoints,
			VolumeUnit:           model.VolumeUnitLots,
			ExpirationUTCMS:      1795651200000, // 2026-11-26T00:00:00Z, provider expiration_date
		},
		{CanonicalSymbol: "SBER", ProviderInstrumentID: "BBG004730N88", MarketType: "shares", Aliases: []string{"SBER/RUB"}},
		{CanonicalSymbol: "GAZP", ProviderInstrumentID: "BBG004730RP0", MarketType: "shares", Aliases: []string{"GAZP/RUB"}},
		{CanonicalSymbol: "LKOH", ProviderInstrumentID: "BBG004731032", MarketType: "shares", Aliases: []string{"LKOH/RUB"}},
		{CanonicalSymbol: "YDEX", ProviderInstrumentID: "TCS00A107T19", MarketType: "shares", Aliases: []string{"YDEX/RUB"}},
		{CanonicalSymbol: "VTBR", ProviderInstrumentID: "BBG004730ZJ9", MarketType: "shares", Aliases: []string{"VTBR/RUB", "VTB", "VTB/RUB"}},
		{CanonicalSymbol: "MOEX", ProviderInstrumentID: "BBG004730JJ5", MarketType: "shares", Aliases: []string{"MOEX/RUB"}},
		{CanonicalSymbol: "IRAO", ProviderInstrumentID: "BBG004S68473", MarketType: "shares", Aliases: []string{"IRAO/RUB"}},
		{CanonicalSymbol: "NLMK", ProviderInstrumentID: "BBG004S681B4", MarketType: "shares", Aliases: []string{"NLMK/RUB"}},
		{CanonicalSymbol: "GCHE", ProviderInstrumentID: "BBG000RTHVK7", MarketType: "shares", Aliases: []string{"GCHE/RUB"}},
		{CanonicalSymbol: "RASP", ProviderInstrumentID: "BBG004S68696", MarketType: "shares", Aliases: []string{"RASP/RUB"}},
		{CanonicalSymbol: "HEAD", ProviderInstrumentID: "TCS20A107662", MarketType: "shares", Aliases: []string{"HEAD/RUB"}},
		{CanonicalSymbol: "VKCO", ProviderInstrumentID: "TCS00A106YF0", MarketType: "shares", Aliases: []string{"VKCO/RUB"}},
		{CanonicalSymbol: "ROSN", ProviderInstrumentID: "BBG004731354", MarketType: "shares", Aliases: []string{"ROSN/RUB"}},
		{CanonicalSymbol: "ASTR", ProviderInstrumentID: "RU000A106T36", MarketType: "shares", Aliases: []string{"ASTR/RUB"}},
		{CanonicalSymbol: "AFLT", ProviderInstrumentID: "BBG004S683W7", MarketType: "shares", Aliases: []string{"AFLT/RUB"}},
		{CanonicalSymbol: "FLOT", ProviderInstrumentID: "BBG000R04X57", MarketType: "shares", Aliases: []string{"FLOT/RUB"}},
		{CanonicalSymbol: "MAGN", ProviderInstrumentID: "BBG004S68507", MarketType: "shares", Aliases: []string{"MAGN/RUB"}},
		{CanonicalSymbol: "POSI", ProviderInstrumentID: "TCS00A103X66", MarketType: "shares", Aliases: []string{"POSI/RUB"}},
		{CanonicalSymbol: "MTSS", ProviderInstrumentID: "BBG004S681W1", MarketType: "shares", Aliases: []string{"MTSS/RUB"}},
		{CanonicalSymbol: "RUAL", ProviderInstrumentID: "BBG008F2T3T2", MarketType: "shares", Aliases: []string{"RUAL/RUB"}},
		{CanonicalSymbol: "OZON", ProviderInstrumentID: "TCS80A10CW95", MarketType: "shares", Aliases: []string{"OZON/RUB"}},
		{CanonicalSymbol: "T", ProviderInstrumentID: "TCS80A107UL4", MarketType: "shares", Aliases: []string{"T/RUB"}},
		{CanonicalSymbol: "X5", ProviderInstrumentID: "TCS03A108X38", MarketType: "shares", Aliases: []string{"X5/RUB"}},
		{CanonicalSymbol: "MGNT", ProviderInstrumentID: "BBG004RVFCY3", MarketType: "shares", Aliases: []string{"MGNT/RUB"}},
		{CanonicalSymbol: "SMLT", ProviderInstrumentID: "BBG00F6NKQX3", MarketType: "shares", Aliases: []string{"SMLT/RUB"}},
		{CanonicalSymbol: "ALRS", ProviderInstrumentID: "BBG004S68B31", MarketType: "shares", Aliases: []string{"ALRS/RUB"}},
		{CanonicalSymbol: "PLZL", ProviderInstrumentID: "BBG000R607Y3", MarketType: "shares", Aliases: []string{"PLZL/RUB"}},
		{CanonicalSymbol: "SIBN", ProviderInstrumentID: "BBG004S684M6", MarketType: "shares", Aliases: []string{"SIBN/RUB"}},
		{CanonicalSymbol: "TATN", ProviderInstrumentID: "BBG004RVFFC0", MarketType: "shares", Aliases: []string{"TATN/RUB"}},
		{CanonicalSymbol: "NVTK", ProviderInstrumentID: "BBG00475KKY8", MarketType: "shares", Aliases: []string{"NVTK/RUB"}},
		{CanonicalSymbol: "GMKN", ProviderInstrumentID: "BBG004731489", MarketType: "shares", Aliases: []string{"GMKN/RUB"}},
		{CanonicalSymbol: "SGZH", ProviderInstrumentID: "BBG0100R9963", MarketType: "shares", Aliases: []string{"SGZH/RUB"}},
		{CanonicalSymbol: "UPRO", ProviderInstrumentID: "BBG004S686W0", MarketType: "shares", Aliases: []string{"UPRO/RUB"}},
		{CanonicalSymbol: "SPBE", ProviderInstrumentID: "TCS60A0JQ9P9", MarketType: "shares", Aliases: []string{"SPBE/RUB"}},
		{CanonicalSymbol: "AFKS", ProviderInstrumentID: "BBG004S68614", MarketType: "shares", Aliases: []string{"AFKS/RUB"}},
		{CanonicalSymbol: "CHMF", ProviderInstrumentID: "BBG00475K6C3", MarketType: "shares", Aliases: []string{"CHMF/RUB"}},
		{CanonicalSymbol: "RNFT", ProviderInstrumentID: "BBG00F9XX7H4", MarketType: "shares", Aliases: []string{"RNFT/RUB"}},
		{CanonicalSymbol: "RAGR", ProviderInstrumentID: "TCS90A0JQUZ6", MarketType: "shares", Aliases: []string{"RAGR/RUB"}},
		{CanonicalSymbol: "SFIN", ProviderInstrumentID: "BBG003LYCMB1", MarketType: "shares", Aliases: []string{"SFIN/RUB"}},
		{CanonicalSymbol: "SVCB", ProviderInstrumentID: "TCS00A0ZZAC4", MarketType: "shares", Aliases: []string{"SVCB/RUB"}},
	}
	return instruments
}
