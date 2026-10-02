// Package tbank adapts the TBank investapi gRPC surface to the gateway
// provider boundary, plus the venue instrument registry. Registry alias
// resolution (logical aliases such as a rolling futures ticker) is
// deliberately separate from stored series identity (the physical provider
// instrument UID): resolution maps a request alias onto exactly one physical
// instrument per snapshot, and Refresh replaces the whole snapshot atomically
// so a logical alias may roll to a new contract without ever mixing old and
// new series data.
package tbank

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/jedi108/market-data-gateway/internal/apperror"
	"github.com/jedi108/market-data-gateway/internal/model"
)

// Instrument is one physical instrument served by the TBank gateway boundary.
// ProviderInstrumentID is the physical provider identifier — a share FIGI or a
// futures contract instrument_uid — and is the only storage/cache identity.
// Aliases (including a logical rolling futures alias such as "GOLD") are
// resolution inputs only.
//
// Zero-valued F20 fields keep the legacy share shape: InstrumentType defaults
// per MarketType, ProviderIDKind to FIGI, PriceUnit to currency, VolumeUnit to
// lots. The zero value is therefore exactly the pre-futures share instrument.
type Instrument struct {
	CanonicalSymbol      string   `json:"canonical_symbol"`
	ProviderInstrumentID string   `json:"provider_instrument_id"`
	MarketType           string   `json:"market_type"`
	Aliases              []string `json:"aliases,omitempty"`
	// InstrumentType is model.InstrumentTypeShare or model.InstrumentTypeFuture.
	// Empty is derived from MarketType ("futures" -> future, otherwise share).
	InstrumentType string `json:"instrument_type,omitempty"`
	// ProviderIDKind is model.ProviderIDKindFIGI (shares) or
	// model.ProviderIDKindInstrumentUID (futures). Empty is derived from
	// InstrumentType.
	ProviderIDKind string `json:"provider_id_kind,omitempty"`
	// PriceUnit is model.PriceUnitCurrency (shares) or model.PriceUnitPoints
	// (futures). Empty is derived from InstrumentType.
	PriceUnit string `json:"price_unit,omitempty"`
	// VolumeUnit is model.VolumeUnitLots unless explicitly overridden.
	VolumeUnit string `json:"volume_unit,omitempty"`
	// Currency names the share quote currency (for example "RUB"); empty for
	// futures, whose price domain is points.
	Currency string `json:"currency,omitempty"`
	// ExpirationUTCMS is the physical futures contract expiration (UTC ms).
	// Required for futures, meaningless for shares.
	ExpirationUTCMS int64 `json:"expiration_utc_ms,omitempty"`
}

// futuresMarketType is the only market the future instrument class may live
// in. Keeping the pair consistent prevents a futures UID from being served
// under a share market (and vice versa), which would let one physical contract
// produce two different series identities.
const futuresMarketType = "futures"

// Registry resolves request aliases onto physical instruments. It is safe for
// concurrent use: Resolve/Describe read a lock-free snapshot pointer, Refresh
// installs a fully-built replacement snapshot atomically.
type Registry struct {
	mu        sync.RWMutex
	snapshot  *registrySnapshot
	refreshes atomic.Uint64
}

type registrySnapshot struct {
	aliases      map[string]Instrument
	byProviderID map[string]Instrument
	ordered      []Instrument
}

func NewRegistry(instruments []Instrument) (*Registry, error) {
	snapshot, err := buildSnapshot(instruments)
	if err != nil {
		return nil, err
	}
	return &Registry{snapshot: snapshot}, nil
}

// buildSnapshot validates and normalizes a full instrument list into an
// immutable registry snapshot. It has no side effects: on error nothing is
// built, so callers can use it to validate a candidate refresh before any
// state changes.
func buildSnapshot(instruments []Instrument) (*registrySnapshot, error) {
	snapshot := &registrySnapshot{
		aliases:      map[string]Instrument{},
		byProviderID: map[string]Instrument{},
		ordered:      make([]Instrument, 0, len(instruments)),
	}
	for _, instrument := range instruments {
		normalized, err := normalizeInstrument(instrument)
		if err != nil {
			return nil, fmt.Errorf("instrument %q: %w", instrument.CanonicalSymbol, err)
		}
		// Physical identity: the provider instrument UID indexes identity
		// metadata. Two entries with the same UID are a configuration error
		// (the same physical contract cannot have two canonical symbols).
		if _, exists := snapshot.byProviderID[normalized.ProviderInstrumentID]; exists {
			return nil, fmt.Errorf("duplicate provider instrument id %q", normalized.ProviderInstrumentID)
		}
		snapshot.byProviderID[normalized.ProviderInstrumentID] = normalized
		// Resolution: canonical symbol, the provider id itself, and every
		// declared alias. Within one snapshot an alias may map to exactly one
		// physical instrument; a conflict is an ambiguous-alias configuration
		// error (fail closed, never last-write-wins).
		for _, alias := range append(append([]string{}, normalized.Aliases...), normalized.CanonicalSymbol, normalized.ProviderInstrumentID) {
			key := normalize(alias)
			if key == "" {
				return nil, fmt.Errorf("blank alias")
			}
			if prior, exists := snapshot.aliases[key]; exists && prior.ProviderInstrumentID != normalized.ProviderInstrumentID {
				return nil, apperror.New(apperror.CodeAmbiguousSymbol, "ambiguous symbol alias", 0)
			}
			snapshot.aliases[key] = normalized
		}
		snapshot.ordered = append(snapshot.ordered, normalized)
	}
	sort.Slice(snapshot.ordered, func(i, j int) bool {
		return snapshot.ordered[i].ProviderInstrumentID < snapshot.ordered[j].ProviderInstrumentID
	})
	return snapshot, nil
}

// normalizeInstrument applies the per-instrument-type defaults and the
// capability boundary: only share and future instrument classes are served by
// the gateway, and a physical future must carry its expiration.
func normalizeInstrument(instrument Instrument) (Instrument, error) {
	if instrument.CanonicalSymbol == "" || instrument.ProviderInstrumentID == "" || instrument.MarketType == "" {
		return Instrument{}, fmt.Errorf("canonical symbol, provider instrument id, and market type are required")
	}
	instrument.MarketType = strings.ToLower(strings.TrimSpace(instrument.MarketType))
	instrument.CanonicalSymbol = strings.TrimSpace(instrument.CanonicalSymbol)
	// The provider instrument id is an opaque, case-sensitive identifier
	// (uppercase FIGI, lowercase UUID-like futures uid): it is stored and
	// looked up verbatim (trim only), never case-folded like a resolution
	// alias — identity comparisons must be exact.
	instrument.ProviderInstrumentID = strings.TrimSpace(instrument.ProviderInstrumentID)
	if instrument.InstrumentType == "" {
		if instrument.MarketType == futuresMarketType {
			instrument.InstrumentType = model.InstrumentTypeFuture
		} else {
			instrument.InstrumentType = model.InstrumentTypeShare
		}
	}
	switch instrument.InstrumentType {
	case model.InstrumentTypeFuture:
		if instrument.MarketType != futuresMarketType {
			return Instrument{}, fmt.Errorf("future instrument must use market type %q", futuresMarketType)
		}
		if instrument.ExpirationUTCMS <= 0 {
			return Instrument{}, fmt.Errorf("future instrument requires a physical expiration_utc_ms")
		}
		if instrument.Currency != "" {
			return Instrument{}, fmt.Errorf("future price domain is points; currency must be empty")
		}
		if instrument.ProviderIDKind == "" {
			instrument.ProviderIDKind = model.ProviderIDKindInstrumentUID
		}
		if instrument.PriceUnit == "" {
			instrument.PriceUnit = model.PriceUnitPoints
		}
	case model.InstrumentTypeShare:
		if instrument.MarketType == futuresMarketType {
			return Instrument{}, fmt.Errorf("share instrument must not use market type %q", futuresMarketType)
		}
		if instrument.ExpirationUTCMS != 0 {
			return Instrument{}, fmt.Errorf("share instrument must not carry an expiration")
		}
		if instrument.ProviderIDKind == "" {
			instrument.ProviderIDKind = model.ProviderIDKindFIGI
		}
		if instrument.PriceUnit == "" {
			instrument.PriceUnit = model.PriceUnitCurrency
		}
	default:
		// Capability boundary (F20): instrument classes outside the gateway's
		// served vocabulary fail closed with the explicit unsupported-
		// capability error instead of being silently dropped or served.
		return Instrument{}, apperror.New(apperror.CodeUnsupportedCapability, fmt.Sprintf("instrument type %q is not served by the gateway", instrument.InstrumentType), 0)
	}
	switch instrument.ProviderIDKind {
	case model.ProviderIDKindFIGI, model.ProviderIDKindInstrumentUID:
	default:
		return Instrument{}, fmt.Errorf("provider id kind must be figi or instrument_uid")
	}
	switch instrument.PriceUnit {
	case model.PriceUnitCurrency, model.PriceUnitPoints:
	default:
		return Instrument{}, fmt.Errorf("price unit must be currency or points")
	}
	if instrument.VolumeUnit == "" {
		instrument.VolumeUnit = model.VolumeUnitLots
	}
	if instrument.VolumeUnit != model.VolumeUnitLots {
		return Instrument{}, fmt.Errorf("unsupported volume unit %q", instrument.VolumeUnit)
	}
	if instrument.Currency != "" {
		instrument.Currency = strings.ToUpper(strings.TrimSpace(instrument.Currency))
	}
	aliases := make([]string, 0, len(instrument.Aliases))
	for _, alias := range instrument.Aliases {
		if trimmed := strings.TrimSpace(alias); trimmed != "" {
			aliases = append(aliases, trimmed)
		}
	}
	instrument.Aliases = aliases
	return instrument, nil
}

// Resolve maps a request alias (logical futures alias, canonical symbol, or
// provider id) onto the physical instrument of the current snapshot.
func (r *Registry) Resolve(alias string) (Instrument, error) {
	instrument, ok := r.current().aliases[normalize(alias)]
	if !ok {
		return Instrument{}, apperror.New(apperror.CodeUnknownSymbol, "unknown symbol", 0)
	}
	return instrument, nil
}

// InstrumentByProviderID returns the physical instrument for a provider
// instrument UID. This is the identity direction: it never resolves aliases,
// never case-folds (provider ids are exact), and is what stored series
// metadata is derived from.
func (r *Registry) InstrumentByProviderID(providerInstrumentID string) (Instrument, bool) {
	instrument, ok := r.current().byProviderID[strings.TrimSpace(providerInstrumentID)]
	return instrument, ok
}

// Describe maps a resolved SeriesKey onto its response metadata. A key whose
// physical instrument is not in the current snapshot (for example an expired
// contract removed by a later refresh) reports found=false so callers omit
// metadata rather than invent it.
func (r *Registry) Describe(key model.SeriesKey) (model.InstrumentMetadata, bool) {
	instrument, ok := r.InstrumentByProviderID(key.ProviderInstrumentID)
	if !ok {
		return model.InstrumentMetadata{}, false
	}
	return model.InstrumentMetadata{
		InstrumentType:  instrument.InstrumentType,
		CanonicalSymbol: instrument.CanonicalSymbol,
		ProviderIDKind:  instrument.ProviderIDKind,
		PriceUnit:       instrument.PriceUnit,
		VolumeUnit:      instrument.VolumeUnit,
		Currency:        instrument.Currency,
		ExpirationUTCMS: instrument.ExpirationUTCMS,
	}, true
}

// Instruments returns a copy of the current snapshot's instruments ordered by
// provider instrument id (the stable physical-identity order used for
// persistence and metrics).
func (r *Registry) Instruments() []Instrument {
	current := r.current().ordered
	out := make([]Instrument, len(current))
	copy(out, current)
	for i := range out {
		out[i].Aliases = append([]string(nil), out[i].Aliases...)
	}
	return out
}

// Refresh atomically replaces the whole instrument mapping. The candidate list
// is fully validated first: an invalid refresh leaves the current mapping
// untouched (fail closed), and a valid refresh becomes visible to all readers
// in one pointer swap — no reader ever observes a partially applied mapping.
func (r *Registry) Refresh(instruments []Instrument) error {
	candidate, err := buildSnapshot(instruments)
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.snapshot = candidate
	r.mu.Unlock()
	r.refreshes.Add(1)
	return nil
}

// Refreshes reports how many atomic refreshes have been applied since
// construction (startup state is generation zero).
func (r *Registry) Refreshes() uint64 { return r.refreshes.Load() }

func (r *Registry) current() *registrySnapshot {
	r.mu.RLock()
	snapshot := r.snapshot
	r.mu.RUnlock()
	return snapshot
}

func normalize(value string) string { return strings.ToUpper(strings.TrimSpace(value)) }
