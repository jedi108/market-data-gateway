package tbank

import (
	"fmt"
	"strings"

	"github.com/jedi108/market-data-gateway/internal/apperror"
)

type Instrument struct {
	CanonicalSymbol, ProviderInstrumentID, MarketType string
	Aliases                                           []string
}
type Registry struct{ aliases map[string]Instrument }

func NewRegistry(instruments []Instrument) (*Registry, error) {
	r := &Registry{aliases: map[string]Instrument{}}
	for _, instrument := range instruments {
		if instrument.CanonicalSymbol == "" || instrument.ProviderInstrumentID == "" || instrument.MarketType == "" {
			return nil, fmt.Errorf("instrument fields are required")
		}
		aliases := append(instrument.Aliases, instrument.CanonicalSymbol, instrument.ProviderInstrumentID)
		for _, alias := range aliases {
			normalized := normalize(alias)
			if normalized == "" {
				return nil, fmt.Errorf("blank alias")
			}
			if prior, exists := r.aliases[normalized]; exists && prior.ProviderInstrumentID != instrument.ProviderInstrumentID {
				return nil, apperror.New(apperror.CodeAmbiguousSymbol, "ambiguous symbol alias", 0)
			}
			r.aliases[normalized] = instrument
		}
	}
	return r, nil
}
func (r *Registry) Resolve(alias string) (Instrument, error) {
	instrument, ok := r.aliases[normalize(alias)]
	if !ok {
		return Instrument{}, apperror.New(apperror.CodeUnknownSymbol, "unknown symbol", 0)
	}
	return instrument, nil
}
func normalize(value string) string { return strings.ToUpper(strings.TrimSpace(value)) }
