package config

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Cluster            Cluster            `yaml:"cluster"`
	Listeners          Listeners          `yaml:"listeners"`
	Auth               Auth               `yaml:"auth"`
	Providers          Providers          `yaml:"providers"`
	Limits             Limits             `yaml:"limits"`
	Warmup             Warmup             `yaml:"warmup,omitempty"`
	InstrumentRegistry InstrumentRegistry `yaml:"instrument_registry,omitempty"`
	CapacityPlan       CapacityPlan       `yaml:"capacity_plan,omitempty"`
	RoutingHash        string
}
type Cluster struct {
	ClusterID      string `yaml:"cluster_id"`
	NodeID         string `yaml:"node_id"`
	RoutingVersion int    `yaml:"routing_version"`
	Nodes          []Node `yaml:"nodes"`
	// FailoverMode selects the control-plane owner-activation rule.
	//   "manual"     — upstream-owner is exactly the deterministic primary;
	//                  automatic takeover is disabled, the standby may never
	//                  call the provider without a new routing_version. This is
	//                  the fail-closed default (spec: automatic failover
	//                  forbidden until Phase 6 design review).
	//   "automatic"  — on verified primary failure the deterministic standby may
	//                  acquire a per-SeriesKey lease (fencing token) via a
	//                  quorum of witnesses and become the upstream-owner. Requires
	//                  Witnesses to be configured with a strict majority.
	FailoverMode string `yaml:"failover_mode"`
	// Witnesses are extra membership entries that participate only in the lease
	// quorum. They never own series, never serve data, and never call the
	// provider. In a 2-VPS cluster they are what turns a minority partition into
	// a confirmed majority so a surviving node can fence a presumed-dead owner.
	Witnesses []Witness `yaml:"witnesses,omitempty"`
}

// Witness is a quorum participant with no data-plane role.
type Witness struct {
	ID      string `yaml:"id"`
	PeerURL string `yaml:"peer_url"`
}

type Node struct {
	ID      string  `yaml:"id"`
	Weight  float64 `yaml:"weight"`
	PeerURL string  `yaml:"peer_url"`
}
type Listeners struct{ Client, Peer string }
type Auth struct {
	// NodeTokenEnv names the environment variable holding the shared peer
	// node credential. The secret itself never enters the config file.
	NodeTokenEnv string `yaml:"node_token_env"`
	// Clients is the optional authenticated client-identity allowlist for the
	// loopback client boundary (task 109): client_id and priority are derived
	// from the matched Bearer credential, never from client-controlled
	// headers, so a caller cannot raise its own priority. Token values live
	// only in the protected host environment; the config references them by
	// env name. An empty list keeps the legacy loopback behavior (X-Client-ID
	// is advisory, every client shares the live-refresh priority).
	Clients []ClientIdentity `yaml:"clients,omitempty"`
}

// ClientIdentity binds one logical client to its credential env and fixed
// scheduler priority.
type ClientIdentity struct {
	ID       string `yaml:"id"`
	TokenEnv string `yaml:"token_env"`
	// Priority is fixed: "live_refresh" (closed-bar strategy refresh) or
	// "warmup" (historical/cold warmup). It is never accepted from requests.
	Priority string `yaml:"priority"`
}
type Providers struct {
	TBank TBank `yaml:"tbank"`
}
type TBank struct {
	CredentialGroup         string `yaml:"credential_group"`
	TokenEnv                string `yaml:"token_env"`
	SafeBudgetPerMinute     int    `yaml:"safe_budget_per_minute"`
	NodeHardBudgetPerMinute int    `yaml:"node_hard_budget_per_minute"`
	// NodeHardBudgets maps every member node id to its hard per-minute share
	// of the credential group's safe budget. It is required in cluster mode
	// (more than one member) and validated against the cluster invariant
	// sum(shares) <= safe_budget_per_minute on every node.
	NodeHardBudgets map[string]int `yaml:"node_hard_budgets_per_minute"`
}
type Limits struct {
	RequestTimeoutMS        int    `yaml:"request_timeout_ms"`
	MaxRequestLimit         int    `yaml:"max_request_limit"`
	MaxBatchItems           int    `yaml:"max_batch_items"`
	BatchFanOut             int    `yaml:"batch_fan_out"`
	CacheRetentionMS        int64  `yaml:"cache_retention_ms"`
	StoragePath             string `yaml:"storage_path"`
	QueueCapacity           int    `yaml:"queue_capacity"`
	WorkerCount             int    `yaml:"worker_count"`
	MaxRetries              int    `yaml:"max_retries"`
	RetryBaseDelayMS        int    `yaml:"retry_base_delay_ms"`
	PerClientQuotaPerMinute int    `yaml:"per_client_quota_per_minute"`
	MaxTrackedClients       int    `yaml:"max_tracked_clients"`
	DrainTimeoutMS          int    `yaml:"drain_timeout_ms"`
	// LeaseTTLMS bounds how long a fencing token is valid without renewal. A
	// node may only call the provider for a series while its held lease token is
	// newer than the last committed token AND unexpired (spec P6 invariant).
	LeaseTTLMS int `yaml:"lease_ttl_ms"`
	// PublicationGraceMS shifts the refresh-epoch boundary (task 109): a bar
	// that closed less than this ago is still accounted to the previous
	// epoch, so the gateway does not re-fetch a freshly closed bar before the
	// provider typically publishes it. 0 disables the shift.
	PublicationGraceMS int `yaml:"publication_grace_ms,omitempty"`
	// MaxRefreshAttemptsPerEpoch bounds synchronous refresh fetches per
	// series per missing range per refresh epoch (0 selects the default of
	// 2). Beyond the bound, identical re-polls are served from cache with
	// honest stale semantics until the next epoch.
	MaxRefreshAttemptsPerEpoch int `yaml:"max_refresh_attempts_per_epoch,omitempty"`
}

// InstrumentRegistry configures the runtime refresh of the TBank instrument
// mapping (F20). It only matters together with -tbank-instruments-file: the
// gateway periodically re-reads that file and applies changed snapshots
// atomically (validate, persist, swap, re-record registry metrics), so a
// futures contract can roll without a restart.
type InstrumentRegistry struct {
	// RefreshIntervalMS is the reload cadence; 0 (the default) disables the
	// runtime refresh and keeps the startup-only behavior.
	RefreshIntervalMS int `yaml:"refresh_interval_ms,omitempty"`
}

// Warmup configures the controlled prewarm job (task 109): after start the
// gateway doses warmup-priority refreshes for the local universe so cold
// Freqtrade starts find a warm cache instead of initiating 80 upstream
// misses at once.
type Warmup struct {
	// Enabled gates the prewarm job; absent/false keeps startup behavior.
	Enabled bool `yaml:"enabled,omitempty"`
	// Timeframes lists the series timeframes to prewarm (bounded vocabulary).
	Timeframes []string `yaml:"timeframes,omitempty"`
	// SeriesPerMinute is the pacing bound; the job never exceeds it.
	SeriesPerMinute int `yaml:"series_per_minute,omitempty"`
	// WindowLimit is the candle count requested per series (≥ typical client
	// windows so client polls become complete cache hits).
	WindowLimit int `yaml:"window_limit,omitempty"`
}

func Load(data []byte) (Config, error) {
	var cfg Config
	decoder := yaml.NewDecoder(strings.NewReader(string(data)))
	decoder.KnownFields(true)
	if err := decoder.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("decode config: %w", err)
	}
	// Default the lease TTL when unset so existing configs stay valid; the
	// fail-closed gate and renewer still get a sane bound. In automatic mode a
	// node will also refuse to start if the value is missing via the quorum gate
	// in main.go, but we do not force it for the manual (default) path.
	if cfg.Limits.LeaseTTLMS <= 0 {
		cfg.Limits.LeaseTTLMS = 30_000
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	cfg.RoutingHash = cfg.routingHash()
	return cfg, nil
}
func (c Config) Validate() error {
	if c.Cluster.ClusterID == "" || c.Cluster.NodeID == "" || c.Cluster.RoutingVersion <= 0 {
		return fmt.Errorf("cluster id, node id, and positive routing version are required")
	}
	// Failover mode is fail-closed: unrecognized values default to manual, and
	// automatic mode is rejected unless a strict quorum (local node + a majority
	// of witnesses) can be formed. This is the Phase 6 safety gate — automatic
	// takeover must never enable itself without a witness-majority configuration.
	switch c.Cluster.FailoverMode {
	case "", "manual":
		// default fail-closed path; no quorum required.
	case "automatic":
		if len(c.Cluster.Witnesses) == 0 {
			return fmt.Errorf("failover_mode=automatic requires at least one witness for a lease quorum")
		}
		// quorum = local node + witnesses; a strict majority must be reachable
		// for any takeover to confirm, so a minority partition can never fence.
		if 1+len(c.Cluster.Witnesses) < 3 {
			return fmt.Errorf("failover_mode=automatic requires at least two witnesses (quorum needs >=3 members)")
		}
		seenW := map[string]bool{}
		for _, w := range c.Cluster.Witnesses {
			if w.ID == "" || w.ID == c.Cluster.NodeID || seenW[w.ID] || !isPrivateURL(w.PeerURL) {
				return fmt.Errorf("invalid witness member %q", w.ID)
			}
			seenW[w.ID] = true
		}
	default:
		return fmt.Errorf("failover_mode must be manual or automatic (got %q)", c.Cluster.FailoverMode)
	}
	// Loopback exception (GW-PUB.02): the peer boundary may use loopback ONLY
	// when the cluster has exactly one node, no witnesses, and failover mode
	// "" or "manual" — the single-node fake-provider quick start on one
	// developer machine. Every other topology (multi-node membership,
	// automatic failover, any witness present) keeps the private non-loopback
	// requirement exactly as before, fail-closed, and the witness gate stays
	// private-only unconditionally.
	loopbackPeerAllowed := len(c.Cluster.Nodes) == 1 && len(c.Cluster.Witnesses) == 0 &&
		(c.Cluster.FailoverMode == "" || c.Cluster.FailoverMode == "manual")
	if !isLoopback(c.Listeners.Client) {
		return fmt.Errorf("client listener must use loopback")
	}
	if !isPrivate(c.Listeners.Peer) && !(loopbackPeerAllowed && isLoopback(c.Listeners.Peer)) {
		return fmt.Errorf("peer listener must use a private address")
	}
	if c.Auth.NodeTokenEnv == "" {
		return fmt.Errorf("auth.node_token_env is required for the peer boundary")
	}
	seen := map[string]bool{}
	found := false
	for _, node := range c.Cluster.Nodes {
		if node.ID == "" || seen[node.ID] || node.Weight <= 0 || math.IsNaN(node.Weight) {
			return fmt.Errorf("invalid cluster node")
		}
		if !isPrivateURL(node.PeerURL) && !(loopbackPeerAllowed && isLoopbackURL(node.PeerURL)) {
			return fmt.Errorf("invalid cluster node")
		}
		seen[node.ID] = true
		found = found || node.ID == c.Cluster.NodeID
	}
	if !found {
		return fmt.Errorf("local node is not in membership")
	}
	// The peer listener must be the private address of exactly one member so
	// every node binds a distinct, membership-bounded endpoint.
	localNode, _ := c.localNode()
	bindHost := hostOf(c.Listeners.Peer)
	peerHost, _ := peerURLHost(localNode.PeerURL)
	if bindHost != peerHost {
		return fmt.Errorf("peer listener bind must match the local node peer_url host")
	}
	if duplicatePeerURL(c.Cluster.Nodes) {
		return fmt.Errorf("duplicate peer_url across cluster nodes")
	}
	p := c.Providers.TBank
	if p.CredentialGroup == "" || p.TokenEnv == "" || p.SafeBudgetPerMinute <= 0 || p.NodeHardBudgetPerMinute <= 0 || p.NodeHardBudgetPerMinute > p.SafeBudgetPerMinute {
		return fmt.Errorf("invalid tbank provider budget configuration")
	}
	// Cluster budgets: when the full per-node share map is present it
	// must cover exactly the membership and satisfy the cluster invariant
	// sum(shares) <= safe user budget. Every node validates the identical
	// canonical config, so an unsafe split is rejected everywhere, before
	// any listener starts.
	if p.NodeHardBudgets != nil {
		if len(p.NodeHardBudgets) != len(c.Cluster.Nodes) {
			return fmt.Errorf("node_hard_budgets_per_minute must cover every cluster member")
		}
		sum := 0
		for _, node := range c.Cluster.Nodes {
			share, ok := p.NodeHardBudgets[node.ID]
			if !ok || share <= 0 {
				return fmt.Errorf("node_hard_budgets_per_minute is missing a positive share for node %q", node.ID)
			}
			sum += share
		}
		if sum > p.SafeBudgetPerMinute {
			return fmt.Errorf("cluster budget invariant violated: sum of node hard shares (%d) exceeds safe user budget (%d)", sum, p.SafeBudgetPerMinute)
		}
		if p.NodeHardBudgets[c.Cluster.NodeID] != p.NodeHardBudgetPerMinute {
			return fmt.Errorf("node_hard_budget_per_minute must match the local node's share in node_hard_budgets_per_minute")
		}
	}
	if c.Limits.RequestTimeoutMS <= 0 || c.Limits.MaxRequestLimit <= 0 || c.Limits.MaxBatchItems <= 0 || c.Limits.BatchFanOut <= 0 || c.Limits.CacheRetentionMS <= 0 || strings.TrimSpace(c.Limits.StoragePath) == "" || c.Limits.QueueCapacity <= 0 || c.Limits.WorkerCount <= 0 || c.Limits.MaxRetries < 0 || c.Limits.RetryBaseDelayMS <= 0 || c.Limits.PerClientQuotaPerMinute <= 0 || c.Limits.MaxTrackedClients <= 0 || c.Limits.DrainTimeoutMS <= 0 {
		return fmt.Errorf("all runtime limits are required and must be valid")
	}
	// Task 109 client identity allowlist: ids unique, credential envs unique
	// (one identity per token), priorities from the bounded vocabulary.
	seenIDs := map[string]bool{}
	seenTokenEnvs := map[string]bool{}
	for _, client := range c.Auth.Clients {
		if client.ID == "" || client.TokenEnv == "" || seenIDs[client.ID] || seenTokenEnvs[client.TokenEnv] {
			return fmt.Errorf("invalid auth client identity %q", client.ID)
		}
		switch client.Priority {
		case "live_refresh", "warmup":
		default:
			return fmt.Errorf("auth client %q priority must be live_refresh or warmup (got %q)", client.ID, client.Priority)
		}
		seenIDs[client.ID] = true
		seenTokenEnvs[client.TokenEnv] = true
	}
	// Task 109 controlled prewarm: bounded timeframes, positive pacing, and a
	// window that cannot exceed the per-request provider bound.
	if c.Warmup.Enabled {
		if len(c.Warmup.Timeframes) == 0 {
			return fmt.Errorf("warmup.enabled requires at least one timeframe")
		}
		allowedTimeframes := map[string]bool{"1m": true, "5m": true, "15m": true, "30m": true, "1h": true, "1d": true}
		seenTF := map[string]bool{}
		for _, tf := range c.Warmup.Timeframes {
			if !allowedTimeframes[tf] || seenTF[tf] {
				return fmt.Errorf("invalid warmup timeframe %q", tf)
			}
			seenTF[tf] = true
		}
		if c.Warmup.SeriesPerMinute <= 0 || c.Warmup.WindowLimit <= 0 || c.Warmup.WindowLimit > c.Limits.MaxRequestLimit {
			return fmt.Errorf("warmup series_per_minute and window_limit (1..max_request_limit) must be valid")
		}
	}
	if c.Limits.PublicationGraceMS < 0 || c.Limits.MaxRefreshAttemptsPerEpoch < 0 {
		return fmt.Errorf("publication_grace_ms and max_refresh_attempts_per_epoch must be non-negative")
	}
	if c.InstrumentRegistry.RefreshIntervalMS < 0 {
		return fmt.Errorf("instrument_registry.refresh_interval_ms must be non-negative")
	}
	return nil
}

func (c Config) localNode() (Node, bool) {
	for _, node := range c.Cluster.Nodes {
		if node.ID == c.Cluster.NodeID {
			return node, true
		}
	}
	return Node{}, false
}

func duplicatePeerURL(nodes []Node) bool {
	seen := map[string]bool{}
	for _, node := range nodes {
		key := strings.ToLower(strings.TrimSpace(node.PeerURL))
		if seen[key] {
			return true
		}
		seen[key] = true
	}
	return false
}

func hostOf(address string) string {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return ""
	}
	return host
}

func peerURLHost(raw string) (string, bool) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Hostname() == "" {
		return "", false
	}
	return parsed.Hostname(), true
}
func (c Config) routingHash() string {
	// Canonical form: routing-relevant fields only, members in sorted id
	// order, weights rendered via strconv ('1' vs '1.0' unify where possible
	// through FormatFloat with -1 precision). This guarantees every node
	// hashing the same logical config derives the same value, independent of
	// YAML formatting or member order in the file.
	var b strings.Builder
	fmt.Fprintf(&b, "cluster_id=%s\nrouting_version=%d\n", c.Cluster.ClusterID, c.Cluster.RoutingVersion)
	nodes := append([]Node(nil), c.Cluster.Nodes...)
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	for _, node := range nodes {
		fmt.Fprintf(&b, "node=%s,weight=%s,peer_url=%s\n", node.ID, strconv.FormatFloat(node.Weight, 'g', -1, 64), node.PeerURL)
	}
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:])
}
func isLoopback(address string) bool {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return false
	}
	return net.ParseIP(host).IsLoopback()
}
func isPrivate(address string) bool {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsPrivate()
}
func isPrivateURL(raw string) bool {
	raw = strings.TrimPrefix(strings.TrimPrefix(raw, "https://"), "http://")
	return isPrivate(raw)
}
func isLoopbackURL(raw string) bool {
	raw = strings.TrimPrefix(strings.TrimPrefix(raw, "https://"), "http://")
	return isLoopback(raw)
}
