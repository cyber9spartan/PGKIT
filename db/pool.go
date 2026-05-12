package db

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PoolConfig holds settings for a single named connection pool.
// ConnString is required; all other fields have built-in defaults.
type PoolConfig struct {
	ConnString        string
	MaxConns          int32
	MinConns          int32
	MaxConnIdleTime   time.Duration
	MaxConnLifetime   time.Duration
	HealthCheckPeriod time.Duration
	ConnectTimeout    time.Duration
	ForceIPv4         bool
}

func (c *PoolConfig) applyDefaults() {
	if c.MaxConns == 0 {
		c.MaxConns = 10
	}
	if c.MinConns == 0 {
		c.MinConns = 2
	}
	if c.MaxConnIdleTime == 0 {
		c.MaxConnIdleTime = 5 * time.Minute
	}
	if c.MaxConnLifetime == 0 {
		c.MaxConnLifetime = 1 * time.Hour
	}
	if c.HealthCheckPeriod == 0 {
		c.HealthCheckPeriod = 1 * time.Minute
	}
	if c.ConnectTimeout == 0 {
		c.ConnectTimeout = 20 * time.Second
	}
}

// NamedPool pairs a name with its pool configuration.
// Each NamedPool can have completely independent credentials and DSN.
type NamedPool struct {
	Name string
	PoolConfig
}

type poolManager struct {
	mu    sync.RWMutex
	pools map[string]*pgxpool.Pool
}

// Get returns the *pgxpool.Pool registered under name, or nil if not found.
func (m *poolManager) Get(name string) *pgxpool.Pool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.pools[name]
}

func (m *poolManager) HealthCheck(ctx context.Context) error {
	m.mu.RLock()
	names := make([]string, 0, len(m.pools))
	pools := make([]*pgxpool.Pool, 0, len(m.pools))
	for name, p := range m.pools {
		names = append(names, name)
		pools = append(pools, p)
	}
	m.mu.RUnlock()

	type result struct {
		name string
		err  error
	}
	ch := make(chan result, len(pools))
	for i, p := range pools {
		go func(name string, pool *pgxpool.Pool) {
			if err := pool.Ping(ctx); err != nil {
				ch <- result{name, err}
			} else {
				ch <- result{name, nil}
			}
		}(names[i], p)
	}

	var errs []error
	for range pools {
		if r := <-ch; r.err != nil {
			errs = append(errs, fmt.Errorf("pool %s: %w", r.name, r.err))
		}
	}
	return errors.Join(errs...)
}

func (m *poolManager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, p := range m.pools {
		p.Close()
	}
}

func newPoolManager(ctx context.Context, namedPools []NamedPool) (*poolManager, error) {
	if len(namedPools) == 0 {
		return nil, errors.New("db: at least one NamedPool is required")
	}

	built := make(map[string]*pgxpool.Pool, len(namedPools))
	for _, np := range namedPools {
		if np.Name == "" {
			closeAll(built)
			return nil, errors.New("db: NamedPool.Name must not be empty")
		}
		if np.ConnString == "" {
			closeAll(built)
			return nil, fmt.Errorf("db: pool %q: ConnString is required", np.Name)
		}
		if _, dup := built[np.Name]; dup {
			closeAll(built)
			return nil, fmt.Errorf("db: duplicate pool name %q", np.Name)
		}
		p, err := buildPool(ctx, np.PoolConfig, np.Name)
		if err != nil {
			closeAll(built)
			return nil, fmt.Errorf("db: pool %q: %w", np.Name, err)
		}
		built[np.Name] = p
	}

	return &poolManager{pools: built}, nil
}

func closeAll(pools map[string]*pgxpool.Pool) {
	for _, p := range pools {
		p.Close()
	}
}

func buildPool(ctx context.Context, cfg PoolConfig, name string) (*pgxpool.Pool, error) {
	cfg.applyDefaults()

	pCfg, err := pgxpool.ParseConfig(cfg.ConnString)
	if err != nil {
		return nil, fmt.Errorf("parse config for %s pool: %w", name, err)
	}

	if cfg.ForceIPv4 {
		pCfg.ConnConfig.LookupFunc = ipv4OnlyLookup
	} else {
		pCfg.ConnConfig.LookupFunc = preferIPv4Lookup
	}

	pCfg.MaxConns = cfg.MaxConns
	pCfg.MinConns = cfg.MinConns
	pCfg.MaxConnIdleTime = cfg.MaxConnIdleTime
	pCfg.MaxConnLifetime = cfg.MaxConnLifetime
	pCfg.HealthCheckPeriod = cfg.HealthCheckPeriod
	pCfg.ConnConfig.ConnectTimeout = cfg.ConnectTimeout
	pCfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	pool, err := pgxpool.NewWithConfig(ctx, pCfg)
	if err != nil {
		return nil, fmt.Errorf("create %s pool: %w", name, err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, cfg.ConnectTimeout)
	defer cancel()
	if err = pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping %s pool: %w", name, err)
	}
	return pool, nil
}

// preferIPv4Lookup returns IPv4 addresses first, IPv6 as fallback.
// pgx tries each in order — this prefers IPv4 (required on GCP Cloud Run)
// while still working when only AAAA records exist (local dev with Neon/Supabase).
func preferIPv4Lookup(ctx context.Context, host string) ([]string, error) {
	if ip := net.ParseIP(host); ip != nil {
		return []string{host}, nil
	}
	all, err := net.DefaultResolver.LookupHost(ctx, host)
	if err != nil {
		return nil, fmt.Errorf("db: DNS lookup %s: %w", host, err)
	}
	var v4, v6 []string
	for _, a := range all {
		if ip := net.ParseIP(a); ip != nil {
			if ip.To4() != nil {
				v4 = append(v4, a)
			} else {
				v6 = append(v6, a)
			}
		}
	}
	return append(v4, v6...), nil
}

// ipv4OnlyLookup fails hard if no A record exists.
// Avoids a slow IPv6 timeout on VMs without an IPv6 internet route.
func ipv4OnlyLookup(ctx context.Context, host string) ([]string, error) {
	if ip := net.ParseIP(host); ip != nil {
		if ip.To4() != nil {
			return []string{host}, nil
		}
		return nil, fmt.Errorf("db: IPv6 literal %s rejected (ForceIPv4=true)", host)
	}
	all, err := net.DefaultResolver.LookupHost(ctx, host)
	if err != nil {
		return nil, fmt.Errorf("db: DNS lookup %s: %w", host, err)
	}
	var v4 []string
	for _, a := range all {
		if ip := net.ParseIP(a); ip != nil && ip.To4() != nil {
			v4 = append(v4, a)
		}
	}
	if len(v4) == 0 {
		return nil, fmt.Errorf("db: no IPv4 address for %s (got %v)", host, all)
	}
	return v4, nil
}
