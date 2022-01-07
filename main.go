// Copyright © 2021 Weald Technology Trading.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"fmt"
	"net/http"
	"time"

	// #nosec G108
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"syscall"

	execclient "github.com/attestantio/go-execution-client"
	homedir "github.com/mitchellh/go-homedir"
	"github.com/pkg/errors"
	zerologger "github.com/rs/zerolog/log"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/wealdtech/execd/services/blocks"
	batchblocks "github.com/wealdtech/execd/services/blocks/batch"
	individualblocks "github.com/wealdtech/execd/services/blocks/individual"
	execdb "github.com/wealdtech/execd/services/execdb"
	postgresqlexecdb "github.com/wealdtech/execd/services/execdb/postgresql"
	"github.com/wealdtech/execd/services/metrics"
	nullmetrics "github.com/wealdtech/execd/services/metrics/null"
	prometheusmetrics "github.com/wealdtech/execd/services/metrics/prometheus"
	"github.com/wealdtech/execd/services/mev"
	batchmev "github.com/wealdtech/execd/services/mev/batch"
	standardscheduler "github.com/wealdtech/execd/services/scheduler/standard"
	"github.com/wealdtech/execd/util"
)

// ReleaseVersion is the release version for the code.
var ReleaseVersion = "0.3.0"

func main() {
	os.Exit(main2())
}

func main2() int {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := fetchConfig(); err != nil {
		zerologger.Error().Err(err).Msg("Failed to fetch configuration")
		return 1
	}

	if err := initLogging(); err != nil {
		log.Error().Err(err).Msg("Failed to initialise logging")
		return 1
	}

	// runCommands will not return if a command is run.
	runCommands(ctx)

	logModules()
	log.Info().Str("version", ReleaseVersion).Msg("Starting execd")

	if err := initProfiling(); err != nil {
		log.Error().Err(err).Msg("Failed to initialise profiling")
		return 1
	}

	runtime.GOMAXPROCS(runtime.NumCPU() * 8)

	log.Trace().Msg("Starting metrics service")
	monitor, err := startMonitor(ctx)
	if err != nil {
		log.Error().Err(err).Msg("Failed to start metrics service")
		return 1
	}
	if err := registerMetrics(ctx, monitor); err != nil {
		log.Error().Err(err).Msg("Failed to register metrics")
		return 1
	}
	setRelease(ctx, ReleaseVersion)
	setReady(ctx, false)

	if err := startServices(ctx, monitor); err != nil {
		log.Error().Err(err).Msg("Failed to initialise services")
		return 1
	}
	setReady(ctx, true)

	log.Info().Msg("All services operational")

	// Wait for signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, os.Interrupt)
	for {
		sig := <-sigCh
		if sig == syscall.SIGINT || sig == syscall.SIGTERM || sig == os.Interrupt || sig == os.Kill {
			break
		}
	}

	log.Info().Msg("Stopping execd")
	return 0
}

// fetchConfig fetches configuration from various sources.
func fetchConfig() error {
	pflag.String("base-dir", "", "base directory for configuration files")
	pflag.Bool("version", false, "show version and exit")
	pflag.String("log-level", "info", "minimum level of messsages to log")
	pflag.String("log-file", "", "redirect log output to a file")
	pflag.String("profile-address", "", "Address on which to run Go profile server")
	pflag.String("tracing-address", "", "Address to which to send tracing data")
	pflag.Bool("blocks.enable", true, "Enable fetching of block-related information")
	pflag.Bool("blocks.transactions.enable", true, "Enable fetching of transaction-related information (requires blocks to be enabled)")
	pflag.Bool("blocks.transactions.events.enable", true, "Enable fetching of transaction event information (requires blocks and transactions to be enabled)")
	pflag.Bool("blocks.transactions.balances.enable", true, "Enable fetching of balance change information (requires blocks and transactions to be enabled)")
	pflag.Bool("blocks.transactions.storage.enable", true, "Enable fetching of storage change information (requires blocks and transactions to be enabled)")
	pflag.String("blocks.style", "batch", "Use different blocks fetcher (available: batch, individual)")
	pflag.Duration("blocks.interval", 10*time.Second, "Interval between block updates")
	pflag.Int32("blocks.start-height", -1, "Slot from which to start fetching blocks")
	pflag.Bool("mev.enable", true, "Enable setting MEV-related information")
	pflag.Int32("mev.start-height", -1, "Slot from which to start setting MEV information")
	pflag.Duration("mev.interval", 10*time.Second, "Interval between MEV updates")
	pflag.String("execclient.address", "", "Address for execution node JSON-RPC endpoint")
	pflag.Duration("execclient.timeout", 60*time.Second, "Timeout for execution node requests")
	pflag.Parse()
	if err := viper.BindPFlags(pflag.CommandLine); err != nil {
		return errors.Wrap(err, "failed to bind pflags to viper")
	}

	if viper.GetString("base-dir") != "" {
		// User-defined base directory.
		viper.AddConfigPath(resolvePath(""))
		viper.SetConfigName("execd")
	} else {
		// Home directory.
		home, err := homedir.Dir()
		if err != nil {
			return errors.Wrap(err, "failed to obtain home directory")
		}
		viper.AddConfigPath(home)
		viper.SetConfigName(".execd")
	}

	// Environment settings.
	viper.SetEnvPrefix("EXECD")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_", ".", "_"))
	viper.AutomaticEnv()

	// Defaults.
	viper.SetDefault("process-concurrency", int64(runtime.GOMAXPROCS(-1)))

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return errors.Wrap(err, "failed to read configuration file")
		}
	}

	return nil
}

// initProfiling initialises the profiling server.
func initProfiling() error {
	profileAddress := viper.GetString("profile-address")
	if profileAddress != "" {
		go func() {
			log.Info().Str("profile_address", profileAddress).Msg("Starting profile server")
			runtime.SetMutexProfileFraction(1)
			if err := http.ListenAndServe(profileAddress, nil); err != nil {
				log.Warn().Str("profile_address", profileAddress).Err(err).Msg("Failed to run profile server")
			}
		}()
	}
	return nil
}

func startMonitor(ctx context.Context) (metrics.Service, error) {
	var monitor metrics.Service
	if viper.Get("metrics.prometheus.listen-address") != nil {
		var err error
		monitor, err = prometheusmetrics.New(ctx,
			prometheusmetrics.WithLogLevel(util.LogLevel("metrics.prometheus")),
			prometheusmetrics.WithAddress(viper.GetString("metrics.prometheus.listen-address")),
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to start prometheus metrics service")
		}
		log.Info().Str("listen_address", viper.GetString("metrics.prometheus.listen-address")).Msg("Started prometheus metrics service")
	} else {
		log.Debug().Msg("No metrics service supplied; monitor not starting")
		monitor = &nullmetrics.Service{}
	}
	return monitor, nil
}

func startServices(ctx context.Context, monitor metrics.Service) error {
	log.Trace().Msg("Starting exec database service")
	execDB, err := postgresqlexecdb.New(ctx,
		postgresqlexecdb.WithLogLevel(util.LogLevel("execdb")),
		postgresqlexecdb.WithServer(viper.GetString("execdb.server")),
		postgresqlexecdb.WithPort(viper.GetInt32("execdb.port")),
		postgresqlexecdb.WithUser(viper.GetString("execdb.user")),
		postgresqlexecdb.WithPassword(viper.GetString("execdb.password")),
	)
	if err != nil {
		return errors.Wrap(err, "failed to start exec database service")
	}

	log.Trace().Msg("Checking for schema upgrades")
	if err := execDB.Upgrade(ctx); err != nil {
		return errors.Wrap(err, "failed to upgrade exec database")
	}

	log.Trace().Str("address", viper.GetString("execclient.address")).Msg("Fetching execution client")
	execClient, err := fetchClient(ctx, viper.GetString("execclient.address"))
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to fetch client %q", viper.GetString("execclient.address")))
	}
	if err != nil {
		return errors.Wrap(err, "failed to fetch execution client")
	}

	// Wait for the node to sync.
	for {
		syncState, err := execClient.(execclient.SyncingProvider).Syncing(ctx)
		if err != nil {
			log.Debug().Err(err).Msg("Failed to obtain node sync state; will re-test in 1 minute")
			time.Sleep(time.Minute)
			continue
		}
		if syncState == nil {
			log.Debug().Msg("No node sync state; will re-test in 1 minute")
			time.Sleep(time.Minute)
			continue
		}
		if syncState.Syncing {
			log.Debug().Msg("Node syncing; will re-test in 1 minute")
			time.Sleep(time.Minute)
			continue
		}
		break
	}

	log.Trace().Msg("Starting blocks service")
	if _, err := startBlocks(ctx, execClient, execDB, monitor); err != nil {
		return errors.Wrap(err, "failed to start blocks service")
	}

	log.Trace().Msg("Starting MEV service")
	if _, err := startMEV(ctx, execDB, monitor); err != nil {
		return errors.Wrap(err, "failed to start MEV service")
	}

	return nil
}

func logModules() {
	buildInfo, ok := debug.ReadBuildInfo()
	if ok {
		log.Trace().Str("path", buildInfo.Path).Msg("Main package")
		for _, dep := range buildInfo.Deps {
			log := log.Trace()
			if dep.Replace == nil {
				log = log.Str("path", dep.Path).Str("version", dep.Version)
			} else {
				log = log.Str("path", dep.Replace.Path).Str("version", dep.Replace.Version)
			}
			log.Msg("Dependency")
		}
	}
}

// resolvePath resolves a potentially relative path to an absolute path.
func resolvePath(path string) string {
	if filepath.IsAbs(path) {
		return path
	}
	baseDir := viper.GetString("base-dir")
	if baseDir == "" {
		homeDir, err := homedir.Dir()
		if err != nil {
			log.Fatal().Err(err).Msg("Could not determine a home directory")
		}
		baseDir = homeDir
	}
	return filepath.Join(baseDir, path)
}

func startBlocks(
	ctx context.Context,
	execClient execclient.Service,
	execDB execdb.Service,
	monitor metrics.Service,
) (
	blocks.Service,
	error,
) {
	if !viper.GetBool("blocks.enable") {
		return nil, nil
	}

	scheduler, err := standardscheduler.New(ctx,
		standardscheduler.WithLogLevel(util.LogLevel("scheduler")),
		standardscheduler.WithMonitor(monitor),
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to start scheduler service")
	}

	if viper.GetString("blocks.execclient.address") != "" {
		execClient, err = fetchClient(ctx, viper.GetString("blocks.execclient.address"))
		if err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf("failed to fetch client %q", viper.GetString("blocks.execclient.address")))
		}
	}

	chainHeightProvider, isProvider := execClient.(execclient.ChainHeightProvider)
	if !isProvider {
		return nil, errors.New("client does not provide chain height")
	}
	blocksProvider, isProvider := execClient.(execclient.BlocksProvider)
	if !isProvider {
		return nil, errors.New("client does not provide blocks")
	}
	blockReplaysProvider, isProvider := execClient.(execclient.BlockReplaysProvider)
	if !isProvider {
		return nil, errors.New("client does not provide block replays")
	}
	issuanceProvider, isProvider := execClient.(execclient.IssuanceProvider)
	if isProvider {
		// Confirm that it can fetch issuance.
		_, err := issuanceProvider.Issuance(ctx, "1")
		if err != nil {
			// It can't, remove the provider.
			issuanceProvider = nil
		}
	}
	transactionReceiptsProvider, isProvider := execClient.(execclient.TransactionReceiptsProvider)
	if !isProvider {
		return nil, errors.New("client does not provide transaction receipts")
	}
	blocksSetter, isSetter := execDB.(execdb.BlocksSetter)
	if !isSetter {
		return nil, errors.New("database does not store blocks")
	}
	transactionsSetter, isSetter := execDB.(execdb.TransactionsSetter)
	if !isSetter {
		return nil, errors.New("database does not store transactions")
	}
	transactionStateDiffsSetter, isSetter := execDB.(execdb.TransactionStateDiffsSetter)
	if !isSetter {
		return nil, errors.New("database does not store transaction state differences")
	}
	eventsSetter, isSetter := execDB.(execdb.EventsSetter)
	if !isSetter {
		return nil, errors.New("database does not store events")
	}

	var s blocks.Service
	switch viper.GetString("blocks.style") {
	case "individual":
		s, err = individualblocks.New(ctx,
			individualblocks.WithLogLevel(util.LogLevel("blocks")),
			individualblocks.WithMonitor(monitor),
			individualblocks.WithScheduler(scheduler),
			individualblocks.WithChainHeightProvider(chainHeightProvider),
			individualblocks.WithBlocksProvider(blocksProvider),
			individualblocks.WithBlockReplaysProvider(blockReplaysProvider),
			individualblocks.WithIssuanceProvider(issuanceProvider),
			individualblocks.WithTransactionReceiptsProvider(transactionReceiptsProvider),
			individualblocks.WithBlocksSetter(blocksSetter),
			individualblocks.WithTransactionsSetter(transactionsSetter),
			individualblocks.WithTransactionStateDiffsSetter(transactionStateDiffsSetter),
			individualblocks.WithEventsSetter(eventsSetter),
			individualblocks.WithStartHeight(viper.GetInt64("blocks.start-height")),
			individualblocks.WithTransactions(viper.GetBool("blocks.transactions.enable")),
			individualblocks.WithStorageChanges(viper.GetBool("blocks.transactions.storage.enable")),
			individualblocks.WithBalanceChanges(viper.GetBool("blocks.transactions.balances.enable")),
			individualblocks.WithTransactionEvents(viper.GetBool("blocks.transactions.events.enable")),
			individualblocks.WithInterval(viper.GetDuration("blocks.interval")),
		)
	case "batch":
		s, err = batchblocks.New(ctx,
			batchblocks.WithLogLevel(util.LogLevel("blocks")),
			batchblocks.WithMonitor(monitor),
			batchblocks.WithScheduler(scheduler),
			batchblocks.WithChainHeightProvider(chainHeightProvider),
			batchblocks.WithBlocksProvider(blocksProvider),
			batchblocks.WithBlockReplaysProvider(blockReplaysProvider),
			batchblocks.WithIssuanceProvider(issuanceProvider),
			batchblocks.WithTransactionReceiptsProvider(transactionReceiptsProvider),
			batchblocks.WithBlocksSetter(blocksSetter),
			batchblocks.WithTransactionsSetter(transactionsSetter),
			batchblocks.WithTransactionStateDiffsSetter(transactionStateDiffsSetter),
			batchblocks.WithEventsSetter(eventsSetter),
			batchblocks.WithStartHeight(viper.GetInt64("blocks.start-height")),
			batchblocks.WithTransactions(viper.GetBool("blocks.transactions.enable")),
			batchblocks.WithStorageChanges(viper.GetBool("blocks.transactions.storage.enable")),
			batchblocks.WithBalanceChanges(viper.GetBool("blocks.transactions.balances.enable")),
			batchblocks.WithTransactionEvents(viper.GetBool("blocks.transactions.events.enable")),
			batchblocks.WithProcessConcurrency(util.ProcessConcurrency("blocks")),
			batchblocks.WithInterval(viper.GetDuration("blocks.interval")),
		)
	default:
		return nil, errors.New("unknown blocks stylw")
	}
	if err != nil {
		return nil, errors.Wrap(err, "failed to create blocks service")
	}

	return s, nil
}

func startMEV(
	ctx context.Context,
	execDB execdb.Service,
	monitor metrics.Service,
) (
	blocks.Service,
	error,
) {
	if !viper.GetBool("mev.enable") {
		return nil, nil
	}

	scheduler, err := standardscheduler.New(ctx,
		standardscheduler.WithLogLevel(util.LogLevel("scheduler")),
		standardscheduler.WithMonitor(monitor),
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to start scheduler service")
	}

	blocksProvider, isProvider := execDB.(execdb.BlocksProvider)
	if !isProvider {
		return nil, errors.New("database does not provide blocks")
	}
	transactionsProvider, isProvider := execDB.(execdb.TransactionsProvider)
	if !isProvider {
		return nil, errors.New("database does not provide transactions")
	}
	transactionStateDiffsProvider, isProvider := execDB.(execdb.TransactionStateDiffsProvider)
	if !isProvider {
		return nil, errors.New("database does not provide transaction state diffs")
	}
	blockMEVsSetter, isSetter := execDB.(execdb.BlockMEVsSetter)
	if !isSetter {
		return nil, errors.New("database does not store MEV")
	}

	var s mev.Service

	s, err = batchmev.New(ctx,
		batchmev.WithLogLevel(util.LogLevel("mev")),
		batchmev.WithMonitor(monitor),
		batchmev.WithScheduler(scheduler),
		batchmev.WithBlocksProvider(blocksProvider),
		batchmev.WithTransactionsProvider(transactionsProvider),
		batchmev.WithTransactionStateDiffsProvider(transactionStateDiffsProvider),
		batchmev.WithBlockMEVsSetter(blockMEVsSetter),
		batchmev.WithStartHeight(viper.GetInt64("mev.start-height")),
		batchmev.WithProcessConcurrency(util.ProcessConcurrency("mev")),
		batchmev.WithInterval(viper.GetDuration("mev.interval")),
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create mev service")
	}

	return s, nil
}

func runCommands(ctx context.Context) {
	if viper.GetBool("version") {
		fmt.Printf("%s\n", ReleaseVersion)
		os.Exit(0)
	}
}
