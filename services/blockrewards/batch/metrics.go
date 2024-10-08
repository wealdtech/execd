// Copyright © 2021, 2022 Weald Technology Limited.
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

package batch

import (
	"context"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/wealdtech/execd/services/metrics"
)

var metricsNamespace = "execd"

var (
	latestBlock     prometheus.Gauge
	blocksProcessed prometheus.Gauge
)

func registerMetrics(ctx context.Context, monitor metrics.Service) error {
	if latestBlock != nil {
		// Already registered.
		return nil
	}
	if monitor == nil {
		// No monitor.
		return nil
	}
	if monitor.Presenter() == "prometheus" {
		return registerPrometheusMetrics(ctx)
	}
	return nil
}

func registerPrometheusMetrics(_ context.Context) error {
	latestBlock = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: "rewards",
		Name:      "latest",
		Help:      "Latest block processed",
	})
	if err := prometheus.Register(latestBlock); err != nil {
		return errors.Wrap(err, "failed to register latest block")
	}

	blocksProcessed = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: "rewards",
		Name:      "processed",
		Help:      "Number of blocks processed",
	})
	if err := prometheus.Register(blocksProcessed); err != nil {
		return errors.Wrap(err, "failed to register blocks processed")
	}

	return nil
}

func monitorBlockProcessed(height uint32) {
	if blocksProcessed != nil {
		blocksProcessed.Inc()
		latestBlock.Set(float64(height))
	}
}
