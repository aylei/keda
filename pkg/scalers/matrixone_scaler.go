package scalers

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/go-logr/logr"
	"github.com/go-sql-driver/mysql"
	v2 "k8s.io/api/autoscaling/v2"
	"k8s.io/metrics/pkg/apis/external_metrics"
	"net"
	"strconv"
	"time"
)

const (
	// cnset load score is the load score calculated for the CNSet, normalized by 1
	cnLoadScore = "cnset_load_score"

	dbName = "system_metrics"
)

type matrixoneScaler struct {
	metricType v2.MetricTargetType
	metadata   *matrixoneMetadata
	connection *sql.DB
	logger     logr.Logger
}

type matrixoneMetadata struct {
	username    string
	password    string
	host        string
	port        string
	targetValue float64
	window      time.Duration
}

// NewMatrixoneScaler creates a new MatrixOne scaler
func NewMatrixoneScaler(config *ScalerConfig) (Scaler, error) {
	metricType, err := GetMetricTargetType(config)
	if err != nil {
		return nil, fmt.Errorf("error getting scaler metric type: %w", err)
	}

	logger := InitializeLogger(config, "mo_scaler")

	meta, err := parseMatrixoneMetadata(config)
	if err != nil {
		return nil, fmt.Errorf("error parsing MySQL metadata: %w", err)
	}

	conn, err := newMOConnection(meta, logger)
	if err != nil {
		return nil, fmt.Errorf("error establishing MySQL connection: %w", err)
	}
	return &matrixoneScaler{
		metricType: metricType,
		metadata:   meta,
		connection: conn,
		logger:     logger,
	}, nil
}

func parseMatrixoneMetadata(config *ScalerConfig) (*matrixoneMetadata, error) {
	meta := matrixoneMetadata{}

	if val, ok := config.TriggerMetadata["targetValue"]; ok {
		tv, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return nil, fmt.Errorf("targetValue parsing error %w", err)
		}
		meta.targetValue = tv
	} else {
		return nil, fmt.Errorf("no targetValue given")
	}

	var err error
	host, err := GetFromAuthOrMeta(config, "host")
	if err != nil {
		return nil, err
	}
	meta.host = host

	port, err := GetFromAuthOrMeta(config, "port")
	if err != nil {
		return nil, err
	}
	meta.port = port

	username, err := GetFromAuthOrMeta(config, "username")
	if err != nil {
		return nil, err
	}
	meta.username = username

	if config.AuthParams["password"] != "" {
		meta.password = config.AuthParams["password"]
	} else if config.TriggerMetadata["passwordFromEnv"] != "" {
		meta.password = config.ResolvedEnv[config.TriggerMetadata["passwordFromEnv"]]
	}

	if len(meta.password) == 0 {
		return nil, fmt.Errorf("no password given")
	}

	return &meta, nil
}

// metadataToConnectionStr builds new MO connection string
func (meta *matrixoneMetadata) toConnectionStr() string {
	// Build connection str
	config := mysql.NewConfig()
	config.Addr = net.JoinHostPort(meta.host, meta.port)
	config.DBName = dbName
	config.Passwd = meta.password
	config.User = meta.username
	config.Net = "tcp"
	return config.FormatDSN()
}

// newMOConnection creates MySQL db connection
func newMOConnection(meta *matrixoneMetadata, logger logr.Logger) (*sql.DB, error) {
	connStr := meta.toConnectionStr()
	db, err := sql.Open("mysql", connStr)
	if err != nil {
		logger.Error(err, fmt.Sprintf("Found error when opening connection: %s", err))
		return nil, err
	}
	err = db.Ping()
	if err != nil {
		logger.Error(err, fmt.Sprintf("Found error when pinging database: %s", err))
		return nil, err
	}
	return db, nil
}

// Close disposes of MySQL connections
func (s *matrixoneScaler) Close(context.Context) error {
	err := s.connection.Close()
	if err != nil {
		s.logger.Error(err, "Error closing MO connection")
		return err
	}
	return nil
}

// GetMetricSpecForScaling returns the MetricSpec for the Horizontal Pod Autoscaler
func (s *matrixoneScaler) GetMetricSpecForScaling(context.Context) []v2.MetricSpec {
	externalMetric := &v2.ExternalMetricSource{
		Metric: v2.MetricIdentifier{
			Name: cnLoadScore,
		},
		Target: GetMetricTargetMili(s.metricType, s.metadata.targetValue),
	}
	metricSpec := v2.MetricSpec{
		External: externalMetric, Type: externalMetricType,
	}
	return []v2.MetricSpec{metricSpec}
}

// GetMetricsAndActivity returns value for a supported metric and an error if there is a problem getting the metric
func (s *matrixoneScaler) GetMetricsAndActivity(ctx context.Context, metricName string) ([]external_metrics.ExternalMetricValue, bool, error) {
	switch metricName {
	case cnLoadScore:
		return s.getCNLoadScore(ctx)
	default:
		return nil, false, fmt.Errorf("metric %s not supported", cnLoadScore)
	}
}

func (s *matrixoneScaler) getCNLoadScore(ctx context.Context) ([]external_metrics.ExternalMetricValue, bool, error) {

	timeStart := time.Now().Add(-s.metadata.window)
	// TODO: integrate with CNSet node ranges
	rows, err := s.connection.QueryContext(ctx, `
SELECT node, (max(value) - min(value)) / ((max(collecttime) - min(collecttime)) + 0.001) as usage
FROM process_cpu_seconds_total
WHERE role="CN"
AND collecttime >= ?
`, timeStart)
	if err != nil {
		return []external_metrics.ExternalMetricValue{}, false, fmt.Errorf("error query MO: %w", err)
	}
	defer rows.Close()
	count := 0
	var total float64
	for rows.Next() {
		var score float64
		var node string
		count += 1
		if err := rows.Scan(&node, &score); err != nil {
			return nil, false, err
		}
		total += score
	}
	num := 0.0
	if count > 0 {
		num = total / float64(count)
	}
	metric := GenerateMetricInMili(cnLoadScore, num)

	return []external_metrics.ExternalMetricValue{metric}, true, nil
}
