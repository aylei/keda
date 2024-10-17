package scalers

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-sql-driver/mysql"
	"github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	"github.com/kedacore/keda/v2/pkg/activate"
	v2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/metrics/pkg/apis/external_metrics"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// cnCPU is the total CPU usage of the CNSet
	cnCPU = "min_cpu"

	// cnConnections is the total connection count of the CNSet
	cnConnections = "connections"

	// connections from pod, use this metric before cnConnections get stable
	cnConnectionsFromPod = "connections_from_pod"

	dbName = "system_metrics"
)

type matrixoneScaler struct {
	metricType v2.MetricTargetType
	metadata   *matrixoneMetadata
	connection *sql.DB
	logger     logr.Logger
	kubeCli    client.Client

	cfg *ScalerConfig
}

type matrixoneMetadata struct {
	username    string
	host        string
	port        string
	password    string
	accountId   string
	metric      string
	targetValue float64
	selector    string

	cnSetKey string

	window time.Duration
}

// NewMatrixoneScaler creates a new MatrixOne scaler
func NewMatrixoneScaler(kubeCli client.Client, config *ScalerConfig) (Scaler, error) {
	logger := InitializeLogger(config, "mo_scaler")

	meta, err := parseMatrixoneMetadata(kubeCli, config)
	if err != nil {
		return nil, fmt.Errorf("error parsing MatrixOne metadata: %w", err)
	}

	metricType := v2.AverageValueMetricType
	metric, ok := config.TriggerMetadata["metric"]
	if ok {
		switch metric {
		case cnCPU, cnConnections, cnConnectionsFromPod:
			metricType = v2.AverageValueMetricType
		default:
			return nil, fmt.Errorf("metric %s is not supported", metric)
		}
		meta.metric = metric
	} else {
		return nil, fmt.Errorf("no metric given")
	}

	ms := &matrixoneScaler{
		kubeCli:    kubeCli,
		metricType: metricType,
		metadata:   meta,
		logger:     logger,
		cfg:        config,
	}
	// TODO: only initiate connection when getting metrics and handle failed scenario
	if metric == cnConnections || metric == cnCPU {
		conn, err := newMOConnection(meta, logger)
		if err != nil {
			return nil, fmt.Errorf("error establishing MO connection: %w", err)
		}
		ms.connection = conn
	}
	return ms, nil
}

func parseMatrixoneMetadata(kubeCli client.Client, config *ScalerConfig) (*matrixoneMetadata, error) {
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
	meta.accountId = config.TriggerMetadata["accountId"]
	meta.selector = config.TriggerMetadata["selector"]

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

	secretNs, err := GetFromAuthOrMeta(config, "secretNamespace")
	if err != nil {
		secretNs = config.ScalableObjectNamespace
	}
	secret, err := GetFromAuthOrMeta(config, "secretName")
	if err != nil {
		return nil, err
	}
	sec := &corev1.Secret{}
	err = kubeCli.Get(context.Background(), client.ObjectKey{Namespace: secretNs, Name: secret}, sec)
	if err != nil {
		return nil, err
	}
	pwd, ok := sec.Data["password"]
	if !ok {
		return nil, errors.New("MO secret does not have password encoded")
	}
	meta.password = string(pwd)
	usn, ok := sec.Data["username"]
	if !ok {
		return nil, errors.New("MO secret does not have username encoded")
	}
	meta.username = string(usn)
	meta.window = 2 * time.Minute

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

// newMOConnection creates MO db connection
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

// Close disposes of MO connections
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
			Name: s.metadata.metric,
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
	switch s.metadata.metric {
	case cnCPU:
		return s.getCPU(ctx, metricName)
	case cnConnections:
		return s.getConnections(ctx, metricName)
	case cnConnectionsFromPod:
		return s.getConnectionsFromPod(ctx, metricName)
	default:
		return nil, false, fmt.Errorf("metric %s is not supported", s.metadata.metric)
	}
}

func (s *matrixoneScaler) Run(ctx context.Context, active chan<- bool) {
	activate.DefaultRegistry.Regist(v1alpha1.GenerateIdentifier(s.cfg.ScalableObjectType, s.cfg.ScalableObjectNamespace, s.cfg.ScalableObjectName), active)
}

func (s *matrixoneScaler) getConnectionsFromPod(ctx context.Context, metricName string) ([]external_metrics.ExternalMetricValue, bool, error) {
	podList := &corev1.PodList{}
	if s.metadata.selector == "" {
		// legacy logic
		err := s.kubeCli.List(ctx, podList, client.InNamespace(s.cfg.ScalableObjectNamespace), client.MatchingLabels(map[string]string{
			"matrixorigin.io/component": "CNSet",
			"matrixorigin.io/instance":  s.cfg.ScalableObjectName,
		}))
		if err != nil {
			return nil, false, err
		}
	} else {
		ls, err := metav1.ParseToLabelSelector(s.metadata.selector)
		if err != nil {
			return nil, false, err
		}
		selector, err := metav1.LabelSelectorAsSelector(ls)
		if err != nil {
			return nil, false, err
		}
		err = s.kubeCli.List(ctx, podList, client.InNamespace(s.cfg.ScalableObjectNamespace), client.MatchingLabelsSelector{
			Selector: selector,
		})
		if err != nil {
			return nil, false, err
		}
	}

	var total int
	for _, pod := range podList.Items {
		c, ok := pod.Annotations["matrixorigin.io/connections"]
		if !ok {
			// dummy count
			total += 1
			continue
		}
		count, err := strconv.Atoi(c)
		if err != nil {
			total += 1
			continue
		}
		total += count
	}
	var isActive bool
	if total > 0 {
		isActive = true
	}
	return []external_metrics.ExternalMetricValue{GenerateMetricInMili(metricName, float64(total))}, isActive, nil
}

func (s *matrixoneScaler) getConnections(ctx context.Context, metricName string) ([]external_metrics.ExternalMetricValue, bool, error) {
	timeStart := time.Now().Add(-s.metadata.window)
	query := fmt.Sprintf(`
SELECT max(value), node
FROM system_metrics.server_connections 
WHERE collecttime >= '%s'
`, timeStart.Format("2006-01-02 15:04:05"))
	if s.metadata.accountId != "" {
		query += fmt.Sprintf("\nAND account='%s'", s.metadata.accountId)
	}
	query += "\nGROUP BY node"
	rows, err := s.connection.QueryContext(ctx, query)
	if err != nil {
		return []external_metrics.ExternalMetricValue{}, false, fmt.Errorf("error query MO: %w", err)
	}
	defer rows.Close()
	var total int
	for rows.Next() {
		var v int
		var node string
		if err := rows.Scan(&v, &node); err != nil {
			return nil, false, err
		}
		total += v
	}
	isActive := false
	if total > 0 {
		isActive = true
	}
	return []external_metrics.ExternalMetricValue{GenerateMetricInMili(metricName, float64(total))}, isActive, nil
}

func (s *matrixoneScaler) getCPU(ctx context.Context, metricName string) ([]external_metrics.ExternalMetricValue, bool, error) {

	timeStart := time.Now().Add(-s.metadata.window)
	query := fmt.Sprintf(`
SELECT node, (max(value) - min(value)) / ((max(collecttime) - min(collecttime)) + 0.001) as usage
FROM process_cpu_seconds_total
WHERE role="CN"
AND collecttime >= '%s'
`, timeStart)
	if s.metadata.accountId != "" {
		query += fmt.Sprintf("\nAND account='%s'", s.metadata.accountId)
	}
	query += "\nGROUP BY node"
	rows, err := s.connection.QueryContext(ctx, query)
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
	metric := GenerateMetricInMili(metricName, num)
	// we cannot scale to zero based on CPU metric so this trigger should always be active
	isActive := true

	return []external_metrics.ExternalMetricValue{metric}, isActive, nil
}
