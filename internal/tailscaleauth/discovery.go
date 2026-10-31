package tailscaleauth

import (
	"cmp"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	DefaultMasterPort         = 8787
	defaultDiscoveryProbeTime = 600 * time.Millisecond
	maximumDiscoveryWorkers   = 16
)

type MasterHealth struct {
	OK              bool   `json:"ok"`
	Service         string `json:"service,omitempty"`
	Version         string `json:"version,omitempty"`
	ProtocolVersion string `json:"protocolVersion"`
	HostName        string `json:"hostName,omitempty"`
	AuthMode        string `json:"authMode,omitempty"`
}

type DiscoveredMaster struct {
	Endpoint        string `json:"endpoint"`
	Address         string `json:"address"`
	HostName        string `json:"hostName"`
	DNSName         string `json:"dnsName,omitempty"`
	Version         string `json:"version,omitempty"`
	ProtocolVersion string `json:"protocolVersion"`
	AuthMode        string `json:"authMode,omitempty"`
}

type DiscoveryResult struct {
	TailscaleAvailable bool               `json:"tailscaleAvailable"`
	OnlinePeers        int                `json:"onlinePeers"`
	Masters            []DiscoveredMaster `json:"masters"`
}

type DiscoverOptions struct {
	BinaryPath    string
	Port          int
	PreferTLS     bool
	HTTPClient    *http.Client
	ProbeTimeout  time.Duration
	LookPath      func(string) (string, error)
	CommandRunner CommandRunner
}

type candidatePeer struct {
	address  netip.Addr
	hostName string
	dnsName  string
}

func ProbeMasterHealth(
	operationContext context.Context,
	baseEndpoint string,
	httpClient *http.Client,
	timeout time.Duration,
) (MasterHealth, bool) {
	normalized := normalizeHTTPBaseEndpoint(baseEndpoint)
	if normalized == "" {
		return MasterHealth{}, false
	}
	if timeout <= 0 {
		timeout = defaultDiscoveryProbeTime
	}
	if httpClient == nil {
		httpClient = http.DefaultClient
	}

	probeContext, cancel := context.WithTimeout(operationContext, timeout)
	defer cancel()

	request, err := http.NewRequestWithContext(probeContext, http.MethodGet, normalized+"/healthz", nil)
	if err != nil {
		return MasterHealth{}, false
	}
	request.Header.Set("Accept", "application/json")

	response, err := httpClient.Do(request)
	if err != nil {
		return MasterHealth{}, false
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return MasterHealth{}, false
	}

	bodyBytes, err := io.ReadAll(io.LimitReader(response.Body, 64*1024))
	if err != nil {
		return MasterHealth{}, false
	}

	var health MasterHealth
	if err := json.Unmarshal(bodyBytes, &health); err != nil {
		return MasterHealth{}, false
	}
	if !health.OK || strings.TrimSpace(health.ProtocolVersion) == "" {
		return MasterHealth{}, false
	}
	if health.Service != "" && health.Service != "orchestra-master" {
		return MasterHealth{}, false
	}
	return health, true
}

func DiscoverMasters(operationContext context.Context, options DiscoverOptions) (DiscoveryResult, error) {
	runCommand := options.CommandRunner
	if runCommand == nil {
		runCommand = defaultCommandRunner
	}

	binaryPath := strings.TrimSpace(options.BinaryPath)
	if binaryPath == "" {
		if options.CommandRunner != nil {
			binaryPath = "tailscale"
		} else {
			discoveredBinary, err := FindTailscaleBinary(options.LookPath)
			if err != nil {
				return DiscoveryResult{TailscaleAvailable: false}, nil
			}
			binaryPath = discoveredBinary
		}
	}

	statusBytes, err := runCommand(operationContext, binaryPath, "status", "--json")
	if err != nil {
		return DiscoveryResult{TailscaleAvailable: false}, nil
	}

	var status statusJSONPayload
	if err := json.Unmarshal(statusBytes, &status); err != nil {
		return DiscoveryResult{TailscaleAvailable: false}, nil
	}
	if status.BackendState != "" && status.BackendState != "Running" {
		return DiscoveryResult{TailscaleAvailable: false}, nil
	}

	suffix := strings.Trim(strings.TrimSpace(status.MagicDNSSuffix), ".")
	if suffix == "" && status.CurrentTailnet != nil {
		suffix = strings.Trim(strings.TrimSpace(status.CurrentTailnet.MagicDNSSuffix), ".")
	}
	var selfUserID int64
	if status.Self != nil {
		selfUserID = status.Self.UserID
	}

	seenAddresses := map[netip.Addr]bool{}
	var candidates []candidatePeer
	addCandidate := func(node statusNodeJSON, isSelf bool) {
		cleanDNS := strings.TrimSuffix(strings.TrimSpace(node.DNSName), ".")
		lowerDNS := strings.ToLower(cleanDNS)
		if strings.HasSuffix(lowerDNS, ".mullvad.ts.net") || slices.Contains(node.Tags, "tag:mullvad-exit-node") {
			return
		}
		if !isSelf {
			if !node.Online {
				return
			}
			if suffix != "" && !nodeMatchesTailnet(cleanDNS, suffix) && (selfUserID == 0 || node.UserID != selfUserID) {
				return
			}
		}

		chosenAddr, ok := selectPreferredPeerAddress(node.TailscaleIPs)
		if !ok || seenAddresses[chosenAddr] {
			return
		}
		seenAddresses[chosenAddr] = true
		candidates = append(candidates, candidatePeer{
			address:  chosenAddr,
			hostName: strings.TrimSpace(node.HostName),
			dnsName:  cleanDNS,
		})
	}

	if status.Self != nil {
		selfNode := *status.Self
		if len(selfNode.TailscaleIPs) == 0 {
			selfNode.TailscaleIPs = status.TailscaleIPs
		}
		addCandidate(selfNode, true)
	}

	peerKeys := make([]string, 0, len(status.Peer))
	for key := range status.Peer {
		peerKeys = append(peerKeys, key)
	}
	slices.Sort(peerKeys)
	for _, key := range peerKeys {
		addCandidate(status.Peer[key], false)
	}

	port := options.Port
	if port <= 0 {
		port = DefaultMasterPort
	}
	timeout := options.ProbeTimeout
	if timeout <= 0 {
		timeout = defaultDiscoveryProbeTime
	}

	schemes := []string{"http", "https"}
	if options.PreferTLS {
		schemes = []string{"https", "http"}
	}

	var resultsMutex sync.Mutex
	var discovered []DiscoveredMaster
	semaphore := make(chan struct{}, maximumDiscoveryWorkers)
	var waitGroup sync.WaitGroup

	for _, candidate := range candidates {
		waitGroup.Add(1)
		go func(peer candidatePeer) {
			defer waitGroup.Done()
			select {
			case semaphore <- struct{}{}:
				defer func() { <-semaphore }()
			case <-operationContext.Done():
				return
			}

			hostPort := net.JoinHostPort(peer.address.String(), strconv.Itoa(port))
			for _, scheme := range schemes {
				if operationContext.Err() != nil {
					return
				}
				endpoint := scheme + "://" + hostPort
				health, ok := ProbeMasterHealth(operationContext, endpoint, options.HTTPClient, timeout)
				if !ok {
					continue
				}
				hostName := peer.hostName
				if hostName == "" {
					hostName = strings.TrimSpace(health.HostName)
				}
				if hostName == "" {
					hostName = peer.dnsName
				}
				resultsMutex.Lock()
				discovered = append(discovered, DiscoveredMaster{
					Endpoint:        endpoint,
					Address:         peer.address.String(),
					HostName:        hostName,
					DNSName:         peer.dnsName,
					Version:         strings.TrimSpace(health.Version),
					ProtocolVersion: strings.TrimSpace(health.ProtocolVersion),
					AuthMode:        strings.TrimSpace(health.AuthMode),
				})
				resultsMutex.Unlock()
				return
			}
		}(candidate)
	}
	waitGroup.Wait()

	slices.SortFunc(discovered, func(left, right DiscoveredMaster) int {
		if delta := cmp.Compare(strings.ToLower(left.HostName), strings.ToLower(right.HostName)); delta != 0 {
			return delta
		}
		if delta := cmp.Compare(strings.ToLower(left.DNSName), strings.ToLower(right.DNSName)); delta != 0 {
			return delta
		}
		return cmp.Compare(left.Endpoint, right.Endpoint)
	})

	return DiscoveryResult{
		TailscaleAvailable: true,
		OnlinePeers:        len(candidates),
		Masters:            discovered,
	}, nil
}

func selectPreferredPeerAddress(rawAddresses []string) (netip.Addr, bool) {
	var fallbackIPv6 netip.Addr
	hasFallback := false
	for _, rawAddress := range rawAddresses {
		parsed, err := netip.ParseAddr(strings.TrimSpace(rawAddress))
		if err != nil {
			continue
		}
		unmapped := parsed.Unmap()
		if !IsTailscaleAddr(unmapped) && !unmapped.IsLoopback() {
			continue
		}
		if unmapped.Is4() {
			return unmapped, true
		}
		if !hasFallback {
			fallbackIPv6 = unmapped
			hasFallback = true
		}
	}
	return fallbackIPv6, hasFallback
}

func normalizeHTTPBaseEndpoint(rawEndpoint string) string {
	trimmed := strings.TrimRight(strings.TrimSpace(rawEndpoint), "/")
	switch {
	case strings.HasPrefix(trimmed, "ws://"):
		return "http://" + strings.TrimPrefix(trimmed, "ws://")
	case strings.HasPrefix(trimmed, "wss://"):
		return "https://" + strings.TrimPrefix(trimmed, "wss://")
	case strings.HasPrefix(trimmed, "http://"), strings.HasPrefix(trimmed, "https://"):
		return trimmed
	default:
		return ""
	}
}
