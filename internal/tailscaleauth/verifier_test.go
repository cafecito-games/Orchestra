package tailscaleauth_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cafecito-games/Orchestra/internal/tailscaleauth"
)

func TestValidateListenAddress(t *testing.T) {
	selfIPs := []netip.Addr{
		netip.MustParseAddr("100.88.1.10"),
		netip.MustParseAddr("fd7a:115c:a1e0::10"),
	}

	require.NoError(t, tailscaleauth.ValidateListenAddress("100.88.1.10:8787", selfIPs))
	require.NoError(t, tailscaleauth.ValidateListenAddress("[fd7a:115c:a1e0::10]:8787", selfIPs))
	require.NoError(t, tailscaleauth.ValidateListenAddress("127.0.0.1:8787", selfIPs))
	require.NoError(t, tailscaleauth.ValidateListenAddress("[::1]:8787", selfIPs))
	require.NoError(t, tailscaleauth.ValidateListenAddress("localhost:8787", selfIPs))

	require.Error(t, tailscaleauth.ValidateListenAddress("0.0.0.0:8787", selfIPs))
	require.Error(t, tailscaleauth.ValidateListenAddress("[::]:8787", selfIPs))
	require.Error(t, tailscaleauth.ValidateListenAddress(":8787", selfIPs))
	require.Error(t, tailscaleauth.ValidateListenAddress("192.168.1.50:8787", selfIPs))
	require.Error(t, tailscaleauth.ValidateListenAddress("100.88.1.99:8787", selfIPs))
	require.Error(t, tailscaleauth.ValidateListenAddress("100.100.100.100:8787", nil))
}

func TestVerifierDefaultSameOwnerAndTaggedDevicesPolicy(t *testing.T) {
	const statusJSON = `{
		"BackendState": "Running",
		"MagicDNSSuffix": "tail999.ts.net",
		"Self": {
			"HostName": "studio-mac",
			"DNSName": "studio-mac.tail999.ts.net.",
			"UserID": 42,
			"TailscaleIPs": ["100.88.1.10", "fd7a:115c:a1e0::10"]
		},
		"User": {
			"42": {
				"ID": 42,
				"LoginName": "christian@example.com",
				"DisplayName": "Christian"
			}
		}
	}`

	var whoisCalls atomic.Int32
	fakeRunner := func(_ context.Context, _ string, arguments ...string) ([]byte, error) {
		if len(arguments) >= 2 && arguments[0] == "status" {
			return []byte(statusJSON), nil
		}
		if len(arguments) >= 3 && arguments[0] == "whois" {
			whoisCalls.Add(1)
			target := arguments[2]
			switch target {
			case "100.88.1.25:51000", "100.88.1.25:51001":
				return []byte(`{
					"Node": {
						"Name": "linux-worker.tail999.ts.net.",
						"Addresses": ["100.88.1.25/32"]
					},
					"UserProfile": {
						"ID": 42,
						"LoginName": "christian@example.com"
					}
				}`), nil
			case "100.88.1.30:52000":
				return []byte(`{
					"Node": {
						"Name": "gpu-box.tail999.ts.net.",
						"Addresses": ["100.88.1.30/32"],
						"Tags": ["tag:worker"]
					},
					"UserProfile": {
						"ID": 999,
						"LoginName": "tagged-devices"
					}
				}`), nil
			case "100.88.1.40:53000":
				return []byte(`{
					"Node": {
						"Name": "coworker-mac.tail999.ts.net.",
						"Addresses": ["100.88.1.40/32"]
					},
					"UserProfile": {
						"ID": 77,
						"LoginName": "other@example.com"
					}
				}`), nil
			case "100.88.1.50:54000":
				return []byte(`{
					"Node": {
						"Name": "shared-box.other-tailnet.ts.net.",
						"Addresses": ["100.88.1.50/32"],
						"Tags": ["tag:worker"]
					},
					"UserProfile": {
						"ID": 999,
						"LoginName": "tagged-devices"
					}
				}`), nil
			}
		}
		return nil, errors.New("unexpected command")
	}

	currentTime := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	verifier, err := tailscaleauth.NewVerifier(context.Background(), tailscaleauth.Options{
		CacheTTL:      30 * time.Second,
		Now:           func() time.Time { return currentTime },
		CommandRunner: fakeRunner,
	})
	require.NoError(t, err)
	require.Equal(t, "100.88.1.10", verifier.Self().PrimaryIPv4)
	require.Equal(t, "christian@example.com", verifier.Self().LoginName)

	// Loopback and Self IP succeed without spawning whois.
	loopbackRequest := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "http://127.0.0.1:8787/api/status", nil)
	loopbackRequest.RemoteAddr = "127.0.0.1:50000"
	peer, err := verifier.AuthorizeHTTPRequest(loopbackRequest)
	require.NoError(t, err)
	require.True(t, peer.IsLoopback)

	selfRequest := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "http://100.88.1.10:8787/api/status", nil)
	selfRequest.RemoteAddr = "100.88.1.10:50001"
	peer, err = verifier.AuthorizeHTTPRequest(selfRequest)
	require.NoError(t, err)
	require.True(t, peer.IsSelf)
	require.Equal(t, int32(0), whoisCalls.Load())

	// Same-owner remote slave node succeeds and caches by peer IP.
	slaveRequest1 := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "http://100.88.1.10:8787/control-agent", nil)
	slaveRequest1.RemoteAddr = "100.88.1.25:51000"
	peer, err = verifier.AuthorizeHTTPRequest(slaveRequest1)
	require.NoError(t, err)
	require.Equal(t, "linux-worker.tail999.ts.net", peer.NodeName)
	require.Equal(t, int32(1), whoisCalls.Load())

	slaveRequest2 := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "http://100.88.1.10:8787/api/status", nil)
	slaveRequest2.RemoteAddr = "100.88.1.25:51001"
	_, err = verifier.AuthorizeHTTPRequest(slaveRequest2)
	require.NoError(t, err)
	require.Equal(t, int32(1), whoisCalls.Load())

	// Tagged node on the same tailnet succeeds.
	taggedRequest := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "http://100.88.1.10:8787/control-agent", nil)
	taggedRequest.RemoteAddr = "100.88.1.30:52000"
	_, err = verifier.AuthorizeHTTPRequest(taggedRequest)
	require.NoError(t, err)

	// Different user is rejected by default.
	otherUserRequest := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "http://100.88.1.10:8787/control-agent", nil)
	otherUserRequest.RemoteAddr = "100.88.1.40:53000"
	_, err = verifier.AuthorizeHTTPRequest(otherUserRequest)
	require.Error(t, err)
	require.Contains(t, err.Error(), "other@example.com")

	// External tailnet tagged node is rejected.
	foreignTaggedRequest := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "http://100.88.1.10:8787/control-agent", nil)
	foreignTaggedRequest.RemoteAddr = "100.88.1.50:54000"
	_, err = verifier.AuthorizeHTTPRequest(foreignTaggedRequest)
	require.Error(t, err)

	// Browser requests (Origin / Sec-Fetch-*) are rejected even from loopback or Self IP.
	browserOriginRequest := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "http://100.88.1.10:8787/mcp", nil)
	browserOriginRequest.RemoteAddr = "127.0.0.1:55000"
	browserOriginRequest.Header.Set("Origin", "https://evil.example")
	_, err = verifier.AuthorizeHTTPRequest(browserOriginRequest)
	require.Error(t, err)
	require.Contains(t, err.Error(), "browser-origin")

	browserFetchRequest := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "http://100.88.1.10:8787/api/status", nil)
	browserFetchRequest.RemoteAddr = "100.88.1.10:55001"
	browserFetchRequest.Header.Set("Sec-Fetch-Site", "cross-site")
	_, err = verifier.AuthorizeHTTPRequest(browserFetchRequest)
	require.Error(t, err)

	// Funnel or reverse-proxy forwarded requests are rejected even from loopback.
	funnelRequest := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "http://127.0.0.1:8787/api/status", nil)
	funnelRequest.RemoteAddr = "127.0.0.1:55003"
	funnelRequest.Header.Set("Tailscale-Funnel-Request", "?1")
	_, err = verifier.AuthorizeHTTPRequest(funnelRequest)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Funnel")

	forwardedRequest := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "http://127.0.0.1:8787/api/status", nil)
	forwardedRequest.RemoteAddr = "127.0.0.1:55004"
	forwardedRequest.Header.Set("X-Forwarded-For", "203.0.113.10")
	_, err = verifier.AuthorizeHTTPRequest(forwardedRequest)
	require.Error(t, err)

	// Non-Tailscale LAN IP is rejected.
	lanRequest := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "http://100.88.1.10:8787/api/status", nil)
	lanRequest.RemoteAddr = "192.168.1.80:55002"
	_, err = verifier.AuthorizeHTTPRequest(lanRequest)
	require.Error(t, err)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (transport roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return transport(request)
}

func TestDiscoverMastersFiltersPeersAndProbesHealthz(t *testing.T) {
	statusJSON := `{
		"BackendState": "Running",
		"MagicDNSSuffix": "tail999.ts.net",
		"Self": {
			"HostName": "worker-box",
			"DNSName": "worker-box.tail999.ts.net.",
			"UserID": 42,
			"TailscaleIPs": ["100.88.1.5"]
		},
		"Peer": {
			"node-z": {
				"HostName": "zeta-server",
				"DNSName": "zeta-server.tail999.ts.net.",
				"UserID": 42,
				"TailscaleIPs": ["100.88.1.20"],
				"Online": true
			},
			"node-a": {
				"HostName": "alpha-mac",
				"DNSName": "alpha-mac.tail999.ts.net.",
				"UserID": 42,
				"TailscaleIPs": ["100.88.1.10"],
				"Online": true
			},
			"node-other-app": {
				"HostName": "metrics-host",
				"DNSName": "metrics-host.tail999.ts.net.",
				"UserID": 42,
				"TailscaleIPs": ["100.88.1.30"],
				"Online": true
			},
			"node-offline": {
				"HostName": "sleeping-laptop",
				"DNSName": "sleeping-laptop.tail999.ts.net.",
				"UserID": 42,
				"TailscaleIPs": ["100.88.1.40"],
				"Online": false
			},
			"node-mullvad": {
				"HostName": "us-nyc-wg-001",
				"DNSName": "us-nyc-wg-001.mullvad.ts.net.",
				"UserID": 9999,
				"TailscaleIPs": ["100.88.1.50"],
				"Online": true
			}
		}
	}`

	var probedMutex sync.Mutex
	probedHosts := map[string]bool{}
	httpClient := &http.Client{
		Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			probedMutex.Lock()
			probedHosts[request.URL.Host] = true
			probedMutex.Unlock()

			switch request.URL.String() {
			case "http://100.88.1.10:8787/healthz":
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader(`{"ok":true,"service":"orchestra-master","protocolVersion":"1","hostName":"alpha-mac","authMode":"tailscale"}`)),
				}, nil
			case "http://100.88.1.20:8787/healthz":
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader(`{"ok":true,"service":"orchestra-master","protocolVersion":"1","hostName":"zeta-server","authMode":"tailscale"}`)),
				}, nil
			case "http://100.88.1.30:8787/healthz":
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader(`{"ok":true,"service":"unrelated-daemon","protocolVersion":"1"}`)),
				}, nil
			default:
				return nil, errors.New("connection refused")
			}
		}),
	}

	result, err := tailscaleauth.DiscoverMasters(context.Background(), tailscaleauth.DiscoverOptions{
		HTTPClient: httpClient,
		CommandRunner: func(_ context.Context, _ string, arguments ...string) ([]byte, error) {
			if len(arguments) >= 2 && arguments[0] == "status" {
				return []byte(statusJSON), nil
			}
			return nil, errors.New("unexpected command")
		},
	})
	require.NoError(t, err)
	require.True(t, result.TailscaleAvailable)
	require.Equal(t, 4, result.OnlinePeers) // Self + alpha-mac + zeta-server + metrics-host
	require.Len(t, result.Masters, 2)
	require.Equal(t, "alpha-mac", result.Masters[0].HostName)
	require.Equal(t, "http://100.88.1.10:8787", result.Masters[0].Endpoint)
	require.Equal(t, "zeta-server", result.Masters[1].HostName)
	require.Equal(t, "http://100.88.1.20:8787", result.Masters[1].Endpoint)

	// Offline and Mullvad peers must never be probed.
	require.False(t, probedHosts["100.88.1.40:8787"])
	require.False(t, probedHosts["100.88.1.50:8787"])
}
