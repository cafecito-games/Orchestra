// Package tailscaleauth verifies Tailscale peer identities via the local
// Tailscale daemon (`tailscale status --json` and `tailscale whois --json`),
// enforces Tailscale/loopback bind confinement, and blocks browser-origin
// requests to prevent cross-site request forgery and DNS rebinding.
package tailscaleauth

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"os"
	"os/exec"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	AuthModeToken     = "token"
	AuthModeTailscale = "tailscale"

	defaultCommandTimeout = 4 * time.Second
	defaultCacheTTL       = 30 * time.Second
	maximumCachedPeers    = 512
	taggedDevicesLogin    = "tagged-devices"
)

var (
	tailscaleCGNATPrefix = netip.MustParsePrefix("100.64.0.0/10")
	tailscaleULAPrefix   = netip.MustParsePrefix("fd7a:115c:a1e0::/48")
	tailscaleQuad100Addr = netip.MustParseAddr("100.100.100.100")
)

func ParseAuthMode(raw string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", AuthModeToken:
		return AuthModeToken, nil
	case AuthModeTailscale:
		return AuthModeTailscale, nil
	default:
		return "", fmt.Errorf("unknown auth mode %q (expected token or tailscale)", raw)
	}
}

func IsTailscaleAddr(address netip.Addr) bool {
	unmapped := address.Unmap()
	if !unmapped.IsValid() || unmapped == tailscaleQuad100Addr {
		return false
	}
	return tailscaleCGNATPrefix.Contains(unmapped) || tailscaleULAPrefix.Contains(unmapped)
}

// ValidateListenAddress ensures that a master running in tailscale auth mode
// never binds a wildcard (0.0.0.0 / ::) or non-Tailscale LAN interface.
func ValidateListenAddress(listenAddress string, selfIPs []netip.Addr) error {
	host, _, err := net.SplitHostPort(strings.TrimSpace(listenAddress))
	if err != nil {
		return fmt.Errorf("invalid listen address %q: %w", listenAddress, err)
	}
	trimmedHost := strings.TrimSpace(host)
	if trimmedHost == "" {
		return fmt.Errorf("--auth tailscale refuses wildcard listen address %q; bind to a Tailscale IP or 127.0.0.1", listenAddress)
	}
	if strings.EqualFold(trimmedHost, "localhost") {
		return nil
	}
	parsedAddr, parseErr := netip.ParseAddr(trimmedHost)
	if parseErr != nil {
		return fmt.Errorf("--auth tailscale requires an explicit Tailscale IP or loopback address in --listen (got %q)", listenAddress)
	}
	unmapped := parsedAddr.Unmap()
	if unmapped.IsUnspecified() {
		return fmt.Errorf("--auth tailscale refuses wildcard listen address %q; bind to a Tailscale IP or 127.0.0.1", listenAddress)
	}
	if unmapped.IsLoopback() {
		return nil
	}
	if !IsTailscaleAddr(unmapped) {
		return fmt.Errorf("--auth tailscale refuses non-Tailscale listen address %q; bind to a 100.64.0.0/10, fd7a:115c:a1e0::/48, or loopback IP", listenAddress)
	}
	if len(selfIPs) > 0 && !slices.Contains(selfIPs, unmapped) {
		return fmt.Errorf("listen address %s does not belong to this machine's active Tailscale node", unmapped.String())
	}
	return nil
}

// IsBrowserRequest detects browser-originated HTTP or WebSocket requests via
// Origin and Sec-Fetch-* headers so web pages cannot issue cross-site or
// DNS-rebound requests against a tokenless Tailscale listener.
func IsBrowserRequest(request *http.Request) bool {
	if request == nil {
		return false
	}
	if strings.TrimSpace(request.Header.Get("Origin")) != "" {
		return true
	}
	if strings.TrimSpace(request.Header.Get("Sec-Fetch-Site")) != "" {
		return true
	}
	if strings.TrimSpace(request.Header.Get("Sec-Fetch-Mode")) != "" {
		return true
	}
	return false
}

// IsProxiedOrFunnelRequest detects HTTP reverse-proxy or Tailscale Funnel
// forwarding headers so external traffic forwarded over loopback cannot impersonate
// a local operator or direct tailnet peer.
func IsProxiedOrFunnelRequest(request *http.Request) bool {
	if request == nil {
		return false
	}
	if strings.TrimSpace(request.Header.Get("Tailscale-Funnel-Request")) != "" {
		return true
	}
	if strings.TrimSpace(request.Header.Get("X-Forwarded-For")) != "" {
		return true
	}
	if strings.TrimSpace(request.Header.Get("Forwarded")) != "" {
		return true
	}
	return false
}

type CommandRunner func(operationContext context.Context, binary string, arguments ...string) ([]byte, error)

type SelfIdentity struct {
	HostName       string       `json:"hostName"`
	DNSName        string       `json:"dnsName"`
	MagicDNSSuffix string       `json:"magicDnsSuffix"`
	UserID         int64        `json:"userId,omitempty"`
	LoginName      string       `json:"loginName,omitempty"`
	Tags           []string     `json:"tags,omitempty"`
	TailscaleIPs   []netip.Addr `json:"tailscaleIps,omitempty"`
	PrimaryIPv4    string       `json:"primaryIpv4,omitempty"`
}

type PeerIdentity struct {
	NodeName   string       `json:"nodeName"`
	Addresses  []netip.Addr `json:"addresses,omitempty"`
	UserID     int64        `json:"userId,omitempty"`
	LoginName  string       `json:"loginName,omitempty"`
	Tags       []string     `json:"tags,omitempty"`
	IsLoopback bool         `json:"isLoopback,omitempty"`
	IsSelf     bool         `json:"isSelf,omitempty"`
}

type Options struct {
	BinaryPath    string
	AllowedUsers  []string
	AllowedTags   []string
	CacheTTL      time.Duration
	Now           func() time.Time
	LookPath      func(string) (string, error)
	CommandRunner CommandRunner
}

type cachedPeerEntry struct {
	peer      PeerIdentity
	expiresAt time.Time
}

type Verifier struct {
	binaryPath   string
	self         SelfIdentity
	allowedUsers []string
	allowedTags  []string
	cacheTTL     time.Duration
	now          func() time.Time
	runCommand   CommandRunner

	cacheMutex sync.Mutex
	peerCache  map[netip.Addr]cachedPeerEntry
}

func FindTailscaleBinary(lookPath func(string) (string, error)) (string, error) {
	if lookPath == nil {
		lookPath = exec.LookPath
	}
	if resolved, err := lookPath("tailscale"); err == nil && strings.TrimSpace(resolved) != "" {
		return resolved, nil
	}
	if runtime.GOOS == "darwin" {
		const macOSAppBinary = "/Applications/Tailscale.app/Contents/MacOS/Tailscale"
		if info, statErr := os.Stat(macOSAppBinary); statErr == nil && !info.IsDir() {
			return macOSAppBinary, nil
		}
	}
	return "", errors.New("tailscale CLI not found on PATH; install Tailscale or pass --tailscale-binary")
}

func defaultCommandRunner(operationContext context.Context, binary string, arguments ...string) ([]byte, error) {
	commandContext, cancel := context.WithTimeout(operationContext, defaultCommandTimeout)
	defer cancel()

	command := exec.CommandContext(commandContext, binary, arguments...)
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		detail := strings.TrimSpace(stderr.String())
		if detail == "" {
			detail = err.Error()
		}
		return nil, fmt.Errorf("%s %s: %s", binary, strings.Join(arguments, " "), detail)
	}
	return stdout.Bytes(), nil
}

func NewVerifier(operationContext context.Context, options Options) (*Verifier, error) {
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
				return nil, err
			}
			binaryPath = discoveredBinary
		}
	}

	allowedUsers, err := normalizeAllowedUsers(options.AllowedUsers)
	if err != nil {
		return nil, err
	}
	allowedTags, err := normalizeAllowedTags(options.AllowedTags)
	if err != nil {
		return nil, err
	}

	nowFunction := options.Now
	if nowFunction == nil {
		nowFunction = time.Now
	}
	cacheTTL := options.CacheTTL
	if cacheTTL <= 0 {
		cacheTTL = defaultCacheTTL
	}

	selfIdentity, err := loadSelfIdentity(operationContext, runCommand, binaryPath)
	if err != nil {
		return nil, err
	}

	return &Verifier{
		binaryPath:   binaryPath,
		self:         selfIdentity,
		allowedUsers: allowedUsers,
		allowedTags:  allowedTags,
		cacheTTL:     cacheTTL,
		now:          nowFunction,
		runCommand:   runCommand,
		peerCache:    map[netip.Addr]cachedPeerEntry{},
	}, nil
}

func (verifier *Verifier) Self() SelfIdentity {
	if verifier == nil {
		return SelfIdentity{}
	}
	return verifier.self
}

func (verifier *Verifier) AuthorizeHTTPRequest(request *http.Request) (PeerIdentity, error) {
	if verifier == nil {
		return PeerIdentity{}, errors.New("tailscale verifier is not initialized")
	}
	if IsBrowserRequest(request) {
		return PeerIdentity{}, errors.New("browser-origin requests are not permitted in tailscale auth mode")
	}
	if IsProxiedOrFunnelRequest(request) {
		return PeerIdentity{}, errors.New("proxied or Tailscale Funnel requests are not permitted in tailscale auth mode")
	}

	remoteHost, _, splitErr := net.SplitHostPort(strings.TrimSpace(request.RemoteAddr))
	if splitErr != nil {
		return PeerIdentity{}, fmt.Errorf("invalid remote address %q", request.RemoteAddr)
	}
	remoteAddr, parseErr := netip.ParseAddr(remoteHost)
	if parseErr != nil {
		return PeerIdentity{}, fmt.Errorf("invalid remote IP %q", remoteHost)
	}
	unmapped := remoteAddr.Unmap()

	if unmapped.IsLoopback() {
		return PeerIdentity{
			NodeName:   "loopback",
			Addresses:  []netip.Addr{unmapped},
			IsLoopback: true,
		}, nil
	}

	if slices.Contains(verifier.self.TailscaleIPs, unmapped) {
		return PeerIdentity{
			NodeName:  verifier.self.DNSName,
			Addresses: slices.Clone(verifier.self.TailscaleIPs),
			UserID:    verifier.self.UserID,
			LoginName: verifier.self.LoginName,
			Tags:      slices.Clone(verifier.self.Tags),
			IsSelf:    true,
		}, nil
	}

	if !IsTailscaleAddr(unmapped) {
		return PeerIdentity{}, fmt.Errorf("remote address %s is outside Tailscale CGNAT/ULA space", unmapped.String())
	}

	peer, err := verifier.lookupPeer(request.Context(), request.RemoteAddr, unmapped)
	if err != nil {
		return PeerIdentity{}, err
	}
	if err := verifier.evaluatePolicy(peer); err != nil {
		return peer, err
	}
	return peer, nil
}

func (verifier *Verifier) lookupPeer(
	operationContext context.Context,
	remoteHostPort string,
	remoteAddr netip.Addr,
) (PeerIdentity, error) {
	now := verifier.now()
	verifier.cacheMutex.Lock()
	if cached, exists := verifier.peerCache[remoteAddr]; exists && now.Before(cached.expiresAt) {
		verifier.cacheMutex.Unlock()
		return cached.peer, nil
	}
	verifier.cacheMutex.Unlock()

	output, err := verifier.runCommand(operationContext, verifier.binaryPath, "whois", "--json", remoteHostPort)
	if err != nil {
		return PeerIdentity{}, fmt.Errorf("tailscale whois failed for %s: %w", remoteAddr.String(), err)
	}
	peer, parseErr := parseWhoIsJSON(output)
	if parseErr != nil {
		return PeerIdentity{}, fmt.Errorf("parse tailscale whois for %s: %w", remoteAddr.String(), parseErr)
	}

	// Only cache when the node owns the remote Tailscale IP directly (not a shared-node NAT translation).
	if slices.Contains(peer.Addresses, remoteAddr) {
		verifier.cacheMutex.Lock()
		if len(verifier.peerCache) >= maximumCachedPeers {
			for address, entry := range verifier.peerCache {
				if !now.Before(entry.expiresAt) {
					delete(verifier.peerCache, address)
				}
			}
			if len(verifier.peerCache) >= maximumCachedPeers {
				clear(verifier.peerCache)
			}
		}
		verifier.peerCache[remoteAddr] = cachedPeerEntry{
			peer:      peer,
			expiresAt: now.Add(verifier.cacheTTL),
		}
		verifier.cacheMutex.Unlock()
	}

	return peer, nil
}

func (verifier *Verifier) evaluatePolicy(peer PeerIdentity) error {
	sameTailnet := verifier.self.MagicDNSSuffix != "" && nodeMatchesTailnet(peer.NodeName, verifier.self.MagicDNSSuffix)
	explicitPolicy := len(verifier.allowedUsers) > 0 || len(verifier.allowedTags) > 0

	if explicitPolicy {
		if strings.EqualFold(peer.LoginName, taggedDevicesLogin) {
			if !sameTailnet && verifier.self.MagicDNSSuffix != "" {
				return fmt.Errorf("tagged tailscale node %q belongs to an external tailnet", peer.NodeName)
			}
			for _, tag := range peer.Tags {
				if slices.Contains(verifier.allowedTags, strings.ToLower(tag)) {
					return nil
				}
			}
			return fmt.Errorf("tailscale tagged node %q (tags=%s) is not in --tailscale-allow-tag", peer.NodeName, strings.Join(peer.Tags, ","))
		}
		normalizedLogin := strings.ToLower(strings.TrimSpace(peer.LoginName))
		if normalizedLogin != "" && slices.Contains(verifier.allowedUsers, normalizedLogin) {
			return nil
		}
		return fmt.Errorf("tailscale user %q (node=%s) is not in --tailscale-allow-user", peer.LoginName, peer.NodeName)
	}

	if strings.EqualFold(peer.LoginName, taggedDevicesLogin) {
		if len(peer.Tags) > 0 && sameTailnet {
			return nil
		}
		return fmt.Errorf("tagged tailscale node %q is not authorized on this tailnet", peer.NodeName)
	}

	if verifier.self.UserID != 0 && peer.UserID == verifier.self.UserID && peer.LoginName != "" {
		return nil
	}
	if verifier.self.LoginName != "" && !strings.EqualFold(verifier.self.LoginName, taggedDevicesLogin) &&
		strings.EqualFold(peer.LoginName, verifier.self.LoginName) {
		return nil
	}
	if len(verifier.self.Tags) > 0 && strings.EqualFold(verifier.self.LoginName, taggedDevicesLogin) {
		// When the master itself runs as a tagged node without explicit --tailscale-allow-user,
		// require an explicit --tailscale-allow-user or --tailscale-allow-tag unless tags overlap on the same tailnet.
		if sameTailnet {
			for _, tag := range peer.Tags {
				if slices.Contains(verifier.self.Tags, tag) {
					return nil
				}
			}
		}
		return fmt.Errorf(
			"tailscale peer %q (user=%s) rejected: master is a tagged node; pass --tailscale-allow-user or --tailscale-allow-tag",
			peer.NodeName,
			peer.LoginName,
		)
	}

	return fmt.Errorf(
		"tailscale peer %q (user=%s) does not match master owner %q; pass --tailscale-allow-user to authorize additional users",
		peer.NodeName,
		peer.LoginName,
		verifier.self.LoginName,
	)
}

func nodeMatchesTailnet(nodeName string, magicDNSSuffix string) bool {
	cleanNode := strings.ToLower(strings.TrimSuffix(strings.TrimSpace(nodeName), "."))
	cleanSuffix := strings.ToLower(strings.Trim(strings.TrimSpace(magicDNSSuffix), "."))
	if cleanNode == "" || cleanSuffix == "" {
		return false
	}
	return cleanNode == cleanSuffix || strings.HasSuffix(cleanNode, "."+cleanSuffix)
}

type statusNodeJSON struct {
	HostName       string   `json:"HostName"`
	DNSName        string   `json:"DNSName"`
	UserID         int64    `json:"UserID"`
	TailscaleIPs   []string `json:"TailscaleIPs"`
	Tags           []string `json:"Tags"`
	Online         bool     `json:"Online"`
	ExitNodeOption bool     `json:"ExitNodeOption"`
}

type statusJSONPayload struct {
	BackendState   string   `json:"BackendState"`
	MagicDNSSuffix string   `json:"MagicDNSSuffix"`
	TailscaleIPs   []string `json:"TailscaleIPs"`
	CurrentTailnet *struct {
		Name           string `json:"Name"`
		MagicDNSSuffix string `json:"MagicDNSSuffix"`
	} `json:"CurrentTailnet"`
	Self *statusNodeJSON           `json:"Self"`
	Peer map[string]statusNodeJSON `json:"Peer"`
	User map[string]struct {
		ID          int64  `json:"ID"`
		LoginName   string `json:"LoginName"`
		DisplayName string `json:"DisplayName"`
	} `json:"User"`
}

func loadSelfIdentity(
	operationContext context.Context,
	runCommand CommandRunner,
	binaryPath string,
) (SelfIdentity, error) {
	statusOutput, err := runCommand(operationContext, binaryPath, "status", "--json")
	if err != nil {
		return SelfIdentity{}, fmt.Errorf("query tailscale status: %w", err)
	}

	var status statusJSONPayload
	if err := json.Unmarshal(statusOutput, &status); err != nil {
		return SelfIdentity{}, fmt.Errorf("decode tailscale status JSON: %w", err)
	}
	if status.BackendState != "" && status.BackendState != "Running" {
		return SelfIdentity{}, fmt.Errorf("tailscale daemon is in state %q (expected Running)", status.BackendState)
	}
	if status.Self == nil {
		return SelfIdentity{}, errors.New("tailscale status JSON did not include Self node metadata")
	}

	suffix := strings.Trim(strings.TrimSpace(status.MagicDNSSuffix), ".")
	if suffix == "" && status.CurrentTailnet != nil {
		suffix = strings.Trim(strings.TrimSpace(status.CurrentTailnet.MagicDNSSuffix), ".")
	}

	rawIPs := status.Self.TailscaleIPs
	if len(rawIPs) == 0 {
		rawIPs = status.TailscaleIPs
	}
	var parsedIPs []netip.Addr
	primaryIPv4 := ""
	for _, rawIP := range rawIPs {
		addr, parseErr := netip.ParseAddr(strings.TrimSpace(rawIP))
		if parseErr != nil {
			continue
		}
		unmapped := addr.Unmap()
		if !slices.Contains(parsedIPs, unmapped) {
			parsedIPs = append(parsedIPs, unmapped)
		}
		if primaryIPv4 == "" && unmapped.Is4() && IsTailscaleAddr(unmapped) {
			primaryIPv4 = unmapped.String()
		}
	}
	if len(parsedIPs) == 0 {
		return SelfIdentity{}, errors.New("tailscale status reported no active Tailscale IPs for this machine")
	}

	loginName := ""
	if status.Self.UserID != 0 && len(status.User) > 0 {
		if profile, found := status.User[strconv.FormatInt(status.Self.UserID, 10)]; found {
			loginName = strings.TrimSpace(profile.LoginName)
		}
	}
	if loginName == "" && len(status.Self.Tags) > 0 {
		loginName = taggedDevicesLogin
	}
	if loginName == "" {
		whoisTarget := parsedIPs[0].String()
		if whoisBytes, whoisErr := runCommand(operationContext, binaryPath, "whois", "--json", whoisTarget); whoisErr == nil {
			if selfPeer, parseErr := parseWhoIsJSON(whoisBytes); parseErr == nil {
				loginName = selfPeer.LoginName
				if status.Self.UserID == 0 {
					status.Self.UserID = selfPeer.UserID
				}
			}
		}
	}

	dnsName := strings.TrimSuffix(strings.TrimSpace(status.Self.DNSName), ".")
	if dnsName == "" {
		dnsName = strings.TrimSpace(status.Self.HostName)
	}

	var normalizedTags []string
	for _, tag := range status.Self.Tags {
		if trimmed := strings.ToLower(strings.TrimSpace(tag)); trimmed != "" {
			normalizedTags = append(normalizedTags, trimmed)
		}
	}

	return SelfIdentity{
		HostName:       strings.TrimSpace(status.Self.HostName),
		DNSName:        dnsName,
		MagicDNSSuffix: suffix,
		UserID:         status.Self.UserID,
		LoginName:      loginName,
		Tags:           normalizedTags,
		TailscaleIPs:   parsedIPs,
		PrimaryIPv4:    primaryIPv4,
	}, nil
}

type whoIsJSONPayload struct {
	Node *struct {
		Name      string   `json:"Name"`
		Hostinfo  any      `json:"Hostinfo,omitempty"`
		Addresses []string `json:"Addresses"`
		Tags      []string `json:"Tags"`
	} `json:"Node"`
	UserProfile *struct {
		ID          int64  `json:"ID"`
		LoginName   string `json:"LoginName"`
		DisplayName string `json:"DisplayName"`
	} `json:"UserProfile"`
}

func parseWhoIsJSON(payload []byte) (PeerIdentity, error) {
	var parsed whoIsJSONPayload
	if err := json.Unmarshal(payload, &parsed); err != nil {
		return PeerIdentity{}, err
	}
	if parsed.Node == nil || parsed.UserProfile == nil {
		return PeerIdentity{}, errors.New("whois response missing Node or UserProfile")
	}

	var addresses []netip.Addr
	for _, rawEntry := range parsed.Node.Addresses {
		trimmed := strings.TrimSpace(rawEntry)
		if prefix, prefixErr := netip.ParsePrefix(trimmed); prefixErr == nil {
			unmapped := prefix.Addr().Unmap()
			if !slices.Contains(addresses, unmapped) {
				addresses = append(addresses, unmapped)
			}
			continue
		}
		if addr, addrErr := netip.ParseAddr(trimmed); addrErr == nil {
			unmapped := addr.Unmap()
			if !slices.Contains(addresses, unmapped) {
				addresses = append(addresses, unmapped)
			}
		}
	}

	var tags []string
	for _, tag := range parsed.Node.Tags {
		if trimmed := strings.ToLower(strings.TrimSpace(tag)); trimmed != "" {
			tags = append(tags, trimmed)
		}
	}

	return PeerIdentity{
		NodeName:  strings.TrimSuffix(strings.TrimSpace(parsed.Node.Name), "."),
		Addresses: addresses,
		UserID:    parsed.UserProfile.ID,
		LoginName: strings.TrimSpace(parsed.UserProfile.LoginName),
		Tags:      tags,
	}, nil
}

func normalizeAllowedUsers(entries []string) ([]string, error) {
	var normalized []string
	for _, raw := range entries {
		for item := range strings.SplitSeq(raw, ",") {
			trimmed := strings.ToLower(strings.TrimSpace(item))
			if trimmed == "" {
				continue
			}
			if strings.EqualFold(trimmed, taggedDevicesLogin) {
				return nil, errors.New("use --tailscale-allow-tag (for example tag:server) instead of --tailscale-allow-user tagged-devices")
			}
			if !slices.Contains(normalized, trimmed) {
				normalized = append(normalized, trimmed)
			}
		}
	}
	return normalized, nil
}

func normalizeAllowedTags(entries []string) ([]string, error) {
	var normalized []string
	for _, raw := range entries {
		for item := range strings.SplitSeq(raw, ",") {
			trimmed := strings.ToLower(strings.TrimSpace(item))
			if trimmed == "" {
				continue
			}
			if !strings.HasPrefix(trimmed, "tag:") {
				trimmed = "tag:" + trimmed
			}
			if len(trimmed) <= len("tag:") {
				return nil, fmt.Errorf("invalid Tailscale tag %q", item)
			}
			if !slices.Contains(normalized, trimmed) {
				normalized = append(normalized, trimmed)
			}
		}
	}
	return normalized, nil
}
