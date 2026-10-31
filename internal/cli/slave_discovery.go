package cli

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cafecito-games/Orchestra/internal/activitylog"
	"github.com/cafecito-games/Orchestra/internal/localstate"
	"github.com/cafecito-games/Orchestra/internal/slave"
	"github.com/cafecito-games/Orchestra/internal/tailscaleauth"
)

const (
	localMasterProbeTimeout    = 400 * time.Millisecond
	tailscaleDiscoveryInterval = 2 * time.Second
	maximumSelectionAttempts   = 3
)

func resolveSlaveMasterEndpoint(
	operationContext context.Context,
	configuration *slave.Configuration,
	standardInput io.Reader,
	promptWriter io.Writer,
	logger *activitylog.Logger,
) error {
	if configuration == nil || configuration.EndpointExplicit {
		return nil
	}

	masterPort := configuration.MasterPort
	if masterPort <= 0 {
		masterPort = tailscaleauth.DefaultMasterPort
	}

	loggedWaiting := false
	for {
		if operationContext.Err() != nil {
			return operationContext.Err()
		}

		// 1. Check local connection.json discovery state first.
		if tryResolveLocalMaster(operationContext, configuration, logger) {
			return nil
		}

		// 2. Probe online peers on the Tailscale tailnet.
		result, err := tailscaleauth.DiscoverMasters(operationContext, tailscaleauth.DiscoverOptions{
			BinaryPath: configuration.TailscaleBinary,
			Port:       masterPort,
			PreferTLS:  configuration.TLSExplicit,
			HTTPClient: configuration.HTTPClient,
		})
		if err != nil {
			return err
		}
		if !result.TailscaleAvailable {
			// No active Tailscale daemon on this machine: preserve classic localhost default behavior.
			return nil
		}

		if len(result.Masters) == 1 {
			chosen := result.Masters[0]
			applyDiscoveredMaster(configuration, chosen)
			nodeLabel := chosen.DNSName
			if nodeLabel == "" {
				nodeLabel = chosen.HostName
			}
			logger.Info(
				"discovery",
				"discovered orchestra master on tailscale",
				activitylog.String("node", nodeLabel),
				activitylog.String("endpoint", chosen.Endpoint),
				activitylog.String("auth", chosen.AuthMode),
			)
			return nil
		}

		if len(result.Masters) > 1 {
			chosen, selectErr := selectDiscoveredMaster(operationContext, result.Masters, standardInput, promptWriter)
			if selectErr != nil {
				return selectErr
			}
			applyDiscoveredMaster(configuration, chosen)
			nodeLabel := chosen.DNSName
			if nodeLabel == "" {
				nodeLabel = chosen.HostName
			}
			logger.Info(
				"discovery",
				"selected orchestra master on tailscale",
				activitylog.String("node", nodeLabel),
				activitylog.String("endpoint", chosen.Endpoint),
				activitylog.String("auth", chosen.AuthMode),
			)
			return nil
		}

		if !loggedWaiting {
			loggedWaiting = true
			logger.Info(
				"discovery",
				"waiting for orchestra master on localhost or tailscale",
				activitylog.Int("port", masterPort),
				activitylog.Int("onlinePeers", result.OnlinePeers),
			)
		}

		timer := time.NewTimer(tailscaleDiscoveryInterval)
		select {
		case <-operationContext.Done():
			timer.Stop()
			return operationContext.Err()
		case <-timer.C:
		}
	}
}

func tryResolveLocalMaster(
	operationContext context.Context,
	configuration *slave.Configuration,
	logger *activitylog.Logger,
) bool {
	if strings.TrimSpace(configuration.ConnectionFilePath) == "" {
		return false
	}
	discovered, found, err := localstate.ReadConnectionState(configuration.ConnectionFilePath)
	if err != nil || !found || discovered.Endpoint == "" {
		return false
	}
	if _, ok := tailscaleauth.ProbeMasterHealth(operationContext, discovered.Endpoint, configuration.HTTPClient, localMasterProbeTimeout); !ok {
		return false
	}
	configuration.ControlEndpoint = discovered.Endpoint
	if !configuration.TokenExplicit && (discovered.Token != "" || strings.EqualFold(discovered.AuthMode, tailscaleauth.AuthModeTailscale)) {
		configuration.Token = discovered.Token
	}
	logger.Info(
		"discovery",
		"connected to local orchestra master from discovery state",
		activitylog.String("endpoint", discovered.Endpoint),
	)
	return true
}

func applyDiscoveredMaster(configuration *slave.Configuration, chosen tailscaleauth.DiscoveredMaster) {
	configuration.ControlEndpoint = chosen.Endpoint
	if !configuration.TokenExplicit && strings.EqualFold(chosen.AuthMode, tailscaleauth.AuthModeTailscale) {
		configuration.Token = ""
	}
}

func selectDiscoveredMaster(
	operationContext context.Context,
	masters []tailscaleauth.DiscoveredMaster,
	standardInput io.Reader,
	promptWriter io.Writer,
) (tailscaleauth.DiscoveredMaster, error) {
	if !isInteractiveReader(standardInput) {
		return tailscaleauth.DiscoveredMaster{}, multipleMastersNonInteractiveError(masters)
	}

	fmt.Fprintln(promptWriter, "\nMultiple Orchestra masters discovered on Tailscale:")
	for index, master := range masters {
		label := master.HostName
		if master.DNSName != "" && !strings.EqualFold(master.DNSName, master.HostName) {
			if label != "" {
				label = fmt.Sprintf("%s (%s)", label, master.DNSName)
			} else {
				label = master.DNSName
			}
		}
		if label == "" {
			label = master.Address
		}
		authSuffix := ""
		if master.AuthMode != "" {
			authSuffix = fmt.Sprintf(" [auth=%s]", master.AuthMode)
		}
		fmt.Fprintf(promptWriter, "  [%d] %s — %s%s\n", index+1, label, master.Endpoint, authSuffix)
	}

	bufferedReader := bufio.NewReader(standardInput)
	for range maximumSelectionAttempts {
		fmt.Fprintf(promptWriter, "Select master [1-%d]: ", len(masters))

		type readResult struct {
			line string
			err  error
		}
		lineChannel := make(chan readResult, 1)
		go func() {
			line, readErr := bufferedReader.ReadString('\n')
			lineChannel <- readResult{line: line, err: readErr}
		}()

		var outcome readResult
		select {
		case <-operationContext.Done():
			return tailscaleauth.DiscoveredMaster{}, operationContext.Err()
		case outcome = <-lineChannel:
		}

		trimmed := strings.TrimSpace(outcome.line)
		if outcome.err != nil && trimmed == "" {
			if errors.Is(outcome.err, io.EOF) {
				return tailscaleauth.DiscoveredMaster{}, multipleMastersNonInteractiveError(masters)
			}
			return tailscaleauth.DiscoveredMaster{}, outcome.err
		}

		choice, parseErr := strconv.Atoi(trimmed)
		if parseErr == nil && choice >= 1 && choice <= len(masters) {
			return masters[choice-1], nil
		}
		fmt.Fprintf(promptWriter, "Invalid selection %q; enter a number between 1 and %d.\n", trimmed, len(masters))
		if errors.Is(outcome.err, io.EOF) {
			break
		}
	}

	return tailscaleauth.DiscoveredMaster{}, errors.New("aborted after invalid selection; pass --endpoint <url> explicitly")
}

func isInteractiveReader(reader io.Reader) bool {
	if reader == nil {
		return false
	}
	file, isFile := reader.(*os.File)
	if !isFile {
		return true
	}
	if file.Name() == os.DevNull {
		return false
	}
	info, err := file.Stat()
	if err != nil {
		return false
	}
	return (info.Mode() & os.ModeCharDevice) != 0
}

func multipleMastersNonInteractiveError(masters []tailscaleauth.DiscoveredMaster) error {
	descriptions := make([]string, 0, len(masters))
	for _, master := range masters {
		if master.HostName != "" {
			descriptions = append(descriptions, fmt.Sprintf("%s [%s]", master.Endpoint, master.HostName))
		} else {
			descriptions = append(descriptions, master.Endpoint)
		}
	}
	return fmt.Errorf(
		"multiple Orchestra masters discovered on Tailscale (%s); run interactively to select one or pass --endpoint <url>",
		strings.Join(descriptions, ", "),
	)
}
