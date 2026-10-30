package acp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

var ErrConfigurationRejected = errors.New("acp adapter rejected the requested session configuration")

const maximumConfigurationValues = 256

type ConfigurationRequirement int

const (
	ConfigurationPolicy ConfigurationRequirement = iota
	ConfigurationRequested
)

type ConfigurationSelection struct {
	ID          string
	Value       string
	Requirement ConfigurationRequirement
}

func (selection ConfigurationSelection) failure(format string) error {
	cause := ErrMissingCapability
	if selection.Requirement == ConfigurationRequested {
		cause = ErrConfigurationRejected
	}
	return fmt.Errorf("%w: "+format, cause, selection.ID)
}

type configurationState map[string]configurationOptionState

type configurationOptionState struct {
	current string
	offered map[string]bool
}

func newConfigurationState(options []sessionConfigOption) configurationState {
	state := configurationState{}
	for _, option := range options {
		if option.ID == "" || len(option.ID) > eventIdentifierBytes {
			continue
		}
		var current string
		if json.Unmarshal(option.CurrentValue, &current) != nil {
			continue
		}
		state[option.ID] = configurationOptionState{current: current, offered: offeredValues(option.Options)}
	}
	return state
}

func offeredValues(raw json.RawMessage) map[string]bool {
	offered := map[string]bool{}
	for _, value := range orderedOfferedValues(raw) {
		offered[value] = true
	}
	return offered
}

func orderedOfferedValues(raw json.RawMessage) []string {
	values := []string{}
	seen := map[string]bool{}
	add := func(value *string) {
		if value != nil && !seen[*value] && len(values) < maximumConfigurationValues {
			seen[*value] = true
			values = append(values, *value)
		}
	}
	var entries []sessionConfigValue
	if json.Unmarshal(raw, &entries) != nil {
		return values
	}
	for _, entry := range entries {
		if entry.Value != nil {
			add(entry.Value)
			continue
		}
		if entry.Group == nil {
			continue
		}
		var grouped []sessionConfigValue
		if json.Unmarshal(entry.Options, &grouped) != nil {
			continue
		}
		for _, value := range grouped {
			add(value.Value)
		}
	}
	return values
}

func (client *Client) configure(operationContext context.Context, sessionID string, state configurationState, selections []ConfigurationSelection) error {
	for _, selection := range selections {
		option, offered := state[selection.ID]
		if !offered {
			return selection.failure("adapter offers no %s session option")
		}
		if !option.offered[selection.Value] {
			return selection.failure("adapter does not offer the required value for its %s session option")
		}
		if option.current == selection.Value {
			continue
		}
		requestContext, cancel := context.WithTimeout(operationContext, client.options.RequestTimeout)
		var response setConfigOptionResponse
		err := client.peer.call(requestContext, methodSetConfigOption, setConfigOptionRequest{SessionID: sessionID, ConfigID: selection.ID, Value: selection.Value}, &response)
		cancel()
		if err != nil {
			if operationContext.Err() != nil {
				return fmt.Errorf("%w before %s completed", ErrCancelled, methodSetConfigOption)
			}
			var responseError *ResponseError
			if errors.As(err, &responseError) {
				return selection.failure("adapter refused to set its %s session option")
			}
			return client.setupError(operationContext, methodSetConfigOption, err)
		}
		state = newConfigurationState(response.ConfigOptions)
	}
	for _, selection := range selections {
		if state[selection.ID].current != selection.Value {
			return selection.failure("adapter did not apply the required value for its %s session option")
		}
	}
	return nil
}
