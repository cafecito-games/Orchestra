package protocol

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"unicode/utf8"
)

const (
	DefaultRoomChannel      = "main"
	AllRoomChannels         = "*"
	RoomChannelMaximumBytes = 64

	StallKindUnreadByCoordinator = "unread_by_coordinator"
	StallKindUnaddressedResult   = "unaddressed_result"
	StallKindUnansweredMention   = "unanswered_mention"
	StallKindBlockedTasks        = "blocked_tasks"
)

var (
	ErrRoomMainChannelMention = errors.New("room mention @#main is not allowed; use @all")

	roomChannelPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)

	ThreadStallConditionKinds = []string{
		StallKindUnreadByCoordinator,
		StallKindUnaddressedResult,
		StallKindUnansweredMention,
		StallKindBlockedTasks,
	}
)

type ParsedRoomMentions struct {
	Tokens   []string `json:"tokens,omitempty"`
	All      bool     `json:"all,omitempty"`
	Channels []string `json:"channels,omitempty"`
	Handles  []string `json:"handles,omitempty"`
}

type ThreadStallCondition struct {
	Kind         string   `json:"kind"`
	Summary      string   `json:"summary"`
	RoomSequence int64    `json:"roomSequence,omitempty"`
	Handle       string   `json:"handle,omitempty"`
	InstanceID   string   `json:"instanceId,omitempty"`
	TaskID       string   `json:"taskId,omitempty"`
	TaskIDs      []string `json:"taskIds,omitempty"`
	RunID        string   `json:"runId,omitempty"`
}

type ThreadStallReport struct {
	ThreadID          string                 `json:"threadId"`
	Stalled           bool                   `json:"stalled"`
	Kind              string                 `json:"kind,omitempty"`
	Reasons           []string               `json:"reasons,omitempty"`
	Summary           string                 `json:"summary"`
	Fingerprint       string                 `json:"fingerprint"`
	EvaluatedAt       string                 `json:"evaluatedAt"`
	RoomHeadSequence  int64                  `json:"roomHeadSequence"`
	EventHeadSequence int64                  `json:"eventHeadSequence"`
	RoomSequence      int64                  `json:"roomSequence,omitempty"`
	BlockedTaskIDs    []string               `json:"blockedTaskIds,omitempty"`
	Conditions        []ThreadStallCondition `json:"conditions"`
}

// NormalizeRoomChannel validates and normalizes a room channel name, mapping "" and "main"
// to DefaultRoomChannel ("main").
func NormalizeRoomChannel(raw string) (string, error) {
	normalized := strings.ToLower(strings.TrimSpace(raw))
	if normalized == "" || normalized == DefaultRoomChannel {
		return DefaultRoomChannel, nil
	}
	if len(normalized) > RoomChannelMaximumBytes || LooksSecretLike(normalized) || !roomChannelPattern.MatchString(normalized) {
		return "", fmt.Errorf("invalid room channel %q: must match [a-z0-9][a-z0-9_-]{0,63}", raw)
	}
	return normalized, nil
}

// EffectiveRoomChannel returns the normalized channel for a stored RoomMessage,
// treating empty legacy values as DefaultRoomChannel ("main").
func EffectiveRoomChannel(raw string) string {
	normalized, err := NormalizeRoomChannel(raw)
	if err != nil {
		return strings.ToLower(strings.TrimSpace(raw))
	}
	return normalized
}

// NormalizeRoomReadChannel validates a read_room channel filter where "" selects
// DefaultRoomChannel plus caller subscriptions, "*" selects all channels, and a
// named channel selects that channel.
func NormalizeRoomReadChannel(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", nil
	}
	if trimmed == AllRoomChannels {
		return AllRoomChannels, nil
	}
	return NormalizeRoomChannel(trimmed)
}

// NormalizeRoomChannels validates and deduplicates a slice of room channel subscriptions.
func NormalizeRoomChannels(rawChannels []string) ([]string, error) {
	if len(rawChannels) == 0 {
		return nil, nil
	}
	var normalizedChannels []string
	for _, rawChannel := range rawChannels {
		normalized, err := NormalizeRoomChannel(rawChannel)
		if err != nil {
			return nil, err
		}
		if normalized == DefaultRoomChannel {
			continue
		}
		if !slices.Contains(normalizedChannels, normalized) {
			normalizedChannels = append(normalizedChannels, normalized)
		}
	}
	slices.Sort(normalizedChannels)
	return normalizedChannels, nil
}

// InstanceSubscribesToRoomChannel reports whether subscribedChannels includes targetChannel.
func InstanceSubscribesToRoomChannel(subscribedChannels []string, targetChannel string) bool {
	normalizedTarget := EffectiveRoomChannel(targetChannel)
	if normalizedTarget == DefaultRoomChannel {
		return true
	}
	for _, rawChannel := range subscribedChannels {
		normalizedSubscription, err := NormalizeRoomChannel(rawChannel)
		if err == nil && normalizedSubscription == normalizedTarget {
			return true
		}
	}
	return false
}

// MatchesHandleMention reports whether candidateHandle matches mentionHandle either
// exactly or as a numbered group member ("<prefix>-1" .. "<prefix>-k").
func MatchesHandleMention(candidateHandle string, mentionHandle string) bool {
	normalizedCandidate, candidateOK := NormalizeMailHandle(candidateHandle)
	normalizedMention, mentionOK := NormalizeMailHandle(mentionHandle)
	if !candidateOK || !mentionOK {
		return false
	}
	if normalizedCandidate == normalizedMention {
		return true
	}
	suffix, hasPrefix := strings.CutPrefix(normalizedCandidate, normalizedMention+"-")
	if !hasPrefix || suffix == "" || suffix[0] < '1' || suffix[0] > '9' {
		return false
	}
	for index := 1; index < len(suffix); index++ {
		if suffix[index] < '0' || suffix[index] > '9' {
			return false
		}
	}
	return true
}

// ParseRoomMentions extracts @handle, @prefix, @#channel, and @all mentions from body
// merged with explicitMentions. Mentioning @#main returns ErrRoomMainChannelMention.
func ParseRoomMentions(body string, explicitMentions []string) (ParsedRoomMentions, error) {
	var parsed ParsedRoomMentions
	addChannel := func(channel string) {
		if !slices.Contains(parsed.Channels, channel) {
			parsed.Channels = append(parsed.Channels, channel)
		}
	}
	addHandle := func(handle string) {
		if !slices.Contains(parsed.Handles, handle) {
			parsed.Handles = append(parsed.Handles, handle)
		}
	}

	for index := 0; index < len(body); index++ {
		if body[index] != '@' {
			continue
		}
		if index > 0 {
			previousRune, _ := utf8.DecodeLastRuneInString(body[:index])
			if isMentionBoundaryBlocker(previousRune) {
				continue
			}
		}

		tokenStart := index + 1
		if tokenStart >= len(body) {
			continue
		}

		if body[tokenStart] == '#' {
			channelStart := tokenStart + 1
			channelEnd := channelStart
			for channelEnd < len(body) && isChannelTokenByte(body[channelEnd]) {
				channelEnd++
			}
			rawChannel := body[channelStart:channelEnd]
			if rawChannel == "" {
				continue
			}
			if strings.EqualFold(rawChannel, DefaultRoomChannel) {
				return ParsedRoomMentions{}, ErrRoomMainChannelMention
			}
			normalizedChannel, err := NormalizeRoomChannel(rawChannel)
			if err == nil {
				addChannel(normalizedChannel)
			}
			index = channelEnd - 1
			continue
		}

		handleEnd := tokenStart
		for handleEnd < len(body) && isHandleTokenByte(body[handleEnd]) {
			handleEnd++
		}
		if handleEnd == tokenStart {
			continue
		}

		if handleEnd < len(body) && body[handleEnd] == '@' {
			hostStart := handleEnd + 1
			hostEnd := hostStart
			for hostEnd < len(body) && isHandleTokenByte(body[hostEnd]) {
				hostEnd++
			}
			rawAddress := strings.TrimRight(body[tokenStart:hostEnd], "._-")
			if parsedHandle, _, _, ok := ParseMailAddress(rawAddress); ok {
				if parsedHandle == "all" {
					parsed.All = true
				} else {
					addHandle(parsedHandle)
				}
				index = hostEnd - 1
				continue
			}
			index = hostEnd - 1
			continue
		}

		rawHandle := strings.TrimRight(body[tokenStart:handleEnd], "._-")
		if rawHandle == "" {
			continue
		}
		if strings.EqualFold(rawHandle, "all") {
			parsed.All = true
		} else if normalizedHandle, ok := NormalizeMailHandle(rawHandle); ok {
			addHandle(normalizedHandle)
		}
		index = handleEnd - 1
	}

	for _, rawMention := range explicitMentions {
		trimmed := strings.TrimSpace(rawMention)
		if trimmed == "" {
			return ParsedRoomMentions{}, errors.New("room mention must not be empty")
		}
		withoutAt := strings.TrimPrefix(trimmed, "@")
		if withoutAt == "" {
			return ParsedRoomMentions{}, fmt.Errorf("invalid room mention %q", rawMention)
		}
		if rawChannel, isChannel := strings.CutPrefix(withoutAt, "#"); isChannel {
			if strings.EqualFold(strings.TrimSpace(rawChannel), DefaultRoomChannel) {
				return ParsedRoomMentions{}, ErrRoomMainChannelMention
			}
			normalizedChannel, err := NormalizeRoomChannel(rawChannel)
			if err != nil {
				return ParsedRoomMentions{}, err
			}
			addChannel(normalizedChannel)
			continue
		}
		if strings.EqualFold(withoutAt, "all") {
			parsed.All = true
			continue
		}
		if strings.Contains(withoutAt, "@") {
			parsedHandle, _, _, ok := ParseMailAddress(withoutAt)
			if !ok {
				return ParsedRoomMentions{}, fmt.Errorf("invalid room mention %q", rawMention)
			}
			if parsedHandle == "all" {
				parsed.All = true
			} else {
				addHandle(parsedHandle)
			}
			continue
		}
		normalizedHandle, ok := NormalizeMailHandle(withoutAt)
		if !ok {
			return ParsedRoomMentions{}, fmt.Errorf("invalid room mention %q", rawMention)
		}
		addHandle(normalizedHandle)
	}

	slices.Sort(parsed.Channels)
	slices.Sort(parsed.Handles)

	if parsed.All {
		parsed.Tokens = append(parsed.Tokens, "@all")
	}
	for _, channel := range parsed.Channels {
		parsed.Tokens = append(parsed.Tokens, "@#"+channel)
	}
	for _, handle := range parsed.Handles {
		parsed.Tokens = append(parsed.Tokens, "@"+handle)
	}

	return parsed, nil
}

func isMentionBoundaryBlocker(character rune) bool {
	if (character >= 'a' && character <= 'z') ||
		(character >= 'A' && character <= 'Z') ||
		(character >= '0' && character <= '9') {
		return true
	}
	switch character {
	case '.', '_', '-', '@', '#', '/':
		return true
	default:
		return false
	}
}

func isChannelTokenByte(character byte) bool {
	return (character >= 'a' && character <= 'z') ||
		(character >= 'A' && character <= 'Z') ||
		(character >= '0' && character <= '9') ||
		character == '_' ||
		character == '-'
}

func isHandleTokenByte(character byte) bool {
	return isChannelTokenByte(character) || character == '.'
}
