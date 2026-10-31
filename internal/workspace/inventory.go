package workspace

import (
	"errors"
	"strings"
)

// Worktree represents one record of `git worktree list --porcelain -z`.
type Worktree struct {
	Path         string
	Head         string
	Branch       string
	Bare         bool
	Detached     bool
	Locked       bool
	Prunable     bool
	Unrecognized bool
}

var errMalformedInventory = errors.New("worktree inventory is malformed")

func ParseWorktreeList(output []byte) ([]Worktree, error) {
	text := string(output)
	if text == "" {
		return nil, nil
	}
	if !strings.HasSuffix(text, "\x00") {
		return nil, errMalformedInventory
	}
	var worktrees []Worktree
	var current *Worktree
	for attribute := range strings.SplitSeq(strings.TrimSuffix(text, "\x00"), "\x00") {
		if attribute == "" {
			if current == nil {
				return nil, errMalformedInventory
			}
			worktrees = append(worktrees, *current)
			current = nil
			continue
		}
		label, value, hasValue := strings.Cut(attribute, " ")
		if current == nil {
			if label != "worktree" || !hasValue || !strings.HasPrefix(value, "/") {
				return nil, errMalformedInventory
			}
			current = &Worktree{Path: value}
			continue
		}
		switch label {
		case "worktree":
			return nil, errMalformedInventory
		case "HEAD":
			current.Head = value
		case "branch":
			current.Branch = value
		case "bare":
			current.Bare = true
		case "detached":
			current.Detached = true
		case "locked":
			current.Locked = true
		case "prunable":
			current.Prunable = true
		default:
			current.Unrecognized = true
		}
	}
	if current != nil {
		worktrees = append(worktrees, *current)
	}
	return worktrees, nil
}

type StatusSummary struct {
	Modified  int
	Untracked int
	Ignored   int
}

func ParseStatus(output []byte) (StatusSummary, error) {
	var summary StatusSummary
	text := string(output)
	if text == "" {
		return summary, nil
	}
	if !strings.HasSuffix(text, "\x00") {
		return summary, errMalformedInventory
	}
	fields := strings.Split(strings.TrimSuffix(text, "\x00"), "\x00")
	for index := 0; index < len(fields); index++ {
		entry := fields[index]
		if len(entry) < 4 || entry[2] != ' ' {
			return StatusSummary{}, errMalformedInventory
		}
		code := entry[:2]
		switch code {
		case "??":
			summary.Untracked++
		case "!!":
			summary.Ignored++
		default:
			summary.Modified++
			if strings.ContainsAny(code, "RC") {
				index++
				if index >= len(fields) {
					return StatusSummary{}, errMalformedInventory
				}
			}
		}
	}
	return summary, nil
}
