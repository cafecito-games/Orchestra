package mcpserver

import "github.com/cafecito-games/Orchestra/internal/protocol"

const instructions = "Use get_task_context for the durable thread, your task, its lineage, your mailbox cursor, and your room summary. " +
	"Share thread-scoped findings, contracts, decisions, gotchas, questions, and answers on the blackboard with post_room_message and inspect sibling entries with read_room. " +
	"When get_task_context or wait_for_task_events indicates an unread room nudge (or update_task returns unread_room_nudge), call read_room, reconcile your work if needed, and then finalize. " +
	"Workers report with update_task, ask or answer through send_task_message, and wait for replies, nudges, task changes, or incoming mail with wait_for_activity (or wait_for_task_events / wait_for_mail). " +
	"Delegating instances inspect get_execution_inventory and submit parallel work with submit_tasks, describing capabilities rather than machines, " +
	"and may spawn_instance (with optional handle, publicBrief/brief, count, and forkFromInstanceId), inspect it with get_instance, switch models in place with reconfigure_instance, renew_instance while it is needed, and release_instance when it is done. " +
	"Refine or complete the thread with update_thread and post durable files with post_artifact. " +
	"Peer agents across machines also participate in Orchestra Agent Mail (<handle>@<host>): inspect your address with get_mail_identity, rename your handle with set_mail_identity, discover peers with list_mailboxes, send or reply with send_mail, and inspect or await messages with read_mail and wait_for_mail. " +
	"Every mutation takes a stable idempotency key: retry with the same key and arguments after a retryable error."

type schema = map[string]any

func object(properties schema, required ...string) schema {
	result := schema{"type": "object", "properties": properties, "additionalProperties": false}
	if len(required) > 0 {
		result["required"] = required
	}
	return result
}

func resultSchema(properties schema, required ...string) schema {
	output := schema{"type": "object", "properties": properties}
	if len(required) > 0 {
		output["required"] = required
	}
	return output
}

func text() schema                 { return schema{"type": "string"} }
func boolean() schema              { return schema{"type": "boolean"} }
func integer() schema              { return schema{"type": "integer"} }
func anyObject() schema            { return schema{"type": "object"} }
func enum(values ...string) schema { return schema{"type": "string", "enum": values} }
func list(items schema) schema     { return listSchema(items) }
func listSchema(items schema) schema {
	return schema{"type": "array", "items": items}
}
func objects() schema { return list(anyObject()) }

func requirementsSchema() schema {
	strings := list(text())
	return object(schema{
		"skills":                 strings,
		"harnessIds":             list(enum(protocol.HarnessIDs...)),
		"models":                 strings,
		"transports":             list(enum(protocol.HarnessTransports...)),
		"operatingSystems":       strings,
		"architectures":          strings,
		"labels":                 strings,
		"accelerators":           strings,
		"toolchains":             strings,
		"minimumConcurrency":     integer(),
		"minimumMemoryMegabytes": integer(),
		"projectProfileId":       text(),
		"workspace": object(schema{
			"repository":    text(),
			"path":          text(),
			"writable":      boolean(),
			"isolation":     enum("git-worktree", "exclusive-existing"),
			"cleanup":       enum("retain", "when-unchanged"),
			"baseBranch":    text(),
			"sourceLeaseId": text(),
		}, "writable"),
		"preferences": object(schema{
			"nodeIds":    strings,
			"harnessIds": list(enum(protocol.HarnessIDs...)),
			"models":     strings,
			"labels":     strings,
		}),
	})
}

func purposeSchema(includeInstructions bool) schema {
	properties := schema{"name": text(), "title": text(), "summary": text()}
	if includeInstructions {
		properties["instructions"] = text()
	}
	return object(properties)
}

func leaseSchema() schema {
	return object(schema{"idleTimeoutSeconds": integer(), "expiresAt": text()}, "idleTimeoutSeconds", "expiresAt")
}

func safeInstanceSchema() schema {
	return resultSchema(schema{
		"id":                   text(),
		"threadId":             text(),
		"handle":               text(),
		"forkFromInstanceId":   text(),
		"purpose":              purposeSchema(true),
		"delegation":           resultSchema(schema{"canDelegate": boolean()}, "canDelegate"),
		"requirements":         anyObject(),
		"roomChannels":         list(text()),
		"lastReadRoomSequence": integer(),
		"lastPostRoomSequence": integer(),
		"lease":                leaseSchema(),
		"status":               text(),
		"createdAt":            text(),
		"updatedAt":            text(),
	}, "id", "threadId", "delegation", "requirements", "lease", "status", "createdAt", "updatedAt")
}

func safeAllocationSchema() schema {
	return resultSchema(schema{
		"id":         text(),
		"instanceId": text(),
		"nodeId":     text(),
		"harnessId":  enum(protocol.HarnessIDs...),
		"model":      text(),
		"transport":  enum(protocol.HarnessTransports...),
		"workspace":  text(),
		"lease":      leaseSchema(),
		"status":     text(),
		"createdAt":  text(),
		"updatedAt":  text(),
	}, "id", "instanceId", "nodeId", "harnessId", "model", "transport", "lease", "status", "createdAt", "updatedAt")
}

func instanceToolResultSchema(mutation bool) schema {
	required := []string{"instance"}
	if mutation {
		required = append(required, "replayed")
	}
	return resultSchema(schema{
		"instance":          safeInstanceSchema(),
		"allocation":        safeAllocationSchema(),
		"initialTaskId":     text(),
		"instances":         list(safeInstanceSchema()),
		"allocations":       list(safeAllocationSchema()),
		"initialTaskIds":    list(text()),
		"briefRoomSequence": integer(),
		"replayed":          boolean(),
	}, required...)
}

type toolDefinition struct {
	title, description string
	input, output      schema
	readOnly           bool
}

var definitions = map[string]toolDefinition{
	"get_task_context": {
		title:       "Get task context",
		description: "Get the durable thread, your task or a visible related task, its dependencies, attempts, child tasks, artifacts, room summary, your mailbox summary and cursor, and limits.",
		input:       object(schema{"taskId": text()}),
		output: resultSchema(schema{
			"thread": anyObject(), "caller": anyObject(), "task": anyObject(), "room": anyObject(), "durableTask": anyObject(),
			"childTasks": objects(), "taskGraph": objects(), "taskGraphTruncated": boolean(),
			"mailbox": anyObject(), "artifacts": objects(), "limits": anyObject(), "version": text(),
		}, "thread", "task", "room", "artifacts"),
		readOnly: true,
	},
	"read_room": {
		title:       "Read thread room",
		description: "Read ordered blackboard entries for the current thread and advance your task's lastReadRoomSequence cursor.",
		input: object(schema{
			"afterSequence": schema{"type": "integer", "minimum": 0},
			"channel":       text(),
			"kinds":         list(enum(protocol.RoomMessageKinds...)),
			"limit":         schema{"type": "integer", "minimum": 1, "maximum": protocol.MaximumRoomReadLimit},
		}),
		output: resultSchema(schema{
			"threadId": text(), "room": anyObject(), "channel": text(), "messages": objects(),
			"headSequence": integer(), "lastReadSequence": integer(), "requiredSequence": integer(),
			"unreadNudge": boolean(), "throughSequence": integer(), "hasMore": boolean(),
		}, "threadId", "room", "messages", "headSequence", "lastReadSequence", "unreadNudge", "hasMore"),
		readOnly: true,
	},
	"post_room_message": {
		title:       "Post room message",
		description: "Append a structured blackboard entry (finding, contract, decision, gotcha, guidance, question, or answer) to the current thread's room.",
		input:       postRoomMessageInputSchema(false),
		output: resultSchema(schema{
			"threadId": text(), "room": anyObject(), "message": anyObject(),
			"nudgedTaskIds": list(text()), "wokenInstanceIds": list(text()), "wakeTaskIds": list(text()),
			"replayed": boolean(),
		}, "threadId", "room", "message", "nudgedTaskIds", "replayed"),
	},
	"post_artifact": {
		title:       "Post artifact",
		description: "Publish a regular file from the current run workspace. Use a relative path and a stable idempotency key.",
		input: object(schema{
			"relativePath":   text(),
			"title":          text(),
			"kind":           enum(protocol.ArtifactKinds...),
			"mediaType":      text(),
			"summary":        text(),
			"idempotencyKey": text(),
		}, "relativePath", "title", "kind", "mediaType", "idempotencyKey"),
		output: resultSchema(schema{
			"id": text(), "runId": text(), "title": text(), "kind": text(), "downloadPath": text(), "uploaded": boolean(),
		}, "id", "runId", "title", "kind", "downloadPath", "uploaded"),
	},
	"update_thread": {
		title:       "Update thread",
		description: "Refine the current thread title, objective, or summary, or mark it active or completed.",
		input: func() schema {
			input := object(schema{"title": text(), "objective": text(), "summary": text(), "status": enum("active", "completed")})
			input["minProperties"] = 1
			return input
		}(),
		output: resultSchema(schema{"thread": anyObject()}, "thread"),
	},
	"delegate_task": {
		title:       "Delegate task",
		description: "Create a bounded child task pinned to a resident instance or node. Retain the returned task id and inspect it with get_task_context.",
		input: object(schema{
			"instanceId":     text(),
			"nodeId":         text(),
			"title":          text(),
			"task":           text(),
			"requirements":   requirementsSchema(),
			"artifactIds":    list(text()),
			"idempotencyKey": text(),
		}, "task", "idempotencyKey"),
		output: resultSchema(schema{
			"taskId": text(), "status": text(), "created": boolean(), "assignment": anyObject(), "placement": anyObject(),
		}, "taskId", "status", "created"),
	},
	"get_execution_inventory": {
		title:       "Get execution inventory",
		description: "List compute nodes with harnesses, active offering cooldowns, capacity, and worker-reported capabilities and their freshness, plus active instances, to write task requirements.",
		input:       object(schema{}),
		output: resultSchema(schema{
			"generatedAt": text(), "nodes": objects(), "instances": objects(), "offeringCooldowns": objects(), "truncated": anyObject(),
		}, "generatedAt", "nodes"),
		readOnly: true,
	},
	"list_workspace_leases": {
		title:       "List workspace leases",
		description: "List provisioned or retained git-worktree and exclusive workspace leases in this thread, optionally filtered by taskId, nodeId, or status.",
		input: object(schema{
			"threadId": text(),
			"taskId":   text(),
			"nodeId":   text(),
			"status":   enum(protocol.WorkspaceLeaseStatuses...),
		}),
		output:   resultSchema(schema{"workspaceLeases": objects()}, "workspaceLeases"),
		readOnly: true,
	},
	"land_workspace_lease": {
		title:       "Land workspace lease",
		description: "Safely land a retained git-worktree lease branch onto its target branch via fast-forward, merge-ff-only, or cherry-pick, preserving the worktree on dirty/conflict diagnostics and optionally cleaning up after landing.",
		input: object(schema{
			"leaseId":          text(),
			"mode":             enum(protocol.WorkspaceLandingModes...),
			"targetBranch":     text(),
			"cleanupAfterLand": boolean(),
		}, "leaseId"),
		output: resultSchema(schema{
			"workspaceLease": anyObject(),
			"result":         anyObject(),
		}, "workspaceLease", "result"),
	},
	"spawn_instance": {
		title:       "Spawn instance",
		description: "Request one or more non-delegating resident instances for this thread, optionally assigning mail handles, posting a public contract brief to the room, sharding count instances, subscribing to roomChannels, or seeding warm context from forkFromInstanceId.",
		input: object(schema{
			"threadId":           text(),
			"idempotencyKey":     text(),
			"handle":             text(),
			"publicBrief":        boolean(),
			"brief":              text(),
			"count":              schema{"type": "integer", "minimum": 1, "maximum": protocol.MaximumSpawnInstanceCount},
			"forkFromInstanceId": text(),
			"roomChannels":       list(text()),
			"requirements":       requirementsSchema(),
			"purpose":            purposeSchema(true),
			"idleTimeoutSeconds": schema{"type": "integer", "minimum": protocol.MinimumInstanceIdleTimeoutSeconds, "maximum": protocol.MaximumInstanceIdleTimeoutSeconds},
			"initialTask":        object(schema{"title": text(), "instructions": text()}, "title", "instructions"),
			"coordinator":        boolean(),
		}, "idempotencyKey"),
		output: instanceToolResultSchema(true),
	},
	"get_instance": {
		title:       "Get instance",
		description: "Get one same-thread instance and its current occupying allocation, including a terminal instance.",
		input:       object(schema{"instanceId": text()}, "instanceId"),
		output:      instanceToolResultSchema(false),
		readOnly:    true,
	},
	"reconfigure_instance": {
		title:       "Reconfigure instance",
		description: "Switch an idle or ready same-thread resident instance's model in place while preserving its warm session, workspace, and Agent Mail identity.",
		input: object(schema{
			"instanceId":     text(),
			"idempotencyKey": text(),
			"model":          text(),
		}, "instanceId", "idempotencyKey", "model"),
		output: instanceToolResultSchema(true),
	},
	"renew_instance": {
		title:       "Renew instance",
		description: "Renew a nonterminal same-thread instance lease. Retry with the same idempotency key and arguments.",
		input: object(schema{
			"instanceId":         text(),
			"idempotencyKey":     text(),
			"idleTimeoutSeconds": schema{"type": "integer", "minimum": protocol.MinimumInstanceIdleTimeoutSeconds, "maximum": protocol.MaximumInstanceIdleTimeoutSeconds},
		}, "instanceId", "idempotencyKey"),
		output: instanceToolResultSchema(true),
	},
	"release_instance": {
		title:       "Release instance",
		description: "Release a same-thread instance by draining work or cancelling it. Retry with the same idempotency key and arguments.",
		input: object(schema{
			"instanceId":     text(),
			"idempotencyKey": text(),
			"mode":           enum(protocol.InstanceReleaseModes...),
		}, "instanceId", "idempotencyKey", "mode"),
		output: instanceToolResultSchema(true),
	},
	"submit_tasks": {
		title: "Submit tasks",
		description: "Atomically submit a batch of tasks. Name tasks with local keys, depend on sibling keys or existing task ids, and state hard requirements and preferences as capabilities. " +
			"An optional pin narrows placement to an instance or node and never bypasses requirements.",
		input: object(schema{
			"idempotencyKey": text(),
			"tasks": list(object(schema{
				"key":          text(),
				"title":        text(),
				"instructions": text(),
				"requirements": requirementsSchema(),
				"dependencies": list(object(schema{"key": text(), "taskId": text(), "policy": enum("require-success", "allow-failure")})),
				"pin":          object(schema{"instanceId": text(), "nodeId": text()}),
			}, "key", "title", "instructions")),
		}, "idempotencyKey", "tasks"),
		output: resultSchema(schema{
			"created": boolean(), "submissionId": text(), "taskIdsByKey": anyObject(), "tasks": objects(),
		}, "created", "submissionId", "taskIdsByKey", "tasks"),
	},
	"send_task_message": {
		title:       "Send task message",
		description: "Send an immutable message to the thread orchestrator or to a task in your lineage, optionally correlated with or replying to an earlier message and referencing uploaded artifacts.",
		input: object(schema{
			"idempotencyKey":     text(),
			"recipient":          object(schema{"type": enum("task", "orchestrator"), "taskId": text()}, "type"),
			"kind":               enum(protocol.TaskMessageKinds...),
			"body":               text(),
			"correlationId":      text(),
			"inReplyToMessageId": text(),
			"artifactIds":        list(text()),
		}, "idempotencyKey", "recipient", "kind", "body"),
		output: resultSchema(schema{
			"created": boolean(), "messageId": text(), "sequence": integer(), "recipient": anyObject(), "createdAt": text(),
		}, "created", "messageId", "sequence", "recipient", "createdAt"),
	},
	"wait_for_task_events": {
		title:       "Wait for task events",
		description: "Wait until messages to you or changes to visible tasks commit after the cursor, or until the timeout. Always pass the returned cursor to the next call; optionally acknowledge messages you have handled.",
		input: object(schema{
			"cursor":                text(),
			"timeoutMilliseconds":   schema{"type": "integer", "minimum": 0, "maximum": protocol.MaximumWaitMilliseconds},
			"maximumEvents":         schema{"type": "integer", "minimum": 1, "maximum": protocol.MaximumEventsPerWait},
			"acknowledgeMessageIds": list(text()),
		}),
		output: resultSchema(schema{
			"events": objects(), "cursor": text(), "hasMore": boolean(), "timedOut": boolean(),
		}, "events", "cursor", "hasMore", "timedOut"),
	},
	"update_task": {
		title:       "Update task",
		description: "As the task's current assignee, report progress, yield as blocked with blockerKind/blockerReason, or report completion fields.",
		input: object(schema{
			"idempotencyKey": text(),
			"status":         enum("blocked", "completed"),
			"progress":       text(),
			"blockerKind":    enum(protocol.TaskBlockerKinds...),
			"blockerReason":  text(),
			"blockedReason":  schema{"type": []string{"string", "null"}},
			"completion":     object(schema{"summary": text(), "artifactIds": list(text())}, "summary"),
		}, "idempotencyKey"),
		output: resultSchema(schema{"created": boolean(), "updateId": text(), "task": anyObject()}, "created", "updateId", "task"),
	},
	"unblock_task": {
		title:       "Unblock task",
		description: "Idempotently unblock a blocked delegated child task back to ready (or pending if dependencies are unsatisfied), clearing blocker metadata and optionally recording a resolution note.",
		input: object(schema{
			"taskId":         text(),
			"idempotencyKey": text(),
			"note":           text(),
		}, "taskId", "idempotencyKey"),
		output: resultSchema(schema{"task": anyObject(), "replayed": boolean()}, "task", "replayed"),
	},
	"reopen_task": {
		title:       "Reopen task",
		description: "Idempotently reopen a failed delegated child task back to ready (or pending if dependencies are unsatisfied) with a fresh attempt budget.",
		input: object(schema{
			"taskId":             text(),
			"idempotencyKey":     text(),
			"note":               text(),
			"additionalAttempts": schema{"type": "integer", "minimum": 1, "maximum": protocol.MaximumTaskAttempts},
		}, "taskId", "idempotencyKey"),
		output: resultSchema(schema{"task": anyObject(), "replayed": boolean()}, "task", "replayed"),
	},
	"get_mail_identity": {
		title:       "Get mail identity",
		description: "Inspect this agent's Orchestra mail address (<handle>@<host>), aliases, status, and unread count.",
		input:       object(schema{}),
		output:      resultSchema(schema{"mailbox": anyObject()}, "mailbox"),
		readOnly:    true,
	},
	"set_mail_identity": {
		title:       "Set mail identity",
		description: "Rename this agent's mail handle (for example \"auth-api\" -> \"auth-api@fabrica\") while retaining the previous address as an alias.",
		input:       object(schema{"name": text()}, "name"),
		output:      resultSchema(schema{"mailbox": anyObject()}, "mailbox"),
	},
	"list_mailboxes": {
		title:       "List agent mailboxes",
		description: "List peer agent mailboxes (<handle>@<host>) across interactive MCP sessions and slave nodes.",
		input: object(schema{
			"host":            text(),
			"onlineOnly":      boolean(),
			"includeReleased": boolean(),
			"workspace":       text(),
		}),
		output:   resultSchema(schema{"mailboxes": objects()}, "mailboxes"),
		readOnly: true,
	},
	"send_mail": {
		title:       "Send agent mail",
		description: "Send a peer-to-peer prompt or reply to another agent by <handle>@<host>, pushing/waking the target agent immediately when supported.",
		input: object(schema{
			"idempotencyKey": text(),
			"to":             text(),
			"replyToId":      text(),
			"subject":        text(),
			"body":           text(),
			"artifactIds":    list(text()),
			"wake":           boolean(),
		}, "idempotencyKey", "body"),
		output: resultSchema(schema{"message": anyObject(), "recipient": anyObject(), "replayed": boolean()}, "message", "recipient", "replayed"),
	},
	"read_mail": {
		title:       "Read agent mail",
		description: "Read messages in this agent's inbox or sent folder, optionally marking inbox messages read.",
		input: object(schema{
			"box":           enum("inbox", "sent"),
			"unreadOnly":    boolean(),
			"afterSequence": schema{"type": "integer", "minimum": 0},
			"limit":         schema{"type": "integer", "minimum": 1, "maximum": protocol.MaximumMailReadLimit},
			"markRead":      boolean(),
		}),
		output: resultSchema(schema{"mailbox": anyObject(), "messages": objects(), "hasMore": boolean()}, "mailbox", "messages", "hasMore"),
	},
	"wait_for_mail": {
		title:       "Wait for agent mail",
		description: "Wait up to timeoutMilliseconds for unread mail to arrive in this agent's inbox.",
		input: object(schema{
			"timeoutMilliseconds": schema{"type": "integer", "minimum": 0, "maximum": protocol.MaximumWaitMilliseconds},
			"afterSequence":       schema{"type": "integer", "minimum": 0},
			"limit":               schema{"type": "integer", "minimum": 1, "maximum": protocol.MaximumMailReadLimit},
			"markRead":            boolean(),
		}),
		output: resultSchema(schema{"mailbox": anyObject(), "messages": objects(), "timedOut": boolean()}, "mailbox", "messages", "timedOut"),
	},
	"wait_for_activity": {
		title:       "Wait for activity",
		description: "Wait up to timeoutMilliseconds for either new thread/room events or incoming Agent Mail, returning whichever arrives first.",
		input: object(schema{
			"threadId":            text(),
			"eventCursor":         text(),
			"cursor":              text(),
			"eventTypes":          list(text()),
			"afterRoomSequence":   schema{"type": "integer", "minimum": 0},
			"afterMailSequence":   schema{"type": "integer", "minimum": 0},
			"unreadMailOnly":      boolean(),
			"markMailRead":        boolean(),
			"timeoutMilliseconds": schema{"type": "integer", "minimum": 0, "maximum": protocol.MaximumWaitMilliseconds},
			"limit":               schema{"type": "integer", "minimum": 1, "maximum": protocol.MaximumEventsPerWait},
		}),
		output: resultSchema(schema{
			"threadId": text(), "wakeSources": list(text()),
			"events": objects(), "eventCursor": text(), "cursorExpired": boolean(),
			"roomMessages": objects(), "roomHeadSequence": integer(),
			"mailbox": anyObject(), "mailMessages": objects(),
			"timedOut": boolean(),
		}, "wakeSources", "events", "roomMessages", "mailMessages", "timedOut"),
	},
}

func postRoomMessageInputSchema(canDelegate bool) schema {
	properties := schema{
		"idempotencyKey":     text(),
		"kind":               enum(protocol.RoomMessageKinds...),
		"channel":            text(),
		"topic":              text(),
		"body":               text(),
		"mentions":           list(text()),
		"wake":               boolean(),
		"replyToSequence":    schema{"type": "integer", "minimum": 1},
		"supersedesSequence": schema{"type": "integer", "minimum": 1},
		"artifactIds":        list(text()),
		"nudgeTaskIds":       list(text()),
	}
	if canDelegate {
		properties["nudge"] = boolean()
	}
	return object(properties, "idempotencyKey", "kind", "body")
}

func definitionForGrant(name string, canDelegate bool) toolDefinition {
	definition := definitions[name]
	if name == "post_room_message" && canDelegate {
		definition.input = postRoomMessageInputSchema(true)
	}
	return definition
}

func ToolNames(canDelegate bool) []string {
	names := make([]string, 0, len(protocol.HubToolNames))
	for _, name := range protocol.HubToolNames {
		if protocol.IsDelegationHubToolName(name) && !canDelegate {
			continue
		}
		names = append(names, name)
	}
	return names
}
