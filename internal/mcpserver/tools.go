package mcpserver

import "github.com/cafecito-games/Orchestra/internal/protocol"

const instructions = "Use get_task_context for the durable thread, your task, its lineage, and your mailbox cursor. " +
	"Workers report with update_task, ask or answer through send_task_message, and wait for replies or task changes with wait_for_task_events, passing back the cursor it returns. " +
	"Delegating instances inspect get_execution_inventory and submit parallel work with submit_tasks, describing capabilities rather than machines, " +
	"and may spawn_instance, inspect it with get_instance, renew_instance while it is needed, and release_instance when it is done. " +
	"Refine or complete the thread with update_thread and post durable files with post_artifact. " +
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
			"repository": text(),
			"path":       text(),
			"writable":   boolean(),
			"isolation":  enum("git-worktree", "exclusive-existing"),
			"cleanup":    enum("retain", "when-unchanged"),
			"baseBranch": text(),
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
	return object(schema{
		"id":           text(),
		"threadId":     text(),
		"purpose":      purposeSchema(false),
		"delegation":   object(schema{"canDelegate": boolean()}, "canDelegate"),
		"requirements": requirementsSchema(),
		"lease":        leaseSchema(),
		"status":       text(),
		"createdAt":    text(),
		"updatedAt":    text(),
	}, "id", "threadId", "delegation", "requirements", "lease", "status", "createdAt", "updatedAt")
}

func safeAllocationSchema() schema {
	return object(schema{
		"id":         text(),
		"instanceId": text(),
		"nodeId":     text(),
		"harnessId":  enum(protocol.HarnessIDs...),
		"model":      text(),
		"transport":  enum(protocol.HarnessTransports...),
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
		"instance":      safeInstanceSchema(),
		"allocation":    safeAllocationSchema(),
		"initialTaskId": text(),
		"replayed":      boolean(),
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
		description: "Get the durable thread, your task or a visible related task, its dependencies, attempts, child tasks, artifacts, your mailbox summary and cursor, and limits.",
		input:       object(schema{"taskId": text()}),
		output: resultSchema(schema{
			"thread": anyObject(), "caller": anyObject(), "task": anyObject(), "durableTask": anyObject(),
			"childTasks": objects(), "taskGraph": objects(), "taskGraphTruncated": boolean(),
			"mailbox": anyObject(), "artifacts": objects(), "limits": anyObject(), "version": text(),
		}, "thread", "task", "artifacts", "limits", "version"),
		readOnly: true,
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
		description: "List compute nodes with harnesses, capacity, and worker-reported capabilities and their freshness, plus active instances, to write task requirements.",
		input:       object(schema{}),
		output: resultSchema(schema{
			"generatedAt": text(), "nodes": objects(), "instances": objects(), "truncated": anyObject(),
		}, "generatedAt", "nodes"),
		readOnly: true,
	},
	"spawn_instance": {
		title:       "Spawn instance",
		description: "Request a non-delegating resident instance for this thread. Placement may remain pending; use one stable idempotency key for retries.",
		input: object(schema{
			"idempotencyKey":     text(),
			"requirements":       requirementsSchema(),
			"purpose":            purposeSchema(true),
			"idleTimeoutSeconds": schema{"type": "integer", "minimum": protocol.MinimumInstanceIdleTimeoutSeconds, "maximum": protocol.MaximumInstanceIdleTimeoutSeconds},
			"initialTask":        object(schema{"title": text(), "instructions": text()}, "title", "instructions"),
		}, "idempotencyKey", "requirements"),
		output: instanceToolResultSchema(true),
	},
	"get_instance": {
		title:       "Get instance",
		description: "Get one same-thread instance and its current occupying allocation, including a terminal instance.",
		input:       object(schema{"instanceId": text()}, "instanceId"),
		output:      instanceToolResultSchema(false),
		readOnly:    true,
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
		description: "As the task's current assignee, report progress, an advisory blocked reason (null clears it), or completion fields.",
		input: object(schema{
			"idempotencyKey": text(),
			"progress":       text(),
			"blockedReason":  schema{"type": []string{"string", "null"}},
			"completion":     object(schema{"summary": text(), "artifactIds": list(text())}, "summary"),
		}, "idempotencyKey"),
		output: resultSchema(schema{"created": boolean(), "updateId": text(), "task": anyObject()}, "created", "updateId", "task"),
	},
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
