package config

const WorkflowStart = "__start__"
const WorkflowEnd = "__end__"

type WorkflowStep struct {
	Name    string
	Task    string
	Queue   string
	Depends []string
}

type Task struct {
	// Name of the task referenced from a workflow step
	Name string
	// Creates a sub-task for each of the elements in the workflow entry
	// dictionary with this key.
	ItemListKey string `yaml:"itemListKey"`
	// Sequence of workflow steps
	Steps []*WorkflowStep
}

type Workflow struct {
	Name   string
	VHosts []string
	Steps  []*WorkflowStep
	Tasks  []*Task
}
