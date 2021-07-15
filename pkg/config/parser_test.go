package config

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfigSyntax(t *testing.T) {
	workflows, err := ParseConfig("testdata/config.yaml")
	if err != nil {
		t.Error(err)
	}
	if len(workflows) != 1 {
		t.Fatalf("number of workflows %d, expected 1", len(workflows))
	}
	workflow := workflows[0]
	if len(workflow.Steps) != 5 {
		t.Errorf("workflow has %d steps, expected 5", len(workflow.Steps))
	}
	if len(workflow.Tasks) != 1 {
		t.Errorf("workflow has %d tasks, expected 1", len(workflow.Tasks))
	} else {
		task0 := &Task{
			Name:        "sub2",
			ItemListKey: "subid_list",
			Steps: []*WorkflowStep{
				{Name: "s2s1", Depends: []string{"__start__"}},
				{Name: "__end__", Depends: []string{"s2s1"}},
			},
		}
		if !reflect.DeepEqual(workflow.Tasks[0], task0) {
			t.Errorf("unexpected value for task: %v", workflow.Tasks[0])
		}
	}
}

func TestErrNamePattern(t *testing.T) {
	_, err := ParseConfig("testdata/errNamePattern.yaml")
	assert.True(t, strings.Contains(err.Error(), "invalid workflow name"), err.Error())
}

func TestErrNoTask(t *testing.T) {
	_, err := ParseConfig("testdata/errNoTask.yaml")
	assert.True(t, strings.Contains(err.Error(), "task st2 not defined"), err.Error())
}

func TestErrInvalidDependecy(t *testing.T) {
	_, err := ParseConfig("testdata/errInvalidDependency.yaml")
	assert.True(t, strings.Contains(err.Error(), "has not been declared"), err.Error())
}
