package config

import (
	"fmt"
	"io/ioutil"
	"reflect"
	"regexp"

	"gopkg.in/yaml.v2"
)

type configValidator struct {
	namePattern *regexp.Regexp
}

func newConfigValidator() *configValidator {
	return &configValidator{
		namePattern: regexp.MustCompile(`[a-zA-Z][\w\-]*`),
	}
}

func fullMatchString(re *regexp.Regexp, str string) bool {
	locs := re.FindStringIndex(str)
	return reflect.DeepEqual(locs, []int{0, len(str)})
}

func (v *configValidator) validateSteps(taskNames map[string]bool, steps []*WorkflowStep) error {
	if len(steps) == 0 {
		return fmt.Errorf("no steps defined")
	}

	nameIndices := make(map[string]int, len(steps))
	nameIndices[WorkflowStart] = -1

	for i, step := range steps {
		if step.Name != WorkflowEnd && !fullMatchString(v.namePattern, step.Name) {
			return fmt.Errorf("invalid step name: %s", step.Name)
		}
		if step.Queue != "" && !fullMatchString(v.namePattern, step.Queue) {
			return fmt.Errorf("invalid queue name: %s", step.Queue)
		}
		if step.Task != "" {
			if _, exists := taskNames[step.Task]; !exists {
				return fmt.Errorf("task %s not defined", step.Task)
			}
		}
		if _, exists := nameIndices[step.Name]; exists {
			return fmt.Errorf("duplicate step: %s", step.Name)
		}
		for _, dep := range step.Depends {
			if _, exists := nameIndices[dep]; !exists {
				return fmt.Errorf("dependency %s of step %s has not been declared", dep, step.Name)
			}
		}
		nameIndices[step.Name] = i
	}
	if _, exists := nameIndices[WorkflowEnd]; !exists {
		return fmt.Errorf("terminal \"__end__\" step must be defined")
	}
	return nil
}

func (v *configValidator) Validate(workflow *Workflow) error {
	if !fullMatchString(v.namePattern, workflow.Name) {
		return fmt.Errorf("invalid workflow name: %s", workflow.Name)
	}

	taskNames := make(map[string]bool, len(workflow.Tasks))
	for _, task := range workflow.Tasks {
		if _, exists := taskNames[task.Name]; exists {
			return fmt.Errorf("duplicate task name: %s", task.Name)
		}
		taskNames[task.Name] = true
	}

	err := v.validateSteps(taskNames, workflow.Steps)
	if err != nil {
		return err
	}
	for _, task := range workflow.Tasks {
		if !fullMatchString(v.namePattern, task.Name) {
			return fmt.Errorf("invalid task name: %s", task.Name)
		}

		err := v.validateSteps(taskNames, task.Steps)
		if err != nil {
			return fmt.Errorf("task %s: %w", task.Name, err)
		}
	}
	return nil
}

func setStepDefaults(steps []*WorkflowStep) {
	for _, step := range steps {
		if len(step.Depends) == 0 {
			step.Depends = []string{WorkflowStart}
		}
	}
}

func setDefaults(workflow *Workflow) {
	setStepDefaults(workflow.Steps)
	for _, task := range workflow.Tasks {
		setStepDefaults(task.Steps)
	}
}

func ParseConfig(filename string) ([]Workflow, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("error reading %v: %w", filename, err)
	}
	var config struct {
		Workflows []Workflow
	}
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return nil, err
	}

	v := newConfigValidator()
	for i := 0; i < len(config.Workflows); i++ {
		wrk := &config.Workflows[i]
		err := v.Validate(wrk)
		if err != nil {
			return nil, fmt.Errorf("error in workflow %s: %w", wrk.Name, err)
		}
		setDefaults(wrk)
	}
	return config.Workflows, err
}
