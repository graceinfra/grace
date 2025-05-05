package cmd

import (
	"os"
	"path/filepath"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
)

type initModel struct {
	inputs   []textinput.Model
	focusIdx int
	canceled bool
	done     bool
}

func initialInitModel(workflowArg string) initModel {
	cwd, _ := os.Getwd()
	defaultWorkflow := filepath.Base(cwd)

	workflow := textinput.New()
	workflow.Placeholder = defaultWorkflow
	workflow.CharLimit = 64
	workflow.Width = 20

	if workflowArg != "" {
		workflow.SetValue(workflowArg)
		workflow.CursorEnd()
	}
	workflow.Focus()

	hlq := textinput.New()
	hlq.Placeholder = "IBMUSER"
	hlq.CharLimit = 8
	hlq.Width = 20

	// TODO: selection menu for zowe profiles
	profile := textinput.New()
	profile.Placeholder = "zosmf"
	profile.CharLimit = 32
	profile.Width = 20

	return initModel{
		inputs: []textinput.Model{workflow, hlq, profile},
	}
}

func (m initModel) Init() tea.Cmd {
	return textinput.Blink
}

func (m initModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "esc":
			m.canceled = true
			m.done = true
			return m, tea.Quit
		case "enter":
			m.done = true
			return m, tea.Quit
		case "tab", "shift+tab", "down", "up":
			if msg.String() == "up" || msg.String() == "shift+tab" {
				m.focusIdx--
			} else {
				m.focusIdx++
			}
			if m.focusIdx >= len(m.inputs) {
				m.focusIdx = 0
			} else if m.focusIdx < 0 {
				m.focusIdx = len(m.inputs) - 1
			}
			for i := range m.inputs {
				if i == m.focusIdx {
					m.inputs[i].Focus()
				} else {
					m.inputs[i].Blur()
				}
			}
			return m, nil
		}
	}

	cmds := make([]tea.Cmd, len(m.inputs))
	for i := range m.inputs {
		m.inputs[i], cmds[i] = m.inputs[i].Update(msg)
	}

	return m, tea.Batch(cmds...)
}

func (m initModel) View() string {
	s := "\n"
	labels := []string{"Workflow Name", "HLQ", "Profile"}

	for i, input := range m.inputs {
		s += labels[i] + ": " + input.View() + "\n"
	}

	s += "\n[Enter] to continue • [Esc] to cancel\n"
	return s
}

func RunInitTUI(workspaceArg string) (hlq, profile, workflowName string, canceled bool) {
	p := tea.NewProgram(initialInitModel(workspaceArg))
	m, err := p.Run()
	if err != nil {
		return "", "", "", true
	}

	final := m.(initModel)
	if final.canceled {
		return "", "", "", true
	}

	workflowName = final.inputs[0].Value()
	if workflowName == "" {
		workflowName = "."
	}

	hlq = final.inputs[1].Value()
	if hlq == "" {
		hlq = "IBMUSER"
	}

	profile = final.inputs[2].Value()
	if profile == "" {
		profile = "zosmf"
	}

	return hlq, profile, workflowName, false
}
