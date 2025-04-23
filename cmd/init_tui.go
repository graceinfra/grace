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

func initialInitModel(workspaceArg string) initModel {
	cwd, _ := os.Getwd()
	defaultWorkspace := filepath.Base(cwd)
	hlq := textinput.New()
	hlq.Placeholder = "IBMUSER"
	hlq.Focus()
	hlq.CharLimit = 8
	hlq.Width = 20

	// TODO: selection menu for zowe profiles
	profile := textinput.New()
	profile.Placeholder = "zosmf"
	profile.CharLimit = 32
	profile.Width = 20

	workspace := textinput.New()
	if workspaceArg != "" {
		workspace.Placeholder = workspaceArg
	} else {
		workspace.Placeholder = defaultWorkspace
	}
	workspace.CharLimit = 64
	workspace.Width = 20

	return initModel{
		inputs: []textinput.Model{hlq, profile, workspace},
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
	labels := []string{"HLQ (High-Level Qualifier)", "Profile", "Workspace Name"}

	for i, input := range m.inputs {
		s += labels[i] + ": " + input.View() + "\n"
	}

	s += "\n[Enter] to continue • [Esc] to cancel\n"
	return s
}

func RunInitTUI(workspaceArg string) (hlq, profile, workspaceName string, canceled bool) {
	p := tea.NewProgram(initialInitModel(workspaceArg))
	m, err := p.Run()
	if err != nil {
		return "", "", "", true
	}

	final := m.(initModel)
	if final.canceled {
		return "", "", "", true
	}

	hlq = final.inputs[0].Value()
	if hlq == "" {
		hlq = "IBMUSER"
	}

	profile = final.inputs[1].Value()
	if profile == "" {
		profile = "zosmf" // fallback default
	}

	workspaceName = final.inputs[2].Value()
	if workspaceName == "" {
		workspaceName = "."
	}

	return hlq, profile, workspaceName, false
}
