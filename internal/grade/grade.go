// Package grade defines panewire's ordered task-grade scale.
package grade

var scale = [...]string{"C", "B", "A", "A+", "S", "S+", "SS"}

// Grades returns the task grades from weakest to strongest.
func Grades() []string {
	return append([]string(nil), scale[:]...)
}
