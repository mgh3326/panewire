package fleet

import "os"

func writeOMPExtendedContextOverlay() (string, error) {
	file, err := os.CreateTemp("", "panewire-omp-context-*.yml")
	if err != nil {
		return "", err
	}
	path := file.Name()
	remove := func() {
		_ = os.Remove(path)
	}
	if _, err := file.WriteString("extendedContext: true\n"); err != nil {
		_ = file.Close()
		remove()
		return "", err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		remove()
		return "", err
	}
	if err := file.Close(); err != nil {
		remove()
		return "", err
	}
	return path, nil
}
