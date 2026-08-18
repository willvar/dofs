//go:build linux

package dofs_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestRealFUSESupportsMediaContainerRewrite(t *testing.T) {
	if os.Getenv("DOFS_FUSE_INTEGRATION") != "1" {
		t.Skip("set DOFS_FUSE_INTEGRATION=1 to exercise the kernel FUSE contract")
	}
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg is required for the media-container rewrite contract")
	}

	backend, _, _ := newBackend(t, true)
	if err := backend.EnableWriteback(t.Context(), t.TempDir()); err != nil {
		t.Fatal(err)
	}
	mountpoint := t.TempDir()
	mounted, err := dofsMountForTest(mountpoint, backend)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = mounted.Unmount()
		mounted.Wait()
	}()

	source := filepath.Join(t.TempDir(), "source.webm")
	runFFmpeg(t,
		"-f", "lavfi", "-i", "testsrc=size=64x64:rate=10:duration=1.5",
		"-f", "lavfi", "-i", "sine=frequency=440:duration=1.5", "-shortest",
		"-c:v", "libvpx", "-deadline", "realtime", "-cpu-used", "8", "-b:v", "100k",
		"-pix_fmt", "yuv420p", "-c:a", "libopus", "-b:a", "48k", source,
	)
	regularOutput := filepath.Join(mountpoint, "regular.mp4")
	runFFmpeg(t,
		"-i", source, "-map", "0:v:0", "-map", "0:a:0?", "-vf", "scale='min(1280,iw)':-2",
		"-c:v", "libx264", "-preset", "medium", "-crf", "23", "-c:a", "aac", "-b:a", "128k",
		regularOutput,
	)
	runFFmpeg(t, "-xerror", "-i", regularOutput, "-map", "0", "-f", "null", "-")

	output := filepath.Join(mountpoint, "faststart.mp4")
	runFFmpeg(t,
		"-i", source, "-map", "0:v:0", "-map", "0:a:0?", "-vf", "scale='min(1280,iw)':-2",
		"-c:v", "libx264", "-preset", "medium", "-crf", "23", "-c:a", "aac", "-b:a", "128k",
		"-movflags", "+faststart", output,
	)
	runFFmpeg(t, "-xerror", "-i", output, "-map", "0", "-f", "null", "-")
}

func runFFmpeg(t *testing.T, arguments ...string) {
	t.Helper()
	base := []string{"-nostdin", "-hide_banner", "-loglevel", "error", "-y"}
	command := exec.CommandContext(t.Context(), "ffmpeg", append(base, arguments...)...)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("ffmpeg %v: %v\n%s", arguments, err, output)
	}
}
